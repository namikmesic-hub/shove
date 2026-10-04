//! Rebalance-safety integration test for the Kafka backend.
//!
//! Exercises the partition revoke/reassign cycle against a real broker:
//! consumer A owns all partitions, consumer B joins (cooperative rebalance
//! moves partitions to B, which processes and commits on them), B leaves
//! (partitions return to A), and a final batch must both be processed AND
//! committed on every partition.
//!
//! Without the rebalance-aware offset tracking in
//! `src/backends/kafka/consumer.rs`, A's stale `PartitionTracker` for a
//! partition B committed on waits forever for a contiguous run that B
//! already consumed — the partition stops committing for the life of the
//! connection and this test times out waiting for lag to reach zero.

#![cfg(feature = "kafka")]

use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::error::Result as ShoveResult;
use shove::handler::MessageHandler;
use shove::kafka::{
    CommitPolicy, KafkaAutoOffsetReset, KafkaClient, KafkaConfig, KafkaConsumer,
    KafkaLagStatsProvider, KafkaQueueStatsProvider,
};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topology::TopologyBuilder;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Message type + topic
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SimpleMessage {
    id: String,
    content: String,
}

shove::define_topic!(
    RebalanceTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-rebalance").build()
);

// ---------------------------------------------------------------------------
// Handler: records ids into a shared set, counts per consumer instance
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SetHandler {
    seen: Arc<Mutex<HashSet<String>>>,
    own_count: Arc<AtomicU32>,
}

impl SetHandler {
    fn new(seen: Arc<Mutex<HashSet<String>>>) -> Self {
        Self {
            seen,
            own_count: Arc::new(AtomicU32::new(0)),
        }
    }
}

impl MessageHandler<RebalanceTopic> for SetHandler {
    type Context = ();
    async fn handle(&self, msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.seen.lock().await.insert(msg.id);
        self.own_count.fetch_add(1, Ordering::Relaxed);
        Outcome::Ack
    }
}

/// A `SetHandler` that can be told to hold the next record it is handed
/// for a while before acknowledging it, so a test can keep a per-record
/// member paused across a rebalance.
#[derive(Clone)]
struct HoldNextHandler {
    inner: SetHandler,
    hold_next: Arc<std::sync::Mutex<Option<Duration>>>,
}

impl HoldNextHandler {
    fn new(seen: Arc<Mutex<HashSet<String>>>) -> Self {
        Self {
            inner: SetHandler::new(seen),
            hold_next: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    fn hold_next(&self, hold: Duration) {
        *self.hold_next.lock().expect("hold mutex poisoned") = Some(hold);
    }
}

impl MessageHandler<RebalanceTopic> for HoldNextHandler {
    type Context = ();
    async fn handle(&self, msg: SimpleMessage, meta: MessageMetadata, ctx: &()) -> Outcome {
        let hold = self.hold_next.lock().expect("hold mutex poisoned").take();
        let outcome = self.inner.handle(msg, meta, ctx).await;
        if let Some(hold) = hold {
            tokio::time::sleep(hold).await;
        }
        outcome
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const QUEUE: &str = "kafka-rebalance";
const GROUP_ID: &str = "kafka-rebalance-consumer";
const WAIT: Duration = Duration::from_secs(60);

async fn publish_batch(broker: &Broker<Kafka>, prefix: &str, n: u32) -> Vec<String> {
    let publisher = broker.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (0..n)
        .map(|i| SimpleMessage {
            id: format!("{prefix}-{i}"),
            content: format!("payload {i}"),
        })
        .collect();
    publisher
        .publish_batch::<RebalanceTopic>(&messages)
        .await
        .expect("publish_batch should succeed");
    messages.into_iter().map(|m| m.id).collect()
}

async fn wait_for_ids(seen: &Mutex<HashSet<String>>, ids: &[String], what: &str) {
    let deadline = Instant::now() + WAIT;
    loop {
        {
            let set = seen.lock().await;
            if ids.iter().all(|id| set.contains(id)) {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for all {what} messages to be processed"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Polls consumer lag (high watermark minus committed offset, summed over all
/// partitions) until it reaches zero. This is the committed-offset
/// convergence check: lag can only reach zero when every partition's
/// committed offset equals its high watermark.
async fn wait_for_zero_lag(client: &KafkaClient, bootstrap: &str, what: &str) {
    wait_for_zero_lag_on(client, bootstrap, QUEUE, GROUP_ID, what).await;
}

/// [`wait_for_zero_lag`] for any queue and group.
async fn wait_for_zero_lag_on(
    client: &KafkaClient,
    bootstrap: &str,
    queue: &str,
    group: &str,
    what: &str,
) {
    let stats_provider = KafkaLagStatsProvider::new(client.clone());
    let deadline = Instant::now() + WAIT;
    loop {
        let stats = stats_provider
            .get_queue_stats(queue, group, KafkaAutoOffsetReset::Earliest)
            .await
            .expect("get_queue_stats should succeed");
        if stats.messages_pending == 0 {
            return;
        }
        if Instant::now() >= deadline {
            dump_partition_state(bootstrap, queue, group);
            panic!(
                "committed offsets did not converge to the high watermark {what}: \
                 {} message(s) still pending — partitions returned by the departed \
                 consumer have stalled commits",
                stats.messages_pending
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// Failure diagnostics: per-partition committed offset vs watermarks, printed
// when a convergence wait times out so a flake report shows exactly which
// partitions stalled and where.
fn dump_partition_state(bootstrap: &str, queue: &str, group: &str) {
    use rdkafka::TopicPartitionList;
    use rdkafka::consumer::{BaseConsumer, Consumer as _};
    let consumer: BaseConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .create()
        .expect("diag consumer");
    let md = consumer
        .fetch_metadata(Some(queue), Duration::from_secs(5))
        .expect("metadata");
    let pids: Vec<i32> = md.topics()[0].partitions().iter().map(|p| p.id()).collect();
    let mut tpl = TopicPartitionList::new();
    for &pid in &pids {
        tpl.add_partition(queue, pid);
    }
    let committed = consumer
        .committed_offsets(tpl, Duration::from_secs(5))
        .expect("committed");
    for pid in pids {
        let (low, high) = consumer
            .fetch_watermarks(queue, pid, Duration::from_secs(5))
            .expect("watermarks");
        let c = committed
            .find_partition(queue, pid)
            .map(|e| format!("{:?}", e.offset()))
            .unwrap_or_else(|| "absent".into());
        eprintln!("DIAG partition {pid}: low={low} high={high} committed={c}");
    }
}

fn spawn_consumer(
    client: KafkaClient,
    handler: SetHandler,
    shutdown: CancellationToken,
) -> JoinHandle<ShoveResult<()>> {
    let consumer = KafkaConsumer::new(client);
    tokio::spawn(async move {
        consumer
            .run::<RebalanceTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(shutdown)
                    .with_prefetch_count(10),
            )
            .await
    })
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn commits_resume_on_partitions_returned_after_rebalance() {
    // Surface shove's rebalance/commit-retry logs in failure output (nextest
    // prints captured output for failed tests).
    let _ = tracing_subscriber::fmt()
        .with_env_filter("shove=debug")
        .try_init();
    let container = KafkaContainer::default()
        .start()
        .await
        .expect("failed to start Kafka container");
    let port = container
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("failed to get Kafka port");
    let bootstrap_servers = format!("127.0.0.1:{port}");
    let client = KafkaClient::connect_with_retry(&KafkaConfig::new(&bootstrap_servers), 10)
        .await
        .expect("failed to connect to Kafka");
    let broker = Broker::<Kafka>::from_client(client.clone());

    // Declared with the default partition count (8 — ≥ 4 required so the
    // cooperative rebalance moves a meaningful set of partitions to B).
    broker.topology().declare::<RebalanceTopic>().await.unwrap();

    let seen: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    // Phase 1: consumer A alone owns all partitions; a full batch is
    // processed and committed.
    let handler_a = SetHandler::new(seen.clone());
    let shutdown_a = CancellationToken::new();
    let handle_a = spawn_consumer(client.clone(), handler_a.clone(), shutdown_a.clone());

    let batch1 = publish_batch(&broker, "b1", 100).await;
    wait_for_ids(&seen, &batch1, "batch-1").await;
    wait_for_zero_lag(
        &client,
        &bootstrap_servers,
        "after batch 1 (single consumer)",
    )
    .await;

    // Phase 2: consumer B joins the same group; the cooperative rebalance
    // revokes some partitions from A and assigns them to B. Probe batches
    // are published until B demonstrably processes messages, proving it owns
    // partitions (and will advance their committed offsets past A's stale
    // tracker state).
    let handler_b = SetHandler::new(seen.clone());
    let shutdown_b = CancellationToken::new();
    let handle_b = spawn_consumer(client.clone(), handler_b.clone(), shutdown_b.clone());

    let join_deadline = Instant::now() + WAIT;
    let mut probe = 0u32;
    while handler_b.own_count.load(Ordering::Relaxed) == 0 {
        assert!(
            Instant::now() < join_deadline,
            "consumer B never processed a message — rebalance did not move \
             partitions to it"
        );
        publish_batch(&broker, &format!("probe-{probe}"), 8).await;
        probe += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // A larger batch spread across all partitions so B commits well ahead of
    // A's stale per-partition trackers.
    let batch2 = publish_batch(&broker, "b2", 100).await;
    wait_for_ids(&seen, &batch2, "batch-2").await;
    wait_for_zero_lag(&client, &bootstrap_servers, "after batch 2 (two consumers)").await;

    // Phase 3: B leaves gracefully; its partitions return to A. A's offset
    // tracking for those partitions must reset — with stale trackers the
    // partitions B committed on never commit again on A.
    shutdown_b.cancel();
    handle_b.await.unwrap().ok();

    let batch3 = publish_batch(&broker, "b3", 100).await;
    wait_for_ids(&seen, &batch3, "batch-3").await;

    // The money assert: committed offsets on ALL partitions converge to the
    // high watermark. Pre-fix this times out on the partitions B committed
    // on while it owned them.
    wait_for_zero_lag(
        &client,
        &bootstrap_servers,
        "after batch 3 (B's partitions back on A)",
    )
    .await;

    shutdown_a.cancel();
    handle_a.await.unwrap().ok();
    broker.close().await;
}

// ---------------------------------------------------------------------------
// A partition revoked while its work is pending, and the final commit
// ---------------------------------------------------------------------------

const PENDING_QUEUE: &str = "kafka-revoked-pending";
/// The group the topic's default configuration joins: `{queue}-consumer`.
const PENDING_GROUP_ID: &str = "kafka-revoked-pending-consumer";
/// Two partitions, so that when a second member joins the cooperative
/// rebalance moves exactly one of them.
const PENDING_PARTITIONS: [i32; 2] = [0, 1];

// Bound as external: the test creates the topic itself, with the partition
// count above, as infra would.
shove::define_topic!(
    RevokedPendingTopic,
    SimpleMessage,
    TopologyBuilder::new(PENDING_QUEUE).external().build()
);

/// Acknowledges every record, keeps the ids it started and finished, and
/// holds every `hold-*` record until `release` says so.
#[derive(Clone)]
struct HoldingHandler {
    started: Arc<Mutex<HashSet<String>>>,
    finished: Arc<Mutex<HashSet<String>>>,
    release: watch::Receiver<bool>,
}

impl HoldingHandler {
    fn new(release: watch::Receiver<bool>) -> Self {
        Self {
            started: Arc::new(Mutex::new(HashSet::new())),
            finished: Arc::new(Mutex::new(HashSet::new())),
            release,
        }
    }

    async fn started_ids(&self) -> HashSet<String> {
        self.started.lock().await.clone()
    }
}

impl MessageHandler<RevokedPendingTopic> for HoldingHandler {
    type Context = ();
    async fn handle(&self, msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.started.lock().await.insert(msg.id.clone());
        if msg.id.starts_with("hold-") {
            let mut release = self.release.clone();
            release
                .wait_for(|released| *released)
                .await
                .expect("the release sender outlives the handler");
        }
        self.finished.lock().await.insert(msg.id);
        Outcome::Ack
    }
}

/// Creates the two-partition topic through the admin API.
async fn create_pending_topic(bootstrap: &str) {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("admin client");
    let topic = NewTopic::new(
        PENDING_QUEUE,
        PENDING_PARTITIONS.len() as i32,
        TopicReplication::Fixed(1),
    );
    let results = admin
        .create_topics(
            &[topic],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await
        .expect("create_topics");
    for result in results {
        result.expect("the topic is created");
    }
}

/// Produces one record pinned to `partition`, with the topic's JSON payload,
/// and returns the offset the broker gave it.
async fn produce_pinned(producer: &FutureProducer, id: &str, partition: i32) -> i64 {
    let payload = serde_json::to_vec(&SimpleMessage {
        id: id.to_string(),
        content: format!("payload {id}"),
    })
    .expect("encode the record");
    let delivery = producer
        .send(
            FutureRecord::<(), Vec<u8>>::to(PENDING_QUEUE)
                .partition(partition)
                .payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .expect("produce to the broker");
    assert_eq!(
        delivery.partition, partition,
        "the record lands where it was pinned"
    );
    delivery.offset
}

/// The group's committed position on `partition`, read through a probe that
/// never joins the group; `None` before the first accepted commit.
async fn committed_position_of(bootstrap: &str, partition: i32) -> Option<i64> {
    use rdkafka::TopicPartitionList;
    use rdkafka::consumer::{BaseConsumer, Consumer as _};
    let bootstrap = bootstrap.to_owned();
    tokio::task::spawn_blocking(move || {
        let probe: BaseConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", PENDING_GROUP_ID)
            .create()
            .expect("probe consumer");
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(PENDING_QUEUE, partition);
        probe
            .committed_offsets(tpl, Duration::from_secs(5))
            .expect("committed offsets")
            .find_partition(PENDING_QUEUE, partition)
            .and_then(|e| match e.offset() {
                rdkafka::Offset::Offset(offset) => Some(offset),
                _ => None,
            })
    })
    .await
    .expect("probe task")
}

/// The high watermark of `partition`: the position a member that has
/// committed every record stands at.
async fn high_watermark_of(bootstrap: &str, partition: i32) -> i64 {
    use rdkafka::consumer::{BaseConsumer, Consumer as _};
    let bootstrap = bootstrap.to_owned();
    tokio::task::spawn_blocking(move || {
        let probe: BaseConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .create()
            .expect("probe consumer");
        probe
            .fetch_watermarks(PENDING_QUEUE, partition, Duration::from_secs(5))
            .expect("watermarks")
            .1
    })
    .await
    .expect("probe task")
}

fn spawn_holding_consumer(
    client: KafkaClient,
    handler: HoldingHandler,
    shutdown: CancellationToken,
) -> JoinHandle<ShoveResult<()>> {
    let consumer = KafkaConsumer::new(client);
    tokio::spawn(async move {
        consumer
            .run::<RevokedPendingTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(shutdown)
                    .with_prefetch_count(10)
                    // The holds outlast the default handler timeout.
                    .without_handler_timeout(),
            )
            .await
    })
}

/// A partition a rebalance revokes while one of its records is still in a
/// handler is left out of the member's final commit, and the late
/// acknowledgement of that record raises no error.
///
/// Member A owns both partitions and holds one record on each. Member B
/// joins, and the cooperative rebalance moves one partition to B, which
/// resumes it from the last accepted position: the held record itself,
/// which B receives again and holds too. A's holds are then released, so A
/// acknowledges a record on a partition it no longer owns, and A stops. A's
/// run ends clean. The broker's committed position on the moved partition
/// is still the held record's offset, because neither A's late
/// acknowledgement nor its final commit moved it, and on the partition A
/// kept the final commit carried every acknowledged record.
#[tokio::test]
async fn a_partition_revoked_with_work_pending_is_left_out_of_the_final_commit() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("shove=debug")
        .try_init();
    let container = KafkaContainer::default()
        .start()
        .await
        .expect("failed to start Kafka container");
    let port = container
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("failed to get Kafka port");
    let bootstrap = format!("127.0.0.1:{port}");
    create_pending_topic(&bootstrap).await;
    let client = KafkaClient::connect_with_retry(&KafkaConfig::new(&bootstrap), 10)
        .await
        .expect("failed to connect to Kafka");
    let producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .expect("raw producer");

    // Phase 1: A alone. One record per partition, acknowledged and
    // committed, so each partition's committed position is its high
    // watermark.
    let (release_a, released_a) = watch::channel(false);
    let handler_a = HoldingHandler::new(released_a);
    let shutdown_a = CancellationToken::new();
    let handle_a = spawn_holding_consumer(client.clone(), handler_a.clone(), shutdown_a.clone());
    let mut warmup = Vec::new();
    for &p in &PENDING_PARTITIONS {
        let id = format!("warm-{p}");
        produce_pinned(&producer, &id, p).await;
        warmup.push(id);
    }
    wait_for_ids(&handler_a.finished, &warmup, "warm-up").await;
    wait_for_zero_lag_on(
        &client,
        &bootstrap,
        PENDING_QUEUE,
        PENDING_GROUP_ID,
        "after the warm-up",
    )
    .await;

    // One held record per partition. Its offset is the committed position,
    // so a member that takes the partition over receives it first.
    let mut hold_offset = HashMap::new();
    let mut holds = Vec::new();
    for &p in &PENDING_PARTITIONS {
        let id = format!("hold-{p}");
        let offset = produce_pinned(&producer, &id, p).await;
        assert_eq!(
            committed_position_of(&bootstrap, p).await,
            Some(offset),
            "the held record sits at the committed position of partition {p}"
        );
        hold_offset.insert(p, offset);
        holds.push(id);
    }
    wait_for_ids(&handler_a.started, &holds, "the held records").await;

    // Phase 2: B joins, and the cooperative rebalance moves one partition to
    // it while A's handlers hold their records. Probes pinned to both
    // partitions are published until B has started a held record: that
    // record is the first one B receives on the partition it was given.
    let (release_b, released_b) = watch::channel(false);
    let handler_b = HoldingHandler::new(released_b);
    let shutdown_b = CancellationToken::new();
    let handle_b = spawn_holding_consumer(client.clone(), handler_b.clone(), shutdown_b.clone());
    let mut probes: HashMap<i32, Vec<String>> = HashMap::new();
    let join_deadline = Instant::now() + WAIT;
    let mut round = 0u32;
    let started_b = loop {
        let started = handler_b.started_ids().await;
        if started.iter().any(|id| id.starts_with("hold-")) {
            break started;
        }
        assert!(
            Instant::now() < join_deadline,
            "consumer B never received a held record; the rebalance did not move a partition \
             to it: {started:?}"
        );
        for &p in &PENDING_PARTITIONS {
            let id = format!("probe-{round}-{p}");
            produce_pinned(&producer, &id, p).await;
            probes.entry(p).or_default().push(id);
        }
        round += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let moved: Vec<i32> = PENDING_PARTITIONS
        .iter()
        .copied()
        .filter(|p| started_b.contains(&format!("hold-{p}")))
        .collect();
    assert_eq!(
        moved.len(),
        1,
        "one of two partitions moves to B, which receives its held record first: {started_b:?}"
    );
    let moved = moved[0];
    let kept = PENDING_PARTITIONS
        .iter()
        .copied()
        .find(|&p| p != moved)
        .expect("two partitions");

    // A still owns the kept partition: let it finish every probe published
    // there, so that nothing but the held record is in flight on it.
    wait_for_ids(
        &handler_a.finished,
        &probes[&kept],
        "the probes on the kept partition",
    )
    .await;

    // Release A's holds. The acknowledgement on the moved partition arrives
    // after A lost it.
    release_a
        .send(true)
        .expect("A's handlers hold the receiver");
    wait_for_ids(
        &handler_a.finished,
        &holds,
        "A's held records after the release",
    )
    .await;

    // Stop A. Its final commit carries the kept partition only, and the late
    // acknowledgement on the moved one raises no error.
    shutdown_a.cancel();
    let result = handle_a.await.expect("A's task completes");
    assert!(
        result.is_ok(),
        "A ends clean despite the acknowledgement on a revoked partition: {result:?}"
    );

    // The moved partition's committed position is still the held record:
    // A's final commit left the partition out, and B holds the record.
    assert_eq!(
        committed_position_of(&bootstrap, moved).await,
        Some(hold_offset[&moved]),
        "the moved partition {moved} stays at its held record"
    );
    // The kept partition's final commit carried every acknowledged record.
    assert_eq!(
        committed_position_of(&bootstrap, kept).await,
        Some(high_watermark_of(&bootstrap, kept).await),
        "the kept partition {kept} is committed to its high watermark"
    );

    // Release B: it acknowledges the held record and commits past the
    // probes, and the group converges.
    release_b
        .send(true)
        .expect("B's handler holds the receiver");
    wait_for_zero_lag_on(
        &client,
        &bootstrap,
        PENDING_QUEUE,
        PENDING_GROUP_ID,
        "after B's release",
    )
    .await;
    shutdown_b.cancel();
    handle_b
        .await
        .expect("B's task completes")
        .expect("B ends clean");
}

/// The same cycle under `CommitPolicy::PerRecord`, with A paused when B's
/// join revokes partitions from it. A per-record member pauses its whole
/// assignment while a record is in the handler, and librdkafka keeps a
/// partition's pause flag across a revoke, so the partitions B hands back
/// arrive paused, at a moment A is idle and not paused. The loop re-applies
/// its intent on every assign event, so they are resumed; without that, the
/// final batch's records on the returned partitions never reach A and the
/// wait below times out.
#[tokio::test]
async fn per_record_member_resumes_partitions_returned_after_a_paused_revoke() {
    const HOLD: Duration = Duration::from_secs(12);
    let _ = tracing_subscriber::fmt()
        .with_env_filter("shove=debug")
        .try_init();
    let container = KafkaContainer::default()
        .start()
        .await
        .expect("failed to start Kafka container");
    let port = container
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("failed to get Kafka port");
    let bootstrap_servers = format!("127.0.0.1:{port}");
    let client = KafkaClient::connect_with_retry(&KafkaConfig::new(&bootstrap_servers), 10)
        .await
        .expect("failed to connect to Kafka");
    let broker = Broker::<Kafka>::from_client(client.clone());
    broker.topology().declare::<RebalanceTopic>().await.unwrap();

    let seen: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    // Phase 1: A alone, one permit, a commit per record; a first batch is
    // processed and committed on every partition.
    let handler_a = HoldNextHandler::new(seen.clone());
    let shutdown_a = CancellationToken::new();
    let handle_a = {
        let consumer = KafkaConsumer::new(client.clone());
        let handler = handler_a.clone();
        let shutdown = shutdown_a.clone();
        tokio::spawn(async move {
            consumer
                .run::<RebalanceTopic, _>(
                    handler,
                    (),
                    ConsumerOptions::<Kafka>::new()
                        .with_shutdown(shutdown)
                        .with_prefetch_count(1)
                        .with_concurrent_processing(true)
                        .with_commit_policy(CommitPolicy::PerRecord),
                )
                .await
        })
    };
    let batch1 = publish_batch(&broker, "b1", 16).await;
    wait_for_ids(&seen, &batch1, "batch-1").await;
    wait_for_zero_lag(
        &client,
        &bootstrap_servers,
        "after batch 1 (single consumer)",
    )
    .await;

    // Phase 2: A is handed one record and holds it, so it is paused, and B
    // joins meanwhile: the cooperative rebalance revokes partitions from a
    // paused member.
    handler_a.hold_next(HOLD);
    let held = publish_batch(&broker, "held", 1).await;
    wait_for_ids(&seen, &held, "the held record").await;
    let handler_b = SetHandler::new(seen.clone());
    let shutdown_b = CancellationToken::new();
    let handle_b = spawn_consumer(client.clone(), handler_b.clone(), shutdown_b.clone());
    let join_deadline = Instant::now() + WAIT;
    let mut probe = 0u32;
    while handler_b.own_count.load(Ordering::Relaxed) == 0 {
        assert!(
            Instant::now() < join_deadline,
            "consumer B never processed a message: the rebalance did not move partitions to it"
        );
        publish_batch(&broker, &format!("probe-{probe}"), 8).await;
        probe += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // Everything published so far is processed and committed, on A's
    // partitions once its hold ends and on B's as they come. A is then idle
    // and not paused.
    let batch2 = publish_batch(&broker, "b2", 16).await;
    wait_for_ids(&seen, &batch2, "batch-2").await;
    wait_for_zero_lag(&client, &bootstrap_servers, "after batch 2 (two consumers)").await;

    // Phase 3: B leaves; its partitions, paused when they left A, return to
    // an idle A. A third batch must be processed and committed on every
    // partition, the returned ones included.
    shutdown_b.cancel();
    handle_b.await.unwrap().ok();
    let batch3 = publish_batch(&broker, "b3", 100).await;
    wait_for_ids(&seen, &batch3, "batch-3").await;
    wait_for_zero_lag(
        &client,
        &bootstrap_servers,
        "after batch 3 (B's partitions back on a per-record A)",
    )
    .await;

    shutdown_a.cancel();
    handle_a.await.unwrap().ok();
    broker.close().await;
}

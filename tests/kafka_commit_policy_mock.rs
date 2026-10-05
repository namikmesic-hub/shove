//! `CommitPolicy::PerRecord` on the concurrent Kafka consumer, against
//! librdkafka's mock cluster (`rdkafka::mocking`), which serves the consumer
//! group protocol, offset commits and request tracking from a localhost
//! listener and needs no Docker.
//!
//! Under `PerRecord` every completion is committed synchronously and the
//! next record is handed out only once the coordinator has confirmed that
//! commit; the assignment is paused meanwhile and the loop keeps polling.
//! These tests count the requests the mock broker receives, kill a member
//! the moment it holds the record behind a confirmed completion, refuse
//! every commit until the fenced consumer detector fires, hold one answer
//! while a handler runs past the member's poll interval, stop a member while
//! its commit is in flight, and move the group's position back behind a
//! killed member. The mock answers a connection in order, so an answer held
//! past the 10 s session timeout starves the heartbeats behind it and the
//! coordinator drops the member: a rebalance during a pause and a stop
//! against a frozen coordinator are therefore proved against a real broker,
//! in `kafka_rebalance.rs` and `kafka_integration.rs`. The mock's request tracking and its per-request answer delay
//! have no wrapper in `rdkafka::mocking`, so this file reads them through
//! `rdkafka::bindings`, in `Mock`.
//!
//! The topic is bound with `external()`, because the mock broker has no
//! CreateTopics API: each test creates the topic through the mock API, as
//! infra would, and produces through a raw rdkafka producer.
//!
//! `test-support` gates the seams this file reads; both Kafka coverage rows
//! enable it, so the suite runs in each.

#![cfg(all(feature = "kafka", feature = "test-support"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

use std::os::raw::c_int;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(feature = "metrics")]
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use rdkafka::bindings::{
    rd_kafka_handle_mock_cluster, rd_kafka_mock_broker_push_request_error_rtts,
    rd_kafka_mock_get_requests, rd_kafka_mock_request_api_key, rd_kafka_mock_request_destroy_array,
    rd_kafka_mock_request_timestamp, rd_kafka_mock_start_request_tracking,
};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{
    BaseProducer, DefaultProducerContext, FutureProducer, FutureRecord, Producer,
};
use rdkafka::types::{RDKafkaApiKey, RDKafkaMockCluster, RDKafkaRespErr};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use shove::ShoveError;
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::consumer_group::ConsumerGroupConfig;
use shove::handler::MessageHandler;
use shove::kafka::{
    CommitPolicy, KafkaClient, KafkaConfig, KafkaConsumer, KafkaConsumerGroupConfig, fence_probe,
    final_commit_spawn_probe, housekeeping_interval_for_test, per_record_probe,
    shutdown_commit_deadline_for_test,
};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topology::TopologyBuilder;
use tokio::sync::{Notify, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const TOPIC: &str = "kafka-commit-policy-mock";
/// The group the topic's default configuration joins: `{queue}-consumer`.
const GROUP_ID: &str = "kafka-commit-policy-mock-consumer";
/// The mock broker's id with one broker configured.
const BROKER_ID: i32 = 1;
/// A record reaches the handler on a mock cluster in well under this.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(60);
/// The shortest `max.poll.interval.ms` librdkafka admits, the pinned
/// `session.timeout.ms`; a member that stops polling for longer leaves its
/// group.
const SHORT_POLL_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Order {
    id: String,
}

shove::define_topic!(
    OrdersTopic,
    Order,
    TopologyBuilder::new(TOPIC).external().build()
);

/// Acknowledges every record and records its id with the instant it
/// arrived, so a test can wait for a delivery with a bound and measure the
/// gap between two. `hold` keeps the record at that index in the handler
/// for that long before acknowledging it.
#[derive(Clone, Default)]
struct Acking {
    seen: Arc<Mutex<Vec<(String, Instant)>>>,
    hold: Option<(usize, Duration)>,
}

impl Acking {
    fn ids(&self) -> Vec<String> {
        self.seen
            .lock()
            .expect("handler mutex poisoned")
            .iter()
            .map(|(id, _)| id.clone())
            .collect()
    }

    fn count(&self) -> usize {
        self.seen.lock().expect("handler mutex poisoned").len()
    }

    fn arrived_at(&self, index: usize) -> Instant {
        self.seen.lock().expect("handler mutex poisoned")[index].1
    }
}

impl MessageHandler<OrdersTopic> for Acking {
    type Context = ();
    async fn handle(&self, msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
        let index = self.count();
        self.seen
            .lock()
            .expect("handler mutex poisoned")
            .push((msg.id, Instant::now()));
        if let Some((held, hold)) = self.hold
            && held == index
        {
            tokio::time::sleep(hold).await;
        }
        Outcome::Ack
    }
}

/// Acknowledges every record before the one at `index`, and on that one
/// signals `holding` and never returns: the member is killed while it holds
/// that record.
#[derive(Clone)]
struct HoldAt {
    index: usize,
    earlier: Acking,
    holding: Arc<Notify>,
}

impl MessageHandler<OrdersTopic> for HoldAt {
    type Context = ();
    async fn handle(&self, msg: Order, meta: MessageMetadata, ctx: &()) -> Outcome {
        if self.earlier.count() < self.index {
            return self.earlier.handle(msg, meta, ctx).await;
        }
        self.holding.notify_one();
        std::future::pending().await
    }
}

/// One mock broker with the topic created, as infra would create it, and
/// the two mock features `rdkafka::mocking` does not wrap: request tracking,
/// which counts the requests the broker received by API key, and a
/// per-request answer delay, which holds one OffsetCommit answer without
/// touching fetches or the group protocol on other connections.
///
/// The cluster is the one a producer configured with
/// `test.mock.num.brokers` owns, so its raw handle is reachable through
/// `rd_kafka_handle_mock_cluster` beside the safe wrapper the same client
/// hands out. The producer produces nothing; it lives for the cluster.
struct Mock {
    owner: BaseProducer,
    cluster: *mut RDKafkaMockCluster,
}

impl Mock {
    fn start() -> Self {
        let owner: BaseProducer = ClientConfig::new()
            .set("test.mock.num.brokers", "1")
            .create()
            .expect("a producer that owns a mock cluster");
        let cluster = unsafe { rd_kafka_handle_mock_cluster(owner.client().native_ptr()) };
        assert!(!cluster.is_null(), "the producer owns a mock cluster");
        let mock = Self { owner, cluster };
        mock.api()
            .create_topic(TOPIC, 1, 1)
            .expect("create the topic through the mock API");
        mock
    }

    /// The safe wrapper over the same cluster.
    fn api(&self) -> MockCluster<'_, DefaultProducerContext> {
        self.owner
            .client()
            .mock_cluster()
            .expect("the producer owns a mock cluster")
    }

    fn bootstrap(&self) -> String {
        self.api().bootstrap_servers()
    }

    /// Record every request from here on; see `requests_of`.
    fn track_requests(&self) {
        unsafe { rd_kafka_mock_start_request_tracking(self.cluster) }
    }

    /// How many requests of `api_key` the broker has received since
    /// `track_requests`, whichever client sent them.
    fn requests_of(&self, api_key: RDKafkaApiKey) -> usize {
        let mut count = 0usize;
        let requests = unsafe { rd_kafka_mock_get_requests(self.cluster, &mut count) };
        if requests.is_null() {
            return 0;
        }
        let wanted = i16::from(api_key);
        let matching = (0..count)
            .filter(|&i| unsafe { rd_kafka_mock_request_api_key(*requests.add(i)) } == wanted)
            .count();
        unsafe { rd_kafka_mock_request_destroy_array(requests, count) };
        matching
    }

    fn offset_commit_requests(&self) -> usize {
        self.requests_of(RDKafkaApiKey::OffsetCommit)
    }

    /// When each request of `api_key` reached the broker since
    /// `track_requests`, in arrival order, in microseconds of the mock's
    /// clock.
    fn request_timestamps(&self, api_key: RDKafkaApiKey) -> Vec<i64> {
        let mut count = 0usize;
        let requests = unsafe { rd_kafka_mock_get_requests(self.cluster, &mut count) };
        if requests.is_null() {
            return Vec::new();
        }
        let wanted = i16::from(api_key);
        let stamps = (0..count)
            .filter_map(|i| {
                let request = unsafe { *requests.add(i) };
                (unsafe { rd_kafka_mock_request_api_key(request) } == wanted)
                    .then(|| unsafe { rd_kafka_mock_request_timestamp(request) })
            })
            .collect();
        unsafe { rd_kafka_mock_request_destroy_array(requests, count) };
        stamps
    }

    /// Refuse the next `count` OffsetCommit requests with an error
    /// librdkafka neither retries nor rejoins the group over; the ones after
    /// are accepted.
    fn reject_commits(&self, count: usize) {
        let errors = vec![RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED; count];
        self.api()
            .request_errors(RDKafkaApiKey::OffsetCommit, &errors);
    }

    /// Hold the answer to the next OffsetCommit for `delay`, then answer it
    /// with no error. Only that one request is affected: the broker answers
    /// a connection's requests in order, so a later OffsetCommit on the same
    /// connection waits behind it, but fetches and the group protocol on
    /// other connections do not.
    fn delay_next_offset_commit_answer(&self, delay: Duration) {
        self.answer_next_offset_commit_late(delay, RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR);
    }

    /// Hold the answer to the next OffsetCommit for `delay`, then answer it
    /// with `err`: a rejection that arrives late, when `err` is one.
    fn answer_next_offset_commit_late(&self, delay: Duration, err: RDKafkaRespErr) {
        let delay_ms = c_int::try_from(delay.as_millis()).expect("delay fits a C int");
        let err = unsafe {
            rd_kafka_mock_broker_push_request_error_rtts(
                self.cluster,
                BROKER_ID,
                i16::from(RDKafkaApiKey::OffsetCommit),
                1,
                err as c_int,
                delay_ms,
            )
        };
        assert_eq!(
            err,
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
            "push the delayed answer onto the mock broker"
        );
    }
}

/// Polls `done` until it holds, or panics once `timeout` has passed.
async fn wait_until(done: impl Fn() -> bool, timeout: Duration, what: &str) {
    let deadline = Instant::now() + timeout;
    while !done() {
        assert!(
            Instant::now() < deadline,
            "{what} did not happen within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn connect(bootstrap: &str) -> KafkaClient {
    KafkaClient::connect_with_retry(&KafkaConfig::new(bootstrap), 10)
        .await
        .expect("connect to the mock cluster")
}

/// Produces `ids` in order, through a raw rdkafka producer, as the topic's
/// JSON.
async fn produce(bootstrap: &str, ids: &[&str]) {
    produce_on(bootstrap, TOPIC, ids).await;
}

/// Produces one record onto the topic with `payload` as is, so a test can
/// put a record the topic's codec cannot decode in front of valid ones.
async fn produce_raw(bootstrap: &str, payload: Vec<u8>) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("mock producer");
    producer
        .send(
            FutureRecord::<(), Vec<u8>>::to(TOPIC).payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .expect("produce to the mock cluster");
}

/// `produce` onto another topic of the same shape.
async fn produce_on(bootstrap: &str, topic: &str, ids: &[&str]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("mock producer");
    for id in ids {
        let payload = serde_json::to_vec(&Order { id: (*id).into() }).expect("encode the record");
        producer
            .send(
                FutureRecord::to(topic).key(*id).payload(&payload),
                Duration::from_secs(10),
            )
            .await
            .expect("produce to the mock cluster");
    }
}

/// The group's committed position on the topic's one partition, read
/// through a raw consumer that never joins the group. `None` before the
/// first accepted commit.
async fn committed_position(bootstrap: &str) -> Option<i64> {
    committed_position_on(bootstrap, TOPIC, GROUP_ID, 0).await
}

/// [`committed_position`] for any topic, group and partition.
async fn committed_position_on(
    bootstrap: &str,
    topic: &'static str,
    group: &'static str,
    partition: i32,
) -> Option<i64> {
    let bootstrap = bootstrap.to_owned();
    tokio::task::spawn_blocking(move || {
        let probe: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", group)
            .create()
            .expect("probe consumer");
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(topic, partition);
        probe
            .committed_offsets(tpl, Duration::from_secs(5))
            .expect("read the committed offsets")
            .elements()
            .iter()
            .find(|e| e.partition() == partition)
            .and_then(|e| match e.offset() {
                Offset::Offset(offset) => Some(offset),
                _ => None,
            })
    })
    .await
    .expect("probe task")
}

/// Moves the group's committed position on the topic's one partition to
/// `offset`, through a raw consumer that joins the group for the commit
/// and leaves again: what a commit the broker applies late does to the
/// position. The mock refuses a commit from a consumer that is not a
/// member, so the committer joins; it is called once the member under test
/// is gone, so the join and the leave rebalance nobody.
async fn move_committed_position(bootstrap: &str, offset: i64) {
    let bootstrap = bootstrap.to_owned();
    tokio::task::spawn_blocking(move || {
        // The member's own assignor and session timeout, so the group sees
        // one protocol and one timing.
        let committer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", GROUP_ID)
            .set("partition.assignment.strategy", "cooperative-sticky")
            .set("session.timeout.ms", "10000")
            .set("enable.auto.commit", "false")
            .create()
            .expect("outside committer");
        committer
            .subscribe(&[TOPIC])
            .expect("subscribe the committer");
        let joined_by = std::time::Instant::now() + DELIVERY_TIMEOUT;
        while committer.assignment().expect("read the assignment").count() == 0 {
            assert!(
                std::time::Instant::now() < joined_by,
                "the committer joins the group within the bound"
            );
            let _ = committer.poll(Duration::from_millis(100));
        }
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(TOPIC, 0, Offset::Offset(offset))
            .expect("one partition");
        committer
            .commit(&tpl, CommitMode::Sync)
            .expect("move the committed position");
        drop(committer);
    })
    .await
    .expect("outside committer task");
}

/// Polls the broker until the group's committed position is `expected`.
async fn wait_for_committed_position(bootstrap: &str, expected: i64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while committed_position(bootstrap).await != Some(expected) {
        assert!(
            Instant::now() < deadline,
            "the broker did not accept the commit at {expected} within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// One sequential direct consumer on the topic under `policy`: the one
/// permit `PerRecord` needs, and the same shape for the `Interval` control.
fn options(policy: CommitPolicy, shutdown: CancellationToken) -> ConsumerOptions<Kafka> {
    ConsumerOptions::<Kafka>::new()
        .with_prefetch_count(1)
        .with_concurrent_processing(true)
        .with_commit_policy(policy)
        .with_shutdown(shutdown)
}

/// Starts a direct consumer on the topic with `handler` and `options`, and
/// returns its run task and the token that stops it.
fn start<H>(
    client: KafkaClient,
    handler: H,
    options: impl FnOnce(CancellationToken) -> ConsumerOptions<Kafka>,
) -> (
    tokio::task::JoinHandle<Result<(), ShoveError>>,
    CancellationToken,
)
where
    H: MessageHandler<OrdersTopic, Context = ()> + 'static,
{
    let shutdown = CancellationToken::new();
    let opts = options(shutdown.clone());
    let run = tokio::spawn(async move {
        KafkaConsumer::new(client)
            .run::<OrdersTopic, _>(handler, (), opts)
            .await
    });
    (run, shutdown)
}

/// Starts a one-member group on the topic under `config`, through the
/// registry path, where the `test-support` seams on the group config apply.
/// Returns the broker to close, the run task and the token that stops it.
async fn start_group<H>(
    client: KafkaClient,
    handler: H,
    config: KafkaConsumerGroupConfig,
) -> (
    Broker<Kafka>,
    tokio::task::JoinHandle<shove::SupervisorOutcome>,
    CancellationToken,
)
where
    H: MessageHandler<OrdersTopic, Context = ()> + Clone + 'static,
{
    let broker = Broker::<Kafka>::from_client(client);
    let mut group = broker.consumer_group();
    group
        .register::<OrdersTopic, _>(ConsumerGroupConfig::new(config), move || handler.clone())
        .await
        .expect("register on the mock cluster");
    let token = group.cancellation_token();
    let run = tokio::spawn(
        group.run_until_timeout(token.clone().cancelled_owned(), Duration::from_secs(60)),
    );
    (broker, run, token)
}

/// Six records, six completions, six OffsetCommit requests: under
/// `PerRecord` every completion is committed, and confirmed, before the next
/// record is handed out, so the broker sees one commit per record and
/// nothing is left for a later drain.
#[tokio::test]
async fn per_record_issues_one_offset_commit_per_completion() {
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2", "o3", "o4", "o5", "o6"]).await;
    mock.track_requests();

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });

    wait_until(
        || handler.count() == 6,
        DELIVERY_TIMEOUT,
        "six records reaching the handler",
    )
    .await;
    wait_for_committed_position(&bootstrap, 6, DELIVERY_TIMEOUT).await;
    assert_eq!(
        mock.offset_commit_requests(),
        6,
        "one OffsetCommit per completion, none merged and none re-offered"
    );

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

/// The control: six records under `Interval` cost one commit inside the
/// window and one at the stop, against the six a `PerRecord` member makes.
/// The window is twice the test's own delivery bound, so every schedule the
/// test accepts falls inside it: the first completion finds the gate open
/// and is committed at once, the rest wait for the window, and the stop's
/// final commit carries them. An exact count inside a window shorter than
/// the bound depended on the runner.
#[tokio::test]
async fn interval_commits_once_inside_the_window_and_once_at_the_stop_over_six_records() {
    const INTERVAL: Duration = DELIVERY_TIMEOUT.saturating_mul(2);
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2", "o3", "o4", "o5", "o6"]).await;
    mock.track_requests();

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::Interval(INTERVAL), token)
    });

    wait_until(
        || mock.offset_commit_requests() >= 1,
        DELIVERY_TIMEOUT,
        "the first completion's commit reaching the broker",
    )
    .await;
    wait_until(
        || handler.count() == 6,
        DELIVERY_TIMEOUT,
        "six records reaching the handler",
    )
    .await;
    assert_eq!(
        mock.offset_commit_requests(),
        1,
        "the completions after the first wait for the window"
    );

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
    assert_eq!(
        mock.offset_commit_requests(),
        2,
        "the final commit carries the rest"
    );
    assert_eq!(committed_position(&bootstrap).await, Some(6));
}

/// A member killed the moment it holds the second record replays nothing of
/// the first on restart. Under `PerRecord` the second record reaches the
/// handler only once the first record's commit is confirmed, so holding it
/// is the proof that the commit landed; the kill is an abort of the run
/// task, with no final commit. A fresh member of the group is handed the
/// held record and nothing before it. A replay of the first record would
/// arrive before the second, in offset order, so the second record's commit
/// landing at the broker bounds the negative: once the position is 2,
/// nothing earlier is still to come.
#[tokio::test]
async fn a_member_killed_holding_the_record_behind_a_confirmed_completion_replays_nothing() {
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2"]).await;

    let holding = Arc::new(Notify::new());
    let handler = HoldAt {
        index: 1,
        earlier: Acking::default(),
        holding: holding.clone(),
    };
    let (run, _shutdown) = start(connect(&bootstrap).await, handler.clone(), |token| {
        options(CommitPolicy::PerRecord, token)
    });

    tokio::time::timeout(DELIVERY_TIMEOUT, holding.notified())
        .await
        .expect("the handler holds the second record");
    assert_eq!(handler.earlier.ids(), vec!["o1".to_string()]);
    run.abort();
    let _ = run.await;
    assert_eq!(
        committed_position(&bootstrap).await,
        Some(1),
        "the first record's commit was confirmed before the second was handed out"
    );

    let restarted = Acking::default();
    let h = restarted.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });
    wait_until(
        || restarted.count() >= 1,
        DELIVERY_TIMEOUT,
        "the restarted member receiving the held record",
    )
    .await;
    wait_for_committed_position(&bootstrap, 2, DELIVERY_TIMEOUT).await;
    assert_eq!(
        restarted.ids(),
        vec!["o2".to_string()],
        "only the record the killed member held is redelivered"
    );

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

/// A rejected commit is re-offered until the fenced consumer detector
/// fires. Every OffsetCommit is refused with an error librdkafka does not
/// retry, so the first completion's commit is rejected at once and
/// re-offered at the default interval, with the assignment paused, until
/// the fence ends the connection. The member then reconnects, joins the
/// group again and, with nothing committed, is handed the first record
/// again: that second delivery is the proof the fence fired, and nothing
/// else was handed out before it. The floor is lowered through the group
/// config's test seam, so the scenario takes seconds rather than the minute
/// the pinned floor needs.
#[tokio::test]
async fn a_rejected_commit_is_re_offered_until_the_fence_fires() {
    const FLOOR: Duration = Duration::from_secs(5);
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2"]).await;
    mock.track_requests();
    mock.reject_commits(512);

    let handler = Acking::default();
    let h = handler.clone();
    let (broker, run, token) = start_group(
        connect(&bootstrap).await,
        h,
        KafkaConsumerGroupConfig::new(1..=1)
            .with_prefetch_count(1)
            .with_concurrent_processing(true)
            .with_commit_policy(CommitPolicy::PerRecord)
            .with_commit_fence_floor_for_test(FLOOR),
    )
    .await;

    wait_until(
        || handler.count() == 1,
        DELIVERY_TIMEOUT,
        "the record reaching the handler",
    )
    .await;
    let first_delivery = handler.arrived_at(0);
    assert_eq!(
        fence_probe::last_threshold(),
        Some(FLOOR),
        "PerRecord judges rejected commits by the floor"
    );

    wait_until(
        || handler.count() >= 2,
        FLOOR + DELIVERY_TIMEOUT,
        "the reconnected member receiving the first record again",
    )
    .await;
    let until_fence = handler
        .arrived_at(1)
        .saturating_duration_since(first_delivery);
    assert!(
        until_fence >= FLOOR,
        "the fence waits out its floor, fired after {until_fence:?}"
    );
    assert_eq!(
        &handler.ids()[..2],
        &["o1".to_string(), "o1".to_string()],
        "nothing is handed out behind the rejected position; the reconnect replays it"
    );
    assert!(
        mock.offset_commit_requests() >= 3,
        "the rejected commit was re-offered, requests: {}",
        mock.offset_commit_requests()
    );
    // The re-offers are spaced by the default interval the page states,
    // 500 ms, so the loop polls between them: every gap between two
    // OffsetCommit requests at the broker is at least that, less a margin
    // for the clock. Read before the stop, whose final commit follows the
    // last re-offer at no such distance.
    let stamps = mock.request_timestamps(RDKafkaApiKey::OffsetCommit);
    for pair in stamps.windows(2) {
        let gap = Duration::from_micros(
            u64::try_from(pair[1] - pair[0]).expect("timestamps in arrival order"),
        );
        assert!(
            gap >= Duration::from_millis(450),
            "re-offers are spaced by the default interval, gap {gap:?} in {stamps:?}"
        );
    }
    assert_eq!(
        committed_position(&bootstrap).await,
        None,
        "no commit was accepted"
    );

    // The fence reconnected the member rather than ending it, which the
    // second delivery above proved. The stop then makes the final commit,
    // the mock refuses it like every other, and the member reports that
    // one refusal under `errors` instead of ending clean.
    token.cancel();
    let outcome = run.await.expect("the run task completes");
    assert_eq!(outcome.errors, 1, "{outcome:?}");
    assert_eq!(outcome.panics, 0, "{outcome:?}");
    assert!(!outcome.timed_out, "{outcome:?}");
    broker.close().await;
}

/// A rejected commit is re-offered until one is accepted, and only then is
/// the next record handed out. The mock refuses the first two OffsetCommit
/// requests and accepts the third, and the handler holds the second record
/// long enough to count the requests while it is held: three for the first
/// record, one per attempt, then one for the second.
#[tokio::test]
async fn a_rejected_commit_recovers_on_a_re_offer_before_the_next_record_is_handed_out() {
    const HOLD: Duration = Duration::from_secs(2);
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2"]).await;
    mock.track_requests();
    mock.reject_commits(2);

    let handler = Acking {
        hold: Some((1, HOLD)),
        ..Acking::default()
    };
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });

    wait_until(
        || handler.count() == 2,
        DELIVERY_TIMEOUT,
        "both records reaching the handler",
    )
    .await;
    assert_eq!(
        mock.offset_commit_requests(),
        3,
        "two refused attempts and the accepted re-offer, before the second record"
    );
    assert_eq!(committed_position(&bootstrap).await, Some(1));

    wait_for_committed_position(&bootstrap, 2, HOLD + DELIVERY_TIMEOUT).await;
    assert_eq!(mock.offset_commit_requests(), 4);
    assert_eq!(handler.ids(), vec!["o1".to_string(), "o2".to_string()]);

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

/// A commit no thread could carry is never taken as confirmed. With every
/// thread refused, the first record's commit reaches no broker, the
/// position stays unset and the next record is held back; once threads are
/// allowed again the re-offer lands and the member goes on, with one commit
/// per record at the broker.
#[tokio::test]
async fn a_commit_without_a_thread_is_not_confirmed_and_is_re_offered() {
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2"]).await;
    mock.track_requests();
    final_commit_spawn_probe::refuse_threads(true);

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });

    wait_until(
        || handler.count() == 1,
        DELIVERY_TIMEOUT,
        "the record reaching the handler",
    )
    .await;
    assert_eq!(
        committed_position(&bootstrap).await,
        None,
        "a commit without a thread confirms nothing"
    );
    assert_eq!(mock.offset_commit_requests(), 0);
    assert_eq!(handler.ids(), vec!["o1".to_string()]);

    final_commit_spawn_probe::refuse_threads(false);
    wait_for_committed_position(&bootstrap, 2, DELIVERY_TIMEOUT).await;
    assert_eq!(handler.ids(), vec!["o1".to_string(), "o2".to_string()]);
    assert_eq!(
        mock.offset_commit_requests(),
        2,
        "the refused attempts sent nothing; one commit per record once threads were allowed"
    );

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

/// A record dropped before the handler, here one the codec cannot decode,
/// is committed like a completion and holds the next record back the same
/// way. Every commit thread is refused, so the drop's commit confirms
/// nothing and is re-offered with the assignment paused: the valid record
/// behind it must not reach the handler meanwhile. Once threads are allowed
/// the re-offer lands, and the valid record arrives with the drop's
/// position already committed. Red before: the drop ran no pause, so the
/// valid record reached the handler with nothing committed.
#[tokio::test]
async fn a_record_dropped_before_the_handler_holds_the_next_record_until_its_commit_is_accepted() {
    /// Four re-offer windows: long enough for the drop's commit to be
    /// re-offered, short enough to keep the test to its scenario.
    const HOLD: Duration = Duration::from_secs(2);
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce_raw(&bootstrap, b"not the topic's json".to_vec()).await;
    produce(&bootstrap, &["o2"]).await;
    mock.track_requests();
    final_commit_spawn_probe::refuse_threads(true);

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });

    // The drop's commit is attempted, has no thread, and is re-offered;
    // nothing reaches the broker and the valid record waits behind it.
    wait_until(
        || per_record_probe::commit_attempts() >= 1,
        DELIVERY_TIMEOUT,
        "the drop's commit being attempted",
    )
    .await;
    tokio::time::sleep(HOLD).await;
    assert_eq!(
        handler.count(),
        0,
        "the valid record waits behind the unconfirmed drop: {:?}",
        handler.ids()
    );
    assert_eq!(
        mock.offset_commit_requests(),
        0,
        "no thread, so nothing reached the broker"
    );
    assert_eq!(committed_position(&bootstrap).await, None);
    assert!(
        per_record_probe::commit_attempts() >= 2,
        "the drop's commit was re-offered meanwhile, attempts {}",
        per_record_probe::commit_attempts()
    );

    // Threads again: the re-offer lands, the assignment resumes, and the
    // valid record arrives behind a committed position.
    final_commit_spawn_probe::refuse_threads(false);
    wait_until(
        || handler.count() == 1,
        DELIVERY_TIMEOUT,
        "the valid record reaching the handler",
    )
    .await;
    assert_eq!(handler.ids(), vec!["o2".to_string()]);
    assert!(
        committed_position(&bootstrap).await.unwrap_or(0) >= 1,
        "the drop's position was committed before the valid record was handed out"
    );
    wait_for_committed_position(&bootstrap, 2, DELIVERY_TIMEOUT).await;
    assert_eq!(
        mock.offset_commit_requests(),
        2,
        "the drop's re-offer that landed, and the valid record's commit"
    );

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

/// The same drop with its commit rejected twice by the broker: the valid
/// record waits while the position is re-offered, and arrives once the
/// third attempt is accepted, behind a committed position. The valid record
/// is produced only once the first rejection is at the broker, so it meets
/// an idle member between re-offers and not the commit wait, whose own
/// put-back would pause for it. Red before: the drop ran no pause and the
/// rejection paused nothing, so the valid record reached the handler while
/// the drop's position was still rejected.
#[tokio::test]
async fn a_record_dropped_before_the_handler_holds_the_next_record_while_its_commit_is_rejected() {
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce_raw(&bootstrap, b"not the topic's json".to_vec()).await;
    mock.track_requests();
    mock.reject_commits(2);

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });

    // The first rejection is at the broker; the valid record arrives behind
    // a rejected position, with the member between re-offers.
    wait_until(
        || mock.offset_commit_requests() >= 1,
        DELIVERY_TIMEOUT,
        "the drop's commit being rejected once",
    )
    .await;
    produce(&bootstrap, &["o2"]).await;

    // The second rejection, and the valid record still waiting.
    wait_until(
        || mock.offset_commit_requests() >= 2,
        DELIVERY_TIMEOUT,
        "the drop's commit being rejected twice",
    )
    .await;
    assert_eq!(
        handler.count(),
        0,
        "the valid record waits while the drop's position is rejected: {:?}",
        handler.ids()
    );
    assert_eq!(committed_position(&bootstrap).await, None);

    // The third attempt is accepted, and the valid record follows it.
    wait_until(
        || handler.count() == 1,
        DELIVERY_TIMEOUT,
        "the valid record reaching the handler",
    )
    .await;
    assert_eq!(handler.ids(), vec!["o2".to_string()]);
    assert!(
        committed_position(&bootstrap).await.unwrap_or(0) >= 1,
        "the drop's position was accepted before the valid record was handed out"
    );
    wait_for_committed_position(&bootstrap, 2, DELIVERY_TIMEOUT).await;
    assert_eq!(
        mock.offset_commit_requests(),
        4,
        "two rejections and the accepted re-offer for the drop, then the valid record's commit"
    );

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

/// A commit whose answer is late holds the next record back, and the member
/// keeps polling while a handler holds its one permit. The member runs with
/// the shortest poll interval librdkafka admits and the first record's
/// handler holds it past that interval: a loop that stopped polling while
/// the permit is held would leave the group, rejoin, and be handed the
/// first record again. The mock then holds the commit's answer, for less
/// than the session timeout, because the mock answers a connection in
/// order and a longer hold would starve the heartbeats behind it. The
/// handler sees each record once, the second only after the answer, and
/// both commits land.
#[tokio::test]
async fn a_late_answer_holds_the_next_record_while_the_member_keeps_polling() {
    const HANDLER_HOLD: Duration = Duration::from_secs(12);
    const ANSWER_HOLD: Duration = Duration::from_secs(6);
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2"]).await;
    mock.track_requests();
    mock.delay_next_offset_commit_answer(ANSWER_HOLD);

    let handler = Acking {
        hold: Some((0, HANDLER_HOLD)),
        ..Acking::default()
    };
    let h = handler.clone();
    let (broker, run, token) = start_group(
        connect(&bootstrap).await,
        h,
        KafkaConsumerGroupConfig::new(1..=1)
            .with_prefetch_count(1)
            .with_concurrent_processing(true)
            .with_commit_policy(CommitPolicy::PerRecord)
            .with_max_poll_interval_for_test(SHORT_POLL_INTERVAL),
    )
    .await;

    wait_until(
        || handler.count() == 2,
        HANDLER_HOLD + ANSWER_HOLD + DELIVERY_TIMEOUT,
        "both records reaching the handler",
    )
    .await;
    let gap = handler
        .arrived_at(1)
        .saturating_duration_since(handler.arrived_at(0));
    assert!(
        gap >= HANDLER_HOLD + ANSWER_HOLD - Duration::from_millis(500),
        "the second record waits for the handler and then for the answer, gap {gap:?}"
    );
    wait_for_committed_position(&bootstrap, 2, DELIVERY_TIMEOUT).await;
    assert_eq!(
        handler.ids(),
        vec!["o1".to_string(), "o2".to_string()],
        "each record once: the member kept its membership through both waits"
    );
    assert_eq!(
        mock.offset_commit_requests(),
        2,
        "one commit per record, the late one not re-offered"
    );

    token.cancel();
    let outcome = run.await.expect("the run task completes");
    assert!(outcome.is_clean(), "{outcome:?}");
    broker.close().await;
}

/// A stop that lands while a commit is in flight waits for that commit's
/// answer, inside the shutdown deadline, and only then makes the final
/// commit, which carries the position again as every final commit does;
/// `run` returns only once the consumer has closed, so the member has left
/// its group when it does. The mock holds the first OffsetCommit answer,
/// the stop fires once that request has reached the broker, and the request
/// counts at return say what happened.
#[tokio::test]
async fn a_stop_during_a_commit_waits_for_its_answer_and_leaves_the_group_before_returning() {
    const HELD: Duration = Duration::from_secs(6);
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1"]).await;
    mock.track_requests();
    mock.delay_next_offset_commit_answer(HELD);

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });

    wait_until(
        || mock.offset_commit_requests() >= 1,
        DELIVERY_TIMEOUT,
        "the record's commit reaching the broker",
    )
    .await;
    let stopped_at = Instant::now();
    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
    let took = stopped_at.elapsed();

    assert!(
        took >= HELD - Duration::from_millis(500),
        "the stop waits for the commit in flight, took {took:?}"
    );
    assert!(
        took < shutdown_commit_deadline_for_test(),
        "the stop ends inside the shutdown deadline, took {took:?}"
    );
    assert_eq!(
        mock.offset_commit_requests(),
        2,
        "the commit in flight is waited for, then the final commit carries the position again"
    );
    assert!(
        mock.requests_of(RDKafkaApiKey::LeaveGroup) >= 1,
        "the member has left the group when run returns"
    );
    assert_eq!(committed_position(&bootstrap).await, Some(1));
}

/// Kafka orders OffsetCommit requests within one connection only, so a
/// commit librdkafka abandoned on a socket timeout and retried can be
/// applied by the broker after a later one, and no client can prevent that;
/// the Kafka page cites the lines. What it costs can be shown: the member
/// is killed while it holds the third record, with two committed, and the
/// group's position is then moved back from 2 to 1, as a late commit would
/// move it. The restarted member replays the record behind the moved-back
/// position and loses nothing, and its own next commit repairs the
/// position; a position moved back further replays further, by the same
/// mechanism.
#[tokio::test]
async fn a_position_moved_back_behind_the_member_costs_a_replay_and_never_a_loss() {
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2", "o3"]).await;

    let holding = Arc::new(Notify::new());
    let handler = HoldAt {
        index: 2,
        earlier: Acking::default(),
        holding: holding.clone(),
    };
    let (run, _shutdown) = start(connect(&bootstrap).await, handler.clone(), |token| {
        options(CommitPolicy::PerRecord, token)
    });
    tokio::time::timeout(DELIVERY_TIMEOUT, holding.notified())
        .await
        .expect("the handler holds the third record");
    assert_eq!(
        handler.earlier.ids(),
        vec!["o1".to_string(), "o2".to_string()],
        "two records committed, the third held"
    );
    assert_eq!(committed_position(&bootstrap).await, Some(2));
    run.abort();
    let _ = run.await;

    move_committed_position(&bootstrap, 1).await;
    assert_eq!(
        committed_position(&bootstrap).await,
        Some(1),
        "the position moved back, as a late commit would move it"
    );

    let restarted = Acking::default();
    let h = restarted.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });
    wait_for_committed_position(&bootstrap, 3, DELIVERY_TIMEOUT).await;
    assert_eq!(
        restarted.ids(),
        vec!["o2".to_string(), "o3".to_string()],
        "the record behind the moved-back position replays, nothing is lost, and the position is repaired"
    );

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

/// A `Retry` whose republish fails under `PerRecord` ends the member rather
/// than leave the record pinned behind the next one: with its offset in
/// flight and never completed, every later commit would confirm nothing and
/// a crash would replay everything behind it. On Kafka a retry republishes
/// the record onto its own topic after the hold delay; the mock refuses
/// every Produce request from the moment the records are in, so that
/// republish fails. The member ends with a connection error that names the
/// republish, the second record was never handed out, and no commit reached
/// the broker.
#[tokio::test]
async fn a_republish_that_fails_under_per_record_ends_the_member_before_the_next_record() {
    const RETRY_TOPIC: &str = "kafka-commit-policy-retry";
    shove::define_topic!(
        RetryTopic,
        Order,
        TopologyBuilder::new(RETRY_TOPIC)
            .hold_queue(Duration::from_millis(200))
            .build()
    );

    /// Records every record it is handed and asks for a retry each time.
    #[derive(Clone, Default)]
    struct Retrying {
        seen: Arc<Mutex<Vec<String>>>,
    }
    impl MessageHandler<RetryTopic> for Retrying {
        type Context = ();
        async fn handle(&self, msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
            self.seen
                .lock()
                .expect("handler mutex poisoned")
                .push(msg.id);
            Outcome::Retry
        }
    }

    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    mock.api()
        .create_topic(RETRY_TOPIC, 1, 1)
        .expect("create the retry topic through the mock API");
    produce_on(&bootstrap, RETRY_TOPIC, &["o1", "o2"]).await;
    mock.track_requests();
    // Every Produce from here on is refused with an error the producer does
    // not retry, so the republish fails on each of its attempts.
    mock.api().request_errors(
        RDKafkaApiKey::Produce,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED; 32],
    );

    let handler = Retrying::default();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let token = shutdown.clone();
    let client = connect(&bootstrap).await;
    let run = tokio::spawn(async move {
        KafkaConsumer::new(client)
            .run::<RetryTopic, _>(
                h,
                (),
                options(CommitPolicy::PerRecord, token).with_max_reconnect_attempts(1),
            )
            .await
    });

    let result = tokio::time::timeout(DELIVERY_TIMEOUT, run)
        .await
        .expect("the member ends within the bound")
        .expect("the run task completes");
    let err = result.expect_err("a failed republish ends the member");
    assert!(
        matches!(&err, ShoveError::Connection(msg)
            if msg.contains("retry republish") && msg.contains("CommitPolicy::PerRecord")),
        "the error names the republish and the policy: {err}"
    );
    assert_eq!(
        *handler.seen.lock().expect("handler mutex poisoned"),
        vec!["o1".to_string()],
        "the second record was never handed out"
    );
    assert_eq!(
        mock.offset_commit_requests(),
        0,
        "nothing was committed behind the record"
    );
    drop(shutdown);
}

// ---------------------------------------------------------------------------
// A partition handed back paused: the assign drained at the top of a pass, or
// by the receive arm
// ---------------------------------------------------------------------------

const RETURN_TOPIC: &str = "kafka-commit-policy-mock-return";
/// The group the topic's default configuration joins: `{queue}-consumer`.
const RETURN_GROUP_ID: &str = "kafka-commit-policy-mock-return-consumer";
/// Two partitions, so that a second member's join moves exactly one.
const RETURN_PARTITIONS: [i32; 2] = [0, 1];
/// The per-record member's payload limit; a record padded past it is
/// dropped before the handler.
const SIZE_LIMIT: usize = 1024;
/// The returned partition's record reaches the handler in well under this
/// once the partition is resumed, and never while it stays paused.
const RESUME_TIMEOUT: Duration = Duration::from_secs(15);
/// Cycles the receive-arm test runs before it gives up on meeting an assign
/// in the receive arm; the housekeeping tick wins one in a hundred or so.
const RECEIVE_ARM_ATTEMPTS: u32 = 3;

shove::define_topic!(
    ReturnTopic,
    Order,
    TopologyBuilder::new(RETURN_TOPIC).external().build()
);

/// Acknowledges every record, records its id with the partition it came
/// from, and holds every `hold-{round}` record until `release` has reached
/// that round.
#[derive(Clone)]
struct Holding {
    seen: Arc<Mutex<Vec<(String, i32)>>>,
    release: watch::Receiver<u32>,
}

impl Holding {
    fn new(release: watch::Receiver<u32>) -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
            release,
        }
    }

    fn seen(&self) -> Vec<(String, i32)> {
        self.seen.lock().expect("handler mutex poisoned").clone()
    }

    fn has(&self, id: &str) -> bool {
        self.seen().iter().any(|(seen, _)| seen == id)
    }
}

impl MessageHandler<ReturnTopic> for Holding {
    type Context = ();
    async fn handle(&self, msg: Order, meta: MessageMetadata, _: &()) -> Outcome {
        let partition = meta.partition.expect("Kafka fills the partition");
        self.seen
            .lock()
            .expect("handler mutex poisoned")
            .push((msg.id.clone(), partition));
        if let Some(round) = msg.id.strip_prefix("hold-") {
            let round: u32 = round.parse().expect("a hold record names its round");
            let mut release = self.release.clone();
            release
                .wait_for(|released| *released >= round)
                .await
                .expect("the release sender outlives the handler");
        }
        Outcome::Ack
    }
}

/// Produces one record pinned to `partition` of the return topic and
/// returns the offset the broker gave it.
async fn produce_pinned(bootstrap: &str, id: &str, partition: i32) -> i64 {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("mock producer");
    let payload = serde_json::to_vec(&Order { id: id.into() }).expect("encode the record");
    let delivery = producer
        .send(
            FutureRecord::<(), Vec<u8>>::to(RETURN_TOPIC)
                .partition(partition)
                .payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .expect("produce to the mock cluster");
    assert_eq!(
        delivery.partition, partition,
        "the record lands where it was pinned"
    );
    delivery.offset
}

/// Starts a direct consumer on the return topic with `handler` and
/// `options`, and returns its run task and the token that stops it.
fn start_on_return_topic(
    client: KafkaClient,
    handler: Holding,
    options: impl FnOnce(CancellationToken) -> ConsumerOptions<Kafka>,
) -> (
    tokio::task::JoinHandle<Result<(), ShoveError>>,
    CancellationToken,
) {
    let shutdown = CancellationToken::new();
    let opts = options(shutdown.clone());
    let run = tokio::spawn(async move {
        KafkaConsumer::new(client)
            .run::<ReturnTopic, _>(handler, (), opts)
            .await
    });
    (run, shutdown)
}

/// Polls the broker until the group's committed position on `partition` of
/// the return topic is `expected`.
async fn wait_for_return_position(bootstrap: &str, partition: i32, expected: i64, what: &str) {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    while committed_position_on(bootstrap, RETURN_TOPIC, RETURN_GROUP_ID, partition).await
        != Some(expected)
    {
        assert!(
            Instant::now() < deadline,
            "{what}: partition {partition} did not reach position {expected} within {DELIVERY_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The two-partition return topic with a per-record member A on it, and the
/// cycle that hands A a partition back paused: A holds `hold-{round}` on
/// partition 0 and is paused over both partitions, B joins and the
/// cooperative rebalance moves one partition to it, A's hold ends and A
/// works through what it kept, B leaves, and the moved partition returns to
/// an idle, unpaused A with the pause flag it left with. `produced` counts
/// the records on each partition, so a committed position can be waited
/// for whoever consumed the records.
struct ReturnCycle {
    /// Lives for the cluster; the broker is reached through `bootstrap`.
    _mock: Mock,
    bootstrap: String,
    a: Holding,
    run_a: tokio::task::JoinHandle<Result<(), ShoveError>>,
    token_a: CancellationToken,
    release: watch::Sender<u32>,
    produced: [i64; 2],
}

impl ReturnCycle {
    async fn start() -> Self {
        let mock = Mock::start();
        let bootstrap = mock.bootstrap();
        mock.api()
            .create_topic(RETURN_TOPIC, 2, 1)
            .expect("create the return topic through the mock API");
        mock.track_requests();
        let (release, released) = watch::channel(0);
        // Member A: a commit per record, one permit, and a payload limit a
        // padded record exceeds.
        let a = Holding::new(released);
        let (run_a, token_a) =
            start_on_return_topic(connect(&bootstrap).await, a.clone(), |token| {
                options(CommitPolicy::PerRecord, token).with_max_message_size(SIZE_LIMIT)
            });
        Self {
            _mock: mock,
            bootstrap,
            a,
            run_a,
            token_a,
            release,
            produced: [0, 0],
        }
    }

    fn produced_on(&self, partition: i32) -> i64 {
        self.produced[usize::try_from(partition).expect("a partition index")]
    }

    async fn produce(&mut self, id: &str, partition: i32) {
        produce_pinned(&self.bootstrap, id, partition).await;
        self.produced[usize::try_from(partition).expect("a partition index")] += 1;
    }

    /// One cycle, see the type's doc. Returns once A's assign event for the
    /// returned partition has been queued, with the moved and the kept
    /// partition.
    async fn hand_a_partition_back_paused(&mut self, round: u32) -> (i32, i32) {
        let hold = format!("hold-{round}");
        self.produce(&hold, 0).await;
        wait_until(
            || self.a.has(&hold),
            DELIVERY_TIMEOUT,
            "A holding its record",
        )
        .await;
        // One probe per partition, unconsumed while A is paused: whichever
        // partition moves to B, B's first record comes from it.
        for partition in RETURN_PARTITIONS {
            self.produce(&format!("probe-{round}-{partition}"), partition)
                .await;
        }

        // Member B joins, and the cooperative rebalance moves one partition
        // to it while A is paused.
        let b = Holding::new(self.release.subscribe());
        let (run_b, token_b) =
            start_on_return_topic(connect(&self.bootstrap).await, b.clone(), |token| {
                ConsumerOptions::<Kafka>::new().with_shutdown(token)
            });
        wait_until(
            || !b.seen().is_empty(),
            DELIVERY_TIMEOUT,
            "the rebalance moving a partition to B",
        )
        .await;
        let moved = b.seen()[0].1;
        let kept = RETURN_PARTITIONS
            .into_iter()
            .find(|&partition| partition != moved)
            .expect("two partitions");

        // The holds end. A commits what it kept and resumes its assignment,
        // which no longer holds the moved partition: that one keeps A's
        // pause flag. A then works through the kept partition, a commit per
        // record, and idles unpaused once the last is committed.
        self.release
            .send(round)
            .expect("the handlers hold the receiver");
        wait_for_return_position(
            &self.bootstrap,
            kept,
            self.produced_on(kept),
            "A working through the kept partition",
        )
        .await;

        // B leaves, and its partition comes back to A, paused. A's rejoin
        // ends with the assign the rebalance callback queues for the loop,
        // which the probe counts.
        let queued = per_record_probe::assigns_queued();
        token_b.cancel();
        run_b
            .await
            .expect("B's task completes")
            .expect("B ends clean");
        wait_until(
            || per_record_probe::assigns_queued() > queued,
            DELIVERY_TIMEOUT,
            "A's assign being queued",
        )
        .await;
        (moved, kept)
    }

    /// Drops an oversize record on `kept` before the handler, and waits for
    /// its drop to be committed: the next thing through A's `recv()`.
    async fn drop_an_oversize_record_on(&mut self, kept: i32) {
        self.produce(&"x".repeat(SIZE_LIMIT), kept).await;
        wait_for_return_position(
            &self.bootstrap,
            kept,
            self.produced_on(kept),
            "the oversize record's drop being committed",
        )
        .await;
        assert!(
            !self.a.seen().iter().any(|(id, _)| id.len() >= SIZE_LIMIT),
            "the oversize record never reached the handler"
        );
    }

    /// Produces `id` on the returned partition and waits for A to receive
    /// it: the proof that the partition was resumed.
    async fn prove_resumed(&mut self, moved: i32, id: &str) {
        self.produce(id, moved).await;
        wait_until(
            || self.a.has(id),
            RESUME_TIMEOUT,
            "the record on the returned partition reaching A",
        )
        .await;
    }

    async fn stop(self) {
        self.token_a.cancel();
        self.run_a
            .await
            .expect("A's task completes")
            .expect("A ends clean");
    }
}

/// A partition handed back to a per-record member that idles unpaused
/// arrives paused: the member paused its whole assignment while it held a
/// record, and librdkafka keeps a partition's pause flag across a revoke.
/// The assign event is drained by whichever arm runs next, and when the
/// next thing through `recv()` is a record on the member's other partition,
/// that is the receive arm. A record dropped before the handler, here one
/// over the size limit, is taken like any other: the pause the moment it is
/// taken and the resume once its drop is committed cover the whole
/// assignment, the returned partition included. The record produced onto
/// the returned partition afterwards is the proof: it reaches the handler
/// only once the partition is resumed. Red without the pause at the take,
/// where the drop ran no cycle and the partition stayed paused.
///
/// The drain-site probe says which drain took the assign. The oversize
/// record is produced as soon as the assign is queued, so the receive arm
/// drains it unless the housekeeping tick fires first, in which case the
/// cycle runs again; the top-of-pass path has its own test below.
#[tokio::test]
async fn a_partition_handed_back_paused_is_resumed_when_the_receive_arm_drains_the_assign() {
    let mut cycle = ReturnCycle::start().await;
    let mut met_in_the_receive_arm = false;
    for round in 1..=RECEIVE_ARM_ATTEMPTS {
        let (moved, kept) = cycle.hand_a_partition_back_paused(round).await;
        let at_top_of_pass = per_record_probe::reconciled_at_top_of_pass();
        let at_receive_arm = per_record_probe::assigns_drained_at_receive_arm();
        cycle.drop_an_oversize_record_on(kept).await;
        if per_record_probe::assigns_drained_at_receive_arm() > at_receive_arm {
            // The receive arm drained the assign ahead of the oversize
            // record, whose pause and resume cycle resumed the returned
            // partition with the rest.
            cycle
                .prove_resumed(moved, &format!("after-return-{round}"))
                .await;
            met_in_the_receive_arm = true;
            break;
        }
        assert!(
            per_record_probe::reconciled_at_top_of_pass() > at_top_of_pass,
            "the assign was reconciled by one drain or the other"
        );
        // The housekeeping tick came first and the top-of-pass drain took
        // the assign; the partition is resumed all the same. Again.
        cycle
            .prove_resumed(moved, &format!("after-return-{round}"))
            .await;
    }
    assert!(
        met_in_the_receive_arm,
        "the receive arm drained no assign in {RECEIVE_ARM_ATTEMPTS} cycles"
    );
    cycle.stop().await;
}

/// The same return with nothing through `recv()` afterwards: the assign
/// waits in the channel until the housekeeping tick, the one arm due on an
/// idle member, runs a pass, and the drain at the top of that pass
/// reconciles it. The record produced onto the returned partition after the
/// tick is the proof it was resumed, and the drain-site probe says which
/// drain did it. Red without the top-of-pass call: the assign is drained
/// and discarded there, and the record never arrives.
#[tokio::test]
async fn a_partition_handed_back_paused_is_resumed_when_a_pass_drains_the_assign() {
    let mut cycle = ReturnCycle::start().await;
    let (moved, _kept) = cycle.hand_a_partition_back_paused(1).await;
    let at_top_of_pass = per_record_probe::reconciled_at_top_of_pass();
    let at_receive_arm = per_record_probe::assigns_drained_at_receive_arm();

    // Nothing is produced, so the next pass is the housekeeping tick's.
    tokio::time::sleep(housekeeping_interval_for_test() + Duration::from_secs(1)).await;
    assert!(
        per_record_probe::reconciled_at_top_of_pass() > at_top_of_pass,
        "the housekeeping pass drained the assign and reconciled it"
    );
    assert_eq!(
        per_record_probe::assigns_drained_at_receive_arm(),
        at_receive_arm,
        "nothing came through recv meanwhile"
    );

    cycle.prove_resumed(moved, "after-return").await;
    cycle.stop().await;
}

// ---------------------------------------------------------------------------
// The discard a dropped record carries, counted once, on the commit that
// retires it
// ---------------------------------------------------------------------------

/// `shove_messages_discarded_total` for `topic`, summed over its reasons,
/// from one draining snapshot of the debugging recorder.
#[cfg(feature = "metrics")]
fn discarded_total(snapshotter: &Snapshotter, topic: &str) -> u64 {
    snapshotter
        .snapshot()
        .into_hashmap()
        .iter()
        .filter(|(key, _)| key.key().name() == "shove_messages_discarded_total")
        .filter(|(key, _)| {
            key.key()
                .labels()
                .any(|label| label.key() == "topic" && label.value() == topic)
        })
        .map(|(_, (_, _, value))| match value {
            DebugValue::Counter(n) => *n,
            other => panic!("shove_messages_discarded_total is not a counter: {other:?}"),
        })
        .sum()
}

/// Installs the debugging recorder as the process's global recorder; one
/// per test process, which nextest gives every test.
#[cfg(feature = "metrics")]
fn install_recorder() -> Snapshotter {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install the debugging recorder");
    snapshotter
}

/// A record dropped before the handler on a topic with no DLQ is gone for
/// good once its position is committed, and `messages_discarded_total`
/// counts it then, once: the drop's commit under `PerRecord` is accepted at
/// once, and the counter moves on its confirmation.
#[cfg(feature = "metrics")]
#[tokio::test]
async fn a_dropped_records_discard_is_counted_once_when_its_commit_is_accepted() {
    let snapshotter = install_recorder();
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce_raw(&bootstrap, b"not the topic's json".to_vec()).await;
    produce(&bootstrap, &["o2"]).await;
    mock.track_requests();

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });
    wait_for_committed_position(&bootstrap, 2, DELIVERY_TIMEOUT).await;
    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");

    assert_eq!(
        discarded_total(&snapshotter, TOPIC),
        1,
        "the dropped record is counted once its commit is accepted"
    );
}

/// The same drop with its commit rejected twice: the discard rides each
/// re-offer and is counted once, on the third attempt the broker accepts,
/// never on a rejection and never twice.
#[cfg(feature = "metrics")]
#[tokio::test]
async fn a_dropped_records_discard_is_counted_once_when_a_re_offer_is_accepted() {
    let snapshotter = install_recorder();
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce_raw(&bootstrap, b"not the topic's json".to_vec()).await;
    mock.track_requests();
    mock.reject_commits(2);

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });
    wait_until(
        || mock.offset_commit_requests() >= 2,
        DELIVERY_TIMEOUT,
        "the drop's commit being rejected twice",
    )
    .await;
    produce(&bootstrap, &["o2"]).await;
    wait_for_committed_position(&bootstrap, 2, DELIVERY_TIMEOUT).await;
    assert_eq!(
        mock.offset_commit_requests(),
        4,
        "two rejections, the accepted re-offer, and the valid record's commit"
    );
    // The stop's final commit carries the position again and confirms no
    // discard: the one discard was settled by the re-offer that landed.
    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");

    assert_eq!(
        discarded_total(&snapshotter, TOPIC),
        1,
        "the discard is counted once, on the re-offer the broker accepted"
    );
}

/// The drop's commit is in flight when the stop lands and comes back
/// rejected: the shutdown arm hands the discard back, the final commit
/// carries the position again, the broker accepts it, and the discard is
/// counted once on that commit, with the position where the final commit
/// put it.
#[cfg(feature = "metrics")]
#[tokio::test]
async fn a_discard_behind_a_commit_rejected_at_the_stop_is_counted_once_by_the_final_commit() {
    const HELD: Duration = Duration::from_secs(3);
    let snapshotter = install_recorder();
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce_raw(&bootstrap, b"not the topic's json".to_vec()).await;
    mock.track_requests();
    mock.answer_next_offset_commit_late(
        HELD,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED,
    );

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });
    wait_until(
        || mock.offset_commit_requests() >= 1,
        DELIVERY_TIMEOUT,
        "the drop's commit reaching the broker",
    )
    .await;
    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("the final commit is accepted, so the stop is clean");

    assert_eq!(
        mock.offset_commit_requests(),
        2,
        "the commit in flight, rejected late, and the final commit"
    );
    assert_eq!(
        committed_position(&bootstrap).await,
        Some(1),
        "the final commit carried the dropped record's position"
    );
    assert_eq!(
        discarded_total(&snapshotter, TOPIC),
        1,
        "the discard handed back at the stop is counted once, by the final commit"
    );
}

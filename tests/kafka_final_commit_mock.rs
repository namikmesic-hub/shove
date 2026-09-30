//! The final offset commit of a stopping Kafka consumer, against librdkafka's
//! mock cluster (`rdkafka::mocking`), which serves the consumer group
//! protocol and offset commits from a localhost listener and needs no Docker.
//!
//! A member that stops issues one synchronous commit for every partition it
//! holds. When the coordinator rejects that commit, or gives no answer within
//! the shutdown deadline, the member ends with `ShoveError::Commit` instead
//! of a clean exit. The group run counts it once, ends on it, and hands it
//! back typed in `RunReport::fatal`. These tests inject the rejection and the
//! delay through the mock cluster's `OffsetCommit` handling, which a real
//! broker cannot be asked to do.
//!
//! The topic is bound with `external()`, because the mock broker has no
//! CreateTopics API: each test creates the topic through the mock API, as
//! infra would, and the record is produced with a raw rdkafka producer so it
//! carries a key beside its payload.
//!
//! `test-support` gates the deadline seam this file reads; both Kafka
//! coverage rows enable it, so the suite runs in each.

#![cfg(all(feature = "kafka", feature = "test-support"))]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{DefaultProducerContext, FutureProducer, FutureRecord};
use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use shove::broker::Broker;
use shove::consumer_group::ConsumerGroupConfig;
use shove::handler::MessageHandler;
use shove::kafka::{
    KafkaClient, KafkaConfig, KafkaConsumerGroupConfig, shutdown_commit_deadline_for_test,
};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topology::TopologyBuilder;
use shove::{CommitFailure, ConsumerGroup, RunReport, ShoveError};
use tokio::time::Instant;

const TOPIC: &str = "kafka-final-commit-mock";
/// The group the topic's default configuration joins: `{queue}-consumer`.
const GROUP_ID: &str = "kafka-final-commit-mock-consumer";
/// Markers that must never appear in the error a failed commit reports.
const KEY_MARKER: &str = "record-key-marker-7f3a";
const PAYLOAD_MARKER: &str = "record-payload-marker-2c9e";
/// A record reaches the handler on a mock cluster in well under this.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(60);
/// The receive loop's own bound on its final commit, read through the
/// `test-support` seam so this file cannot drift from the constant; the
/// `Deadline` kind carries the value the loop used.
fn shutdown_commit_deadline() -> Duration {
    shutdown_commit_deadline_for_test()
}
/// How long the run may take past the shutdown deadline before the test
/// gives up on it.
const DEADLINE_MARGIN: Duration = Duration::from_secs(20);
/// A drain timeout wide enough that the loop's own commit deadline, not the
/// drain, ends a commit that never answers.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(40);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Order {
    id: String,
    note: String,
}

shove::define_topic!(
    OrdersTopic,
    Order,
    TopologyBuilder::new(TOPIC).external().build()
);

/// Acknowledges every record and counts it, so a test can wait for the
/// delivery with a bound.
#[derive(Clone, Default)]
struct AckingHandler {
    seen: Arc<Mutex<Vec<Order>>>,
}

impl AckingHandler {
    fn count(&self) -> usize {
        self.seen.lock().expect("handler mutex poisoned").len()
    }
}

impl MessageHandler<OrdersTopic> for AckingHandler {
    type Context = ();
    async fn handle(&self, msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
        self.seen.lock().expect("handler mutex poisoned").push(msg);
        Outcome::Ack
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

/// One mock broker with the topic created, as infra would create it.
fn mock_cluster() -> MockCluster<'static, DefaultProducerContext> {
    let mock = MockCluster::new(1).expect("mock cluster");
    mock.create_topic(TOPIC, 1, 1).expect("mock topic");
    mock
}

async fn connect(mock: &MockCluster<'static, DefaultProducerContext>) -> Broker<Kafka> {
    let client = KafkaClient::connect_with_retry(&KafkaConfig::new(mock.bootstrap_servers()), 10)
        .await
        .expect("connect to the mock cluster");
    Broker::<Kafka>::from_client(client)
}

/// A one-member group on the topic whose commits are gated to once an hour:
/// the first completion is committed asynchronously when the gate opens,
/// and the only commit after that is the final one at shutdown, so an answer
/// injected before the stop meets the shutdown commit.
async fn register_group(broker: &Broker<Kafka>) -> (ConsumerGroup<Kafka>, AckingHandler) {
    let handler = AckingHandler::default();
    let h = handler.clone();
    let mut group = broker.consumer_group();
    group
        .register::<OrdersTopic, _>(
            ConsumerGroupConfig::new(
                KafkaConsumerGroupConfig::new(1..=1)
                    .with_commit_interval(Duration::from_secs(3600)),
            ),
            move || h.clone(),
        )
        .await
        .expect("register on the mock cluster");
    (group, handler)
}

/// One record with a key and a payload the assertions can look for, through
/// a raw rdkafka producer so the key is set; the payload is the topic's JSON.
async fn produce_marked_record(bootstrap: &str) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("mock producer");
    let payload = serde_json::to_vec(&Order {
        id: "order-1".into(),
        note: PAYLOAD_MARKER.into(),
    })
    .expect("encode the record");
    producer
        .send(
            FutureRecord::to(TOPIC).key(KEY_MARKER).payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .expect("produce to the mock cluster");
}

/// The group's committed position on the topic's one partition, read
/// through a raw consumer that never joins the group. `None` before the
/// first accepted commit.
async fn committed_position(bootstrap: &str) -> Option<i64> {
    let bootstrap = bootstrap.to_owned();
    tokio::task::spawn_blocking(move || {
        let probe: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", GROUP_ID)
            .create()
            .expect("probe consumer");
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(TOPIC, 0);
        let committed = probe
            .committed_offsets(tpl, Duration::from_secs(5))
            .expect("read the committed offsets");
        committed
            .elements()
            .iter()
            .find(|e| e.partition() == 0)
            .and_then(|e| match e.offset() {
                Offset::Offset(offset) => Some(offset),
                _ => None,
            })
    })
    .await
    .expect("probe task")
}

/// Polls the broker until the group's committed position is `expected`.
async fn wait_for_committed_position(bootstrap: &str, expected: i64) {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    while committed_position(bootstrap).await != Some(expected) {
        assert!(
            Instant::now() < deadline,
            "the broker did not accept the commit at {expected} within {DELIVERY_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Runs the group with the report and `drain_timeout`, delivers the marked
/// record, waits until the broker has accepted the asynchronous commit of
/// its position, applies `before_stop` to the mock cluster, stops the run
/// and returns the report with how long the stop took.
async fn stop_after_one_record(
    mock: &MockCluster<'static, DefaultProducerContext>,
    drain_timeout: Duration,
    before_stop: impl FnOnce(&MockCluster<'static, DefaultProducerContext>),
) -> (RunReport, Duration) {
    let broker = connect(mock).await;
    let (group, handler) = register_group(&broker).await;
    let token = group.cancellation_token();
    let run = tokio::spawn(
        group.run_until_timeout_with_report(token.clone().cancelled_owned(), drain_timeout),
    );

    let bootstrap = mock.bootstrap_servers();
    produce_marked_record(&bootstrap).await;
    wait_until(
        || handler.count() == 1,
        DELIVERY_TIMEOUT,
        "the record reaching the handler",
    )
    .await;
    // The first completion is committed asynchronously as soon as the gate
    // opens. Only once the broker has accepted it does the injection below
    // meet the final commit and no other, and the position it leaves behind
    // is known.
    wait_for_committed_position(&bootstrap, 1).await;

    before_stop(mock);
    let stopped_at = Instant::now();
    token.cancel();
    let report = run.await.expect("the run task completes");
    let took = stopped_at.elapsed();
    broker.close().await;
    (report, took)
}

/// The one fatal error the report carries, checked to name the topic and the
/// acknowledged record's exclusive position on the only partition.
fn the_commit_error(report: &RunReport) -> (&[(i32, i64)], &CommitFailure) {
    assert_eq!(report.fatal.len(), 1, "one fatal error: {:?}", report.fatal);
    let ShoveError::Commit {
        topic,
        offsets,
        kind,
    } = &report.fatal[0]
    else {
        panic!("the fatal error is ShoveError::Commit: {:?}", report.fatal);
    };
    assert_eq!(topic, TOPIC);
    assert_eq!(offsets, &[(0, 1)], "the acknowledged record's position");
    (offsets, kind)
}

/// A rejected OffsetCommit at shutdown ends the member with `Commit` of the
/// `Rejected` kind, and the run reports it: one error, not clean, exit code
/// 1, and the error itself in `fatal` with the broker's answer.
#[tokio::test]
async fn a_rejected_final_commit_ends_the_member_with_a_commit_error_the_run_reports() {
    let mock = mock_cluster();
    let (report, _took) = stop_after_one_record(&mock, DRAIN_TIMEOUT, |mock| {
        // From here every OffsetCommit is refused. Several answers are
        // pushed so a retry meets the same one.
        mock.request_errors(
            RDKafkaApiKey::OffsetCommit,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED; 8],
        );
    })
    .await;

    let (_offsets, kind) = the_commit_error(&report);
    let CommitFailure::Rejected(text) = kind else {
        panic!("a refused commit is reported as Rejected: {kind:?}");
    };
    assert!(
        text.contains("authorization failed") || text.contains("Authorization failed"),
        "the broker's answer is carried: {text}"
    );

    assert_eq!(report.outcome.errors, 1, "{:?}", report.outcome);
    assert_eq!(report.outcome.panics, 0, "{:?}", report.outcome);
    assert!(!report.outcome.timed_out, "{:?}", report.outcome);
    assert!(!report.outcome.is_clean(), "{:?}", report.outcome);
    assert_eq!(report.outcome.exit_code(), 1);

    // What the rejection leaves behind: the position the broker accepted
    // earlier, from which the next member replays. The error says this
    // commit did not land, not that nothing did.
    assert_eq!(
        committed_position(&mock.bootstrap_servers()).await,
        Some(1),
        "the earlier accepted commit stays the group's position"
    );
}

/// The owning run's drain can end before the final commit answers: the
/// member is aborted, and the report still carries the commit, unresolved,
/// with the time the loop waited as its `Deadline`, beside `timed_out`. The
/// mock broker's round-trip time is raised past the drain timeout, so
/// neither the commit nor the drain can finish in time.
#[tokio::test]
async fn a_drain_timeout_before_the_commit_answers_still_reports_the_unresolved_commit() {
    const SHORT_DRAIN: Duration = Duration::from_secs(3);
    let mock = mock_cluster();
    let (report, took) = stop_after_one_record(&mock, SHORT_DRAIN, |mock| {
        mock.broker_round_trip_time(-1, shutdown_commit_deadline() + DEADLINE_MARGIN * 2)
            .expect("raise the mock broker's round-trip time");
    })
    .await;

    assert!(report.outcome.timed_out, "{:?}", report.outcome);
    let (_offsets, kind) = the_commit_error(&report);
    let CommitFailure::Deadline(waited) = kind else {
        panic!("an unresolved commit is reported as Deadline: {kind:?}");
    };
    assert!(
        *waited < shutdown_commit_deadline(),
        "the loop waited only for the drain window, not its own deadline: {waited:?}"
    );
    assert!(
        *waited <= took,
        "the wait is within the stop: {waited:?} of {took:?}"
    );
    assert_eq!(report.outcome.errors, 1, "{:?}", report.outcome);
    assert!(
        took < shutdown_commit_deadline(),
        "the run returned on its drain timeout, took {took:?}"
    );
}

/// The error a rejected commit reports carries the broker's error code and
/// text, and the positions, and nothing of the record behind them: not its
/// payload, not its key. Checked on the `Display` and `Debug` renderings a
/// process would log.
#[tokio::test]
async fn a_rejected_commit_report_carries_no_record_payload_or_key() {
    let mock = mock_cluster();
    let (report, _took) = stop_after_one_record(&mock, DRAIN_TIMEOUT, |mock| {
        mock.request_errors(
            RDKafkaApiKey::OffsetCommit,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED; 8],
        );
    })
    .await;

    let (_offsets, kind) = the_commit_error(&report);
    assert!(matches!(kind, CommitFailure::Rejected(_)), "{kind:?}");
    let rendered = format!("{} {:?} {report:?}", report.fatal[0], report.fatal[0]);
    for marker in [PAYLOAD_MARKER, KEY_MARKER, "order-1"] {
        assert!(
            !rendered.contains(marker),
            "the report must not carry the record's {marker}: {rendered}"
        );
    }
}

/// `run_until_timeout` sees the same failure through its outcome alone: one
/// error and `is_clean()` false, where 0.15 reported a clean run.
#[tokio::test]
async fn run_until_timeout_reports_a_rejected_final_commit_as_one_error() {
    let mock = mock_cluster();
    let broker = connect(&mock).await;
    let (group, handler) = register_group(&broker).await;
    let token = group.cancellation_token();
    let run = tokio::spawn(group.run_until_timeout(token.clone().cancelled_owned(), DRAIN_TIMEOUT));

    produce_marked_record(&mock.bootstrap_servers()).await;
    wait_until(
        || handler.count() == 1,
        DELIVERY_TIMEOUT,
        "the record reaching the handler",
    )
    .await;
    wait_for_committed_position(&mock.bootstrap_servers(), 1).await;

    mock.request_errors(
        RDKafkaApiKey::OffsetCommit,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED; 8],
    );
    token.cancel();
    let outcome = run.await.expect("the run task completes");
    broker.close().await;

    assert_eq!(outcome.errors, 1, "{outcome:?}");
    assert!(!outcome.is_clean(), "{outcome:?}");
    assert_eq!(outcome.exit_code(), 1);
    assert!(!outcome.timed_out, "{outcome:?}");
}

/// An OffsetCommit held past the shutdown deadline ends the member within
/// that deadline, with `Commit` of the `Deadline` kind carrying it: the
/// result is unknown, the detached thread may still land the commit. The
/// mock broker's round-trip time is raised past the deadline just before
/// the stop, so the final `Sync` commit has no answer within it.
#[tokio::test]
async fn a_final_commit_past_the_deadline_ends_the_member_with_the_deadline_kind() {
    let mock = mock_cluster();
    let (report, took) = stop_after_one_record(&mock, DRAIN_TIMEOUT, |mock| {
        mock.broker_round_trip_time(-1, shutdown_commit_deadline() + DEADLINE_MARGIN * 2)
            .expect("raise the mock broker's round-trip time");
    })
    .await;

    let (_offsets, kind) = the_commit_error(&report);
    let CommitFailure::Deadline(deadline) = kind else {
        panic!("a commit without an answer is reported as Deadline: {kind:?}");
    };
    assert_eq!(*deadline, shutdown_commit_deadline());
    assert!(
        took + Duration::from_secs(1) >= *deadline,
        "the member waits out the deadline before it gives up, took {took:?}"
    );
    assert!(
        took < *deadline + DEADLINE_MARGIN,
        "the member returns within the deadline plus a margin, took {took:?}"
    );

    assert_eq!(report.outcome.errors, 1, "{:?}", report.outcome);
    assert!(!report.outcome.is_clean(), "{:?}", report.outcome);
    assert!(!report.outcome.timed_out, "{:?}", report.outcome);
}

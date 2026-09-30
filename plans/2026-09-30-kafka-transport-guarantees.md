# Shove plan 022: six transport guarantees for the Kafka backend

### A1. Summary

Six behaviours move from a downstream consumer into shove, all on the Kafka backend of the `shove` crate.
A broadcast subscription resumes each partition from its own last position after a reconnect.
A member that stops reports a rejected or timed-out final commit as an error instead of a clean exit.
A topic can be bound at run time from a topology value, without a `'static` topology.
A handler panic can end the consumer instead of becoming a `Retry`, and a competing consumer keeps the record unsettled.
A per-record commit mode commits each completion when it lands, not on a one-millisecond gate.
While a record waits in place, no later record of the same partition reaches the handler, on stop or across a rebalance.

The downstream consumer keeps the policy layer only.
That is the group id scheme, the fan-out and worker modes, the success-or-defer handler contract, and the `external()` and no-DLQ defaults.
It also keeps its record decoders, its routing keys, and its lane statistics.

### A2. The six guarantees

#### A2.a Per-partition resume on broadcast reconnect

**What shove does today.**
A broadcast subscription runs inside `run_with_reconnect` (`src/backends/kafka/consumer.rs:6482`).
Each pass of the closure builds a new consumer and assigns every partition with `assign_all_partitions_at` (`consumer.rs:6544-6556`).
The `start` it passes is the configured `BroadcastStart` captured once at `consumer.rs:6439`.
`Tail` and an unset start assign `Offset::End`, and `Head` assigns `Offset::Beginning` (`consumer.rs:2376-2381`).
A `Timestamp` start resolves each partition through `offsets_for_times` (`consumer.rs:2385-2426`).
An idle partition at `Tail` or `Head` therefore holds only a lazy sentinel and no concrete position (`consumer.rs:2351-2382`).
The doc comment states the consequence: a reconnect re-assigns at the configured start as it stands then (`consumer.rs:6407-6411`).
The option doc says the same: "A reconnect re-resolves the same start" (`src/consumer.rs:917-918`).
The broadcast page says Kafka re-assigns at the then-current tail after an outage (`docs/pages/concepts/broadcast.mdx:61` and `:145-146`).
The same page says Redis carries its read position across a reconnect (`broadcast.mdx:61`).
The partition refresh reads the consumer's own `position()` for known partitions (`consumer.rs:2449`), but that consumer is gone after a reconnect.
`BroadcastStart` is `#[non_exhaustive]` so that a later position, such as an offset, can be a new variant (`src/broadcast.rs:35-39`).

**What a consumer must do today.**
A consumer that wants no gap must forbid shove's reconnect, run one subscription per session, and restart the session itself.
It must read every partition's watermark and last record timestamp before it starts, track a floor per partition, and resume at the earliest floor.
It must drop refetched records it already acknowledged before decode, and it depends on `LogAppendTime` on the topic.
Part B names the downstream implementation and its tests.

**Proposed shove change.**
API shape, backend-neutral, on `impl<B: HasBroadcast> ConsumerOptions<B>`:

```rust
/// Where a reconnected broadcast subscription continues.
#[non_exhaustive]
pub enum BroadcastReconnect {
    /// Re-resolve the configured `BroadcastStart`. The Kafka behaviour of 0.15.
    ConfiguredStart,
    /// Continue each partition or stream from its own last position.
    /// The Redis behaviour of 0.15, within what the broker retains.
    Resume,
}

impl<B: HasBroadcast> ConsumerOptions<B> {
    pub fn with_broadcast_reconnect(self, policy: BroadcastReconnect) -> Self;
}
```

Behaviour on Kafka under `Resume`.
The loop keeps a per-partition position map outside the reconnect closure, so it survives a reconnect.
Every assigned partition gets a concrete position when it is assigned, idle partitions included.
A `Tail` start stores each partition's high watermark, and a `Head` start its low watermark, read with `fetch_watermarks` (`consumer.rs:2495-2506`).
A `Timestamp` start already resolves to a concrete offset per partition (`consumer.rs:2420-2421`).
The lazy `Offset::End` and `Offset::Beginning` sentinels (`consumer.rs:2351-2382`) are therefore never a stored position.
A partition the refresh adds gets its concrete position the same way.
The position of a partition is the offset of its unsettled record, else the highest delivered offset plus one, else its stored initial position.
With the prefetch pinned to one (`src/broadcast.rs:294`), at most one record is unsettled per subscription.
A `Defer` keeps its record unsettled through the in-place wait, so a reconnect during the wait resumes at that record.
On reconnect, a known partition is assigned at its stored position, and a new partition at the configured start.
A stored position below the low watermark resolves through the existing watermark helper in `src/backends/kafka/offset_reset.rs`.
The refresh path keeps its current shape (`consumer.rs:2440-2493`).
The promise of `Resume` covers the records the broker still retains: Kafka retention, and the Redis stream's `MAXLEN` bound.
Redis already carries its cursor outside the reconnect closure (`src/backends/redis/broadcast.rs:120-186`, the cursor at `:127`), so `Resume` is its behaviour today.
Redis therefore accepts `Resume` and refuses `ConfiguredStart`, a setting it cannot honour.
An unset option means the backend's 0.15 behaviour: `ConfiguredStart` on Kafka, `Resume` on Redis.
NATS creates its ephemeral consumer with `AckPolicy::None` and `DeliverPolicy::New` (`src/backends/nats/broadcast.rs:297-305`), so a new consumer cannot resume, and NATS refuses `Resume`.
RabbitMQ's queue is `exclusive` and `auto_delete`, so the broker deletes it with the connection (`src/backends/rabbitmq/consumer.rs:157-195`), and RabbitMQ refuses `Resume`.
The in-process broker destroys the subscription's private buffer when its loop ends (`src/backends/inmemory/consumer.rs:468-510`).
It refuses every start but `Tail` today (`src/backends/inmemory/backend.rs:151-157`), and it refuses `Resume` the same way.
Each refusal goes through the existing `check_options` hook (`src/broadcast.rs:262-271`).
NATS can follow additively with a deliver policy by start sequence.
The broadcast page then states per backend what a reconnect replays and what it loses.

Alternative kept open: retain one librdkafka consumer across a broker outage.
librdkafka reconnects on its own with an exponential backoff from `reconnect.backoff.ms`, 100 ms, to `reconnect.backoff.max.ms`, 10 s, and keeps an assigned consumer's positions.
shove instead rebuilds the consumer on every pass of the reconnect closure (`consumer.rs:6482-6556`).
The loop treats every `recv()` error but the group ACL answer as the end of the connection (`consumer.rs:6662-6668`).
`map_kafka_error` sorts those errors (`consumer.rs:2021-2036`): a bad config, a fatal consumption error and a cancelled client become `Topology`, every other error becomes `Connection`.
A `Topology` error is not retryable, so `run_with_reconnect` ends the subscription with it and rebuilds nothing (`consumer.rs:3014-3016`).
Only a `Connection` error rebuilds the consumer, and that class is the transport errors librdkafka reconnects through on its own.
The alternative keeps the consumer through a `Connection` error as a log line, and lets librdkafka reconnect with the assignment intact.
A `Topology` error still ends the subscription, as today, so no `recv()` error then requires a rebuild.
The position map then has no rebuild to serve, so the choice decides how much of part 5 remains.

Compatibility.
Additive when the default stays `ConfiguredStart`.
A default flip to `Resume` is a behaviour change and is an open question (A4, question 1).
Lands in 0.16.0.

**Tests shove gains.**
Unit: the position map takes a delivery, a settlement, and a deferred record, and reports the resume offset per partition.
Unit: a new partition discovered after the reconnect resolves at the configured start, not at a neighbour's position.
Unit: an idle partition keeps its stored initial position, and a resume reads that position, not the then-current tail.
Unit: a stored position of 3 on a partition whose low watermark is 7 resolves to 7 and is logged as retention loss.
Broker harness, with the pause and unpause helpers of `tests/kafka_integration.rs:430-446`:
a subscriber at `Tail` receives records produced during a broker pause once, in order, and receives nothing twice.
The same at `Head` and at `Timestamp`, so a `Head` subscription no longer replays the retained log after a reconnect.
A record deferred at the moment of the reconnect is the first record redelivered after it.
A partition added during the outage is read from the configured start.
An idle partition, with no delivery since the subscribe, receives a record produced on it during the outage.
An empty partition, with no record ever, receives its first record produced during the outage.
The interleaving to drive: deliver offsets 3 and 4 on partition 0, defer 4, pause the broker, produce 5 and 6, and unpause.
Assert `[3, 4, 4, 5, 6]` on partition 0, and the first outage record on an idle partition 1.

#### A2.b A reliable final commit when a member stops

**What shove does today.**
On shutdown the receive loop drains its permits and calls `final_commit_off_runtime` (`consumer.rs:4504-4536`).
The commit runs on a dedicated thread with a 20 s deadline (`consumer.rs:2843-2897`, `src/backends/kafka/constants.rs:167`).
A rejected commit, a spawn failure and a deadline all return `Err` (`consumer.rs:2870-2896`).
The loop logs that error at `warn` and returns `Ok(())` (`consumer.rs:4543-4550`).
`drain_committable` offers a position only on new progress or after a rejected commit (`consumer.rs:361-388`), and the shutdown drain calls it once more (`consumer.rs:4515-4523`).
A position that an earlier asynchronous drain offered is not offered again, so with no new progress the final `Sync` commit has nothing to confirm.
An asynchronous commit still in flight at that moment is then never confirmed by the loop, whatever its result.
The group spawner counts only an `Err` from the loop (`src/backends/kafka/consumer_group.rs:601-607`).
`SupervisorOutcome` therefore reports `errors: 0` and `is_clean()` is true (`src/consumer_supervisor.rs:17-43`).
The Kafka page documents the deadline and says "the batch may be redelivered" (`docs/pages/backends/kafka.mdx:454-456`).
`shutdown_exits_the_process_while_the_broker_is_frozen` proves the process exits within the deadline (`tests/kafka_integration.rs:4790-4802`).
`external_topic_shutdown_during_a_wait_leaves_the_record_uncommitted` asserts a clean outcome after such a shutdown (`tests/kafka_integration.rs:3172-3214`).
No test asserts what the outcome reports when the final commit fails.

**What a consumer must do today.**
A consumer that wants the truth about its final commit must read the group's committed offsets back through a second client after the drain.
It turns an acknowledged offset above the committed one into its own error.
Part B names the downstream implementation and its tests.

**Proposed shove change.**
API shape:

```rust
// src/error.rs, ShoveError is #[non_exhaustive] (src/error.rs:5)
/// The final offset commit of a stopping consumer did not land.
Commit {
    topic: String,
    /// The offsets the consumer tried to commit, per partition.
    offsets: Vec<(i32, i64)>,
    /// Rejected by the coordinator, or unknown after the deadline.
    kind: CommitFailure,
}

#[non_exhaustive]
pub enum CommitFailure { Rejected(String), Deadline(Duration), NoThread }
```

Behaviour.
At shutdown the tracker re-offers every partition's current position, whether or not an earlier drain offered it, through a `drain_all` beside `drain_committable`.
The final `Sync` commit therefore confirms every safe position, including one an in-flight asynchronous commit may not have landed.
A commit of an already committed offset is a broker-side no-op, as the rebalance re-offer relies on today (`consumer.rs:498-502`).
The shutdown arm returns `Err(ShoveError::Commit { .. })` instead of `Ok(())` when the final commit fails.
`is_retryable` stays false for it, so `run_with_reconnect` returns it (`consumer.rs:3014-3016`) instead of swallowing it after shutdown (`consumer.rs:3017-3019`).
Today the spawner only logs and counts a member's error (`consumer_group.rs:601-607`).
The group run waits for its stop signal alone (`src/backends/kafka/backend.rs:353-369`), and autoscaling replaces a dead member (`consumer_group.rs:797-822`).
`SupervisorOutcome` is a `Copy` struct of three counters (`src/consumer_supervisor.rs:17-22`), and `run_until_timeout` returns nothing else (`src/consumer_group.rs:252`, `src/broadcast.rs:309`).
No typed fault can reach the caller through it.
That is not enough for a fatal error, so part 1 adds a fatal path that part 4 shares, with a typed report:

```rust
/// The outcome of a run, plus the fatal errors that ended it.
#[non_exhaustive]
pub struct RunReport {
    pub outcome: SupervisorOutcome,
    /// Every fatal error a member or a subscription ended with, in arrival order.
    pub fatal: Vec<ShoveError>,
}

impl<B: Backend, Ctx> ConsumerGroup<B, Ctx> {
    pub async fn run_until_timeout_with_report<S>(self, signal: S, drain_timeout: Duration) -> RunReport;
}
impl<B: HasBroadcast, Ctx> BroadcastSubscriber<B, Ctx> {
    pub async fn run_until_timeout_with_report<S>(self, signal: S, drain_timeout: Duration) -> RunReport;
}
// `run_until_timeout` delegates to the sibling and returns `report.outcome`.
```

`ShoveError::is_fatal()` is true for `Commit` and for `HandlerPanicked`.
The spawner sends a fatal error to a group-level channel as well as counting it.
The group's run selects on that channel beside the stop signal, cancels the group, drains it, and collects every fatal error into `RunReport::fatal`.
The broadcast subscriber's run does the same for a fatal task error (`src/broadcast.rs:317-320`).
`ensure_min` skips a group whose fatal flag is set, so autoscaling does not replace the member.
`SupervisorOutcome` keeps its three fields and its `Copy`, `errors` grows by one per fatal member, and `exit_code()` is then 1, not 0 (`src/consumer_supervisor.rs:27-37`).
The direct `KafkaConsumer::run` already returns the fault as its `Err`.
The scope of a fatal error is the owning run, a group or a subscriber.
The process then decides from the exit code or from the report.
The `Deadline` kind says the result is unknown, because the detached thread may still land the commit.
The pending discards keep their `survived()` settlement (`consumer.rs:4545-4548`).
Acknowledged offsets on a partition a rebalance revoked are dropped with the tracker (`consumer.rs:451-463`, `:487-493`) and are not part of the final commit.
That redelivery is accepted at-least-once behaviour and is not a `Commit` error, and the docs say so.
A pre-revoke commit would remove it, and stays out of scope as in plan 021 (A6).
The broadcast loop commits nothing and is unchanged.

Compatibility.
Additive on the error enum.
A behaviour change on the exit code: a deployment that gates restarts on exit code 0 now sees 1 after a failed final commit.
Lands in 0.16.0.

**Tests shove gains.**
Unit: `final_commit_thread_tests` (`consumer.rs:9257`) gains a case where the commit result is `Err` and the loop's return carries it.
Unit: after an asynchronous drain offered position 5 and nothing new completed, the shutdown drain still offers 5.
Broker harness on librdkafka's mock cluster through `rdkafka::mocking`, which the pinned `rdkafka 0.39.0` exports:
a rejected OffsetCommit at shutdown makes `run_until_timeout` report one error and `is_clean()` false.
A held OffsetCommit past 20 s makes the member return within the deadline with the `Deadline` kind.
The frozen-broker child test changes on both sides: the child expects `Ok` today (`tests/kafka_integration.rs:4781`), and the parent requires exit success (`:4909-4926`).
Both expectations flip to the typed `Deadline` error and a non-zero exit, while the deadline and the stderr assertions stay.
`a_leaked_consumer_keeps_its_group_member_past_the_session_timeout` forces the spawn failure and expects `Ok` today (`tests/kafka_integration.rs:6306-6365`).
After part 1 it expects the `NoThread` kind.
Broker harness: with autoscaling on, a member that ends with `Commit` is not replaced, against `ensure_min_respawns_dead_consumers` as the control (`consumer_group.rs:1941`).
Broker harness: the group run ends on the fatal error without an external stop, and the outcome reports one error.

#### A2.c Topologies that need no static lifetime

**What shove does today.**
`Topic::topology()` returns `&'static QueueTopology` (`src/topic.rs:24`).
`define_topic!` implements it with a `OnceLock` (`src/macros.rs:38-42`).
The topic name is therefore part of the type, fixed at compile time.
Internal functions take `&'static QueueTopology` (`consumer.rs:1156`, `:1352`, `:1419`, `src/backends/kafka/publisher.rs:209`).
Two registries key on `&'static str` queue names (`src/broadcast.rs:161`, `src/consumer_supervisor.rs:230`).
`T::topology()` is read at 37 sites in `src/`, outside the other backends' directories.
The Kafka declarer already takes a value: `declare(&self, topology: &QueueTopology)` (`src/backends/kafka/topology.rs:251`).
`QueueTopology` derives `Clone` (`src/topology.rs:286`).
`MessageHandler<T: Topic>` and `BatchMessageHandler<T: Topic>` fix the topic type on the handler (`src/handler.rs:13`, `:99`).

**What a consumer must do today.**
A consumer that learns its topic name at run time must leak a topology to obtain a `'static` reference.
It then reads that reference back through a global.
It can then bind one topic per shape type per process, and must refuse a second.
Part B names the downstream implementation and its test.

**Proposed shove change.**
API shape, two options, both in this plan for the maintainer to choose (A4, question 5):

```rust
// Option 1, additive parallel API in 0.16.
/// Message type and codec of a topic, without its topology.
pub trait TopicShape: Send + Sync + 'static {
    type Message: Send + Sync + 'static;
    type Codec: Codec<Self::Message>;
    const SEQUENCE_KEY_FN: Option<fn(&Self::Message) -> String> = None;
}
impl<T: Topic> TopicShape for T { /* forwards */ }

// The handler traits rebind to the shape; today they bind `T: Topic` (src/handler.rs:13, :99).
pub trait MessageHandler<S: TopicShape>: Send + Sync + 'static { /* body unchanged */ }
pub trait BatchMessageHandler<S: TopicShape>: Send + Sync + 'static { /* body unchanged */ }

impl<B: Backend, Ctx> ConsumerGroup<B, Ctx> {
    pub async fn register_on<S: TopicShape, H>(
        &mut self, topology: Arc<QueueTopology>, config: ConsumerGroupConfig<B>,
        factory: impl Fn() -> H + Send + Sync + 'static) -> Result<()>;
}
impl<B: HasBroadcast, Ctx> BroadcastSubscriber<B, Ctx> {
    pub fn subscribe_on<S: TopicShape, H>(
        &mut self, topology: Arc<QueueTopology>, handler: H, options: ConsumerOptions<B>) -> Result<()>;
}
// Same for TopologyDeclarer::declare_on and Publisher::publish_on.

// Option 2, breaking, a 1.0 candidate.
pub trait Topic: TopicShape {
    fn topology() -> Arc<QueueTopology>;
}
```

Behaviour.
The internal loops take the topology as an `Arc<QueueTopology>` value, in place of the `&'static` parameters.
The static entry points wrap `T::topology()` with one clone at registration.
The handler traits rebind to `S: TopicShape`, which every `Topic` satisfies through the blanket impl, so existing handlers compile unchanged.
Without that rebind a shape-only type fails `S: Topic` on the handler bound.
The duplicate-registration sets hold `String` queue names.
The blanket implementation of `TopicShape` for every `Topic` passes coherence beside a downstream shape-only `impl TopicShape`.
A two-crate probe compiled both under `cargo check`, and part 6 keeps that probe as a compile test.
Hand-rolled `impl Topic` blocks keep compiling under option 1.

Compatibility.
Option 1 is additive and lands in 0.16.0.
Option 2 changes every `Topic` implementation and belongs to a 1.0 candidate.

**Tests shove gains.**
Unit: two topologies with different queue names register through one shape type in one process.
Unit: one handler type compiles against two runtime topology bindings of one shape.
Unit: the same queue name twice on one subscriber is refused, as today (`src/broadcast.rs:244-250`).
Broker harness: a runtime-bound `external()` topology consumes a topic that exists and is refused for one that does not.
Doc test: `define_topic!` types work unchanged through the old entry points.

#### A2.d A handler panic policy that never discards the record

**What shove does today.**
`invoke_handler` catches a panic with `catch_unwind` and returns `Outcome::Retry` (`consumer.rs:1958-2000`, the arms at `:1977-1979` and `:1990-1992`).
The option doc states it: "Handler panics are unaffected and always resolve to `Outcome::Retry`" (`src/consumer.rs:555-556`).
`decide_retry` turns a `Retry` at an exhausted budget into a DLQ decision (`src/routing.rs:121-136`).
With no DLQ, the DLQ arm settles the record as retired and commits the offset (`consumer.rs:1440-1492`, the settlement at `:1475-1491`).
A broadcast subscription pins the retry budget to zero (`src/broadcast.rs:281-287`), so the first `Retry` is a discard (`src/backend/broadcast.rs:252-274`).
An in-place redelivery ends on the same terminal decision (`consumer.rs:1735-1738`).
The batch path treats a flush panic as `Retry` too (`consumer.rs:5168-5173`).
Five backends hand-roll an `invoke_handler` of their own (`src/routing.rs:44-46`), and Redis catches the panic in `lease::catch_handler_panic` (`src/backends/redis/lease.rs:190-202`).
The NATS and Redis broadcast loops carry a wrapper of their own as well (`src/backends/nats/broadcast.rs:501-556`, `src/backends/redis/broadcast.rs:337-393`).
`handler_panic_does_not_crash_consumer` pins recovery with a budget of five retries (`tests/kafka_integration.rs:5066-5129`).
`retry_discards_instead_of_looping_the_fan_out` pins the broadcast discard (`tests/kafka_broadcast_integration.rs:1496-1512`).
With `max_retries = 0` and no DLQ, a panic is therefore a discard and a committed offset.

**What a consumer must do today.**
A consumer that must not lose a record on a panic wraps its own handler in `catch_unwind`.
It records the panic as a fault, cancels its own stop token, and returns `Defer` to shove.
The in-place wait then ends on the cancellation with the record unsettled.
Part B names the downstream implementation and its tests.

**Proposed shove change.**
API shape, backend-neutral, beside `with_handler_timeout_outcome`:

```rust
/// What a panic inside `MessageHandler::handle` resolves to.
#[non_exhaustive]
pub enum HandlerPanic {
    /// `Outcome::Retry`, the behaviour of 0.15.
    Retry,
    /// End the consumer with `ShoveError::HandlerPanicked`.
    /// On a competing consumer the record stays unsettled, and the next owner redelivers it.
    /// On an ephemeral broadcast subscription the record goes with the subscription, as every broadcast record does.
    Fail,
}

impl<B: Backend> ConsumerOptions<B> {
    pub fn with_handler_panic(self, policy: HandlerPanic) -> Self;
}
// The same setter on every <Backend>ConsumerGroupConfig and on BatchConsumerOptions<B>.
```

Behaviour under `Fail`.
`invoke_handler` returns a `Failed { message }` verdict instead of an `Outcome`.
The receive loop ends with `ShoveError::HandlerPanicked { topic, partition, offset, message }`, non-retryable, so no reconnect.
Nothing is completed, the offset stays below the record, and no discard is counted.
That holds for a competing consumer, a group member or a supervisor consumer, whose broker keeps the record.
An ephemeral broadcast subscription has nothing that can redeliver: NATS reads with `AckPolicy::None` (`src/backends/nats/broadcast.rs:297-305`), and RabbitMQ's queue dies with the connection (`src/backends/rabbitmq/consumer.rs:157-195`).
The in-process buffer dies with the loop (`src/backends/inmemory/consumer.rs:468-510`), and a Kafka subscription commits nothing.
`Fail` there ends the subscription and counts the record as lost with it, through the terminal path a broadcast drop takes today (`src/backend/broadcast.rs:264-272`).
The promise of `Fail` is therefore no discard by shove's own hand, not a redelivery on every path, and the docs say so per path.
`HandlerPanicked` is fatal, so the fatal path of part 1 stops the owning run, blocks a respawn, and returns the error in `RunReport::fatal` (A2.b).
Whether the tally counts it under `errors` or under `panics` is an open question (A4, question 3).
The panic boundary sits in 17 places, found with a sweep for `catch_unwind`, `JoinError` and the panic log lines under `src/backends`.
Ten are wrapper functions.
Kafka catches the panic with `catch_unwind` in `invoke_handler` (`consumer.rs:1958-2000`).
SNS, RabbitMQ and NATS each spawn a task in their `invoke_handler` and read its `JoinError` (`src/backends/sns/consumer.rs:235`, `src/backends/rabbitmq/consumer.rs:1698`, `src/backends/nats/consumer.rs:556`).
The in-process broker has two such wrappers (`src/backends/inmemory/consumer.rs:2406`, `:2448`).
NATS and Redis each have a broadcast wrapper of their own (`src/backends/nats/broadcast.rs:501-556`, `src/backends/redis/broadcast.rs:337-393`).
Redis catches the competing-consumer panic in `lease::catch_handler_panic` (`src/backends/redis/lease.rs:190-202`).
Every batch flush shares `invoke_batch_handler` (`src/backend/batch_consumer.rs:452-500`).
Seven more sit inline, where a loop reads a dead handler task or a closed outcome channel as `Retry`.
Those are `src/backends/sns/consumer.rs:446` and `:987`, `src/backends/rabbitmq/consumer.rs:516` and `:1256-1260`, `src/backends/nats/consumer.rs:1425-1428`, and `src/backends/inmemory/consumer.rs:368-372` and `:407-411`.
The shared verdict type lives in `src/routing.rs`, next to `handler_timeout_outcome`, and all 17 boundaries return it, so the backends cannot drift.
The scope of `Fail` is the owning run, as in KIP-671's `SHUTDOWN_CLIENT` (A6).

Compatibility.
Additive with the default at `Retry`.
A default flip to `Fail` is a 1.0 candidate.
Lands in 0.16.0.

**Tests shove gains.**
Every settlement route gets a deterministic `Fail` test, one per boundary and per call site of a wrapper.
Kafka: the concurrent loop (`consumer.rs:4949`), the in-place redelivery (`:1811`) and the FIFO shard (`:6160`).
Kafka as well: the broadcast loop (`consumer.rs:6872`) and the batch flush (`:3582`).
NATS: the two handler calls (`src/backends/nats/consumer.rs:968`, `:1416`), the FIFO oneshot at `:1425-1428`, the broadcast wrapper (`src/backends/nats/broadcast.rs:501-556`) and the batch flush (`src/backends/nats/consumer.rs:2183`).
RabbitMQ: the two handler calls (`src/backends/rabbitmq/consumer.rs:1763`, `:1802`), the two inline sites (`:516`, `:1256-1260`) and the batch flush (`:2391`).
SNS: the two handler calls (`src/backends/sns/consumer.rs:300`, `:837`), the two inline sites (`:446`, `:987`) and the batch flush (`:2056`).
Redis: the two lease boundaries (`src/backends/redis/consumer.rs:1009`, `:1478`), the broadcast wrapper (`src/backends/redis/broadcast.rs:337-393`) and the batch flush (`src/backends/redis/consumer.rs:2726`).
The in-process broker: the two handler calls (`src/backends/inmemory/consumer.rs:319`, `:755`), the two inline sites (`:368-372`, `:407-411`) and the batch flush (`:1988`).
Each test asserts that the consumer ends with `HandlerPanicked`, that nothing is settled, and that `messages_discarded_total` stays at zero.
Broker harness, Kafka group: the member ends with `HandlerPanicked` naming partition and offset, nothing is committed, and a restart redelivers the record (mirrors `faults.rs:68-106`).
Broker harness, broadcast: the subscription ends, `RunReport::fatal` carries `HandlerPanicked`, and the record is counted as lost, not redelivered.
Broker harness, in-place: a panic on a redelivery ends the consumer with the record unsettled.
Broker harness, autoscaling: a `Fail` member is not replaced, and the group run ends with the fault.
Unit, batch: a flush panic under `Fail` rewinds nothing and ends the consumer.
Cross-backend: one table test per backend that the `Retry` default is byte for byte the 0.15 behaviour.

#### A2.e A true commit on acknowledgement for the per-record mode

**What shove does today.**
Completions are committed at most once per `ASYNC_COMMIT_INTERVAL`, 500 ms (`consumer.rs:105-126`).
`AsyncCommitGate::due` is true when the last commit is at least one interval old (`consumer.rs:149-154`).
The drain commits `CommitMode::Async`, and `Sync` only when the batch retires discards (`consumer.rs:4437-4468`).
That `Sync` commit waits without a bound: `rd_kafka_commit` blocks on `RD_POLL_INFINITE` (`librdkafka/src/rdkafka_offset.c:387-406` in `rdkafka-sys 4.10.0+2.12.1`).
While it waits, the loop neither polls nor sees the shutdown token.
A wake arm fires when the gate's deadline passes with commit work pending (`consumer.rs:4569-4588`).
`with_commit_interval` refuses zero and anything over one hour (`consumer_group.rs:141-148`, `constants.rs:187`).
The Kafka page says commits are asynchronous, at most once per interval (`kafka.mdx:430-431`).
The FIFO consumer already commits each message as it settles, `Async` by default and `Sync` with a discard (`consumer.rs:1307-1340`).
Every FIFO entry point refuses the interval, because it would change nothing there (`consumer.rs:2749-2761`).

**What a consumer must do today.**
A consumer that wants a commit per record can only ask for a one-millisecond interval and document it as best effort.
Part B names the downstream implementation and its test.

**Proposed shove change.**
API shape:

```rust
/// How the concurrent Kafka consumer commits completed offsets.
#[non_exhaustive]
pub enum CommitPolicy {
    /// At most once per interval, asynchronously. The 0.15 behaviour.
    Interval(Duration),
    /// On every completion, before the next record is handed out.
    PerRecord,
}

impl KafkaConsumerGroupConfig {
    pub fn with_commit_policy(self, policy: CommitPolicy) -> Self;
}
impl ConsumerOptions<Kafka> {
    pub fn with_commit_policy(self, policy: CommitPolicy) -> Self;
}
// `with_commit_interval(d)` stays as a shorthand for `CommitPolicy::Interval(d)`.
```

Behaviour under `PerRecord`.
The drain runs on every completion, with no gate.
The commit carries the partition's safe position, the lowest unfinished offset (`consumer.rs:311-315`), never the completed record's own offset.
`a_delivered_but_unfinished_offset_still_blocks` pins that rule today (`consumer.rs:7360`).
With more than one permit, a completion above an unfinished lower offset confirms nothing yet, and a commit per record is not exact.
`PerRecord` therefore refuses `prefetch_count` above one with `concurrent_processing` on, at the same fail-fast point as the other refusals.
The commit is issued `Async`, never `Sync` inline: a synchronous commit blocks the runtime thread for as long as librdkafka waits.
`commit_callback` today reports only a failure (`consumer.rs:2171-2183`), and part 3 makes it report a success with its offsets too.
The loop then waits for that confirmation before it hands out the next record.
The wait is a select that keeps calling `recv()` and honours the shutdown token.
The wait is bounded: past `socket.timeout.ms`, 60 s by default in librdkafka, the commit counts as rejected, is re-offered, and the fence logic applies.
With the prefetch at one the wait costs nothing extra, because the loop has no other record to hand out.
The existing `Sync` commit on the discard path (`consumer.rs:4450`) moves onto the same confirmation wait.
Whether the loop waits for the confirmation at all is the open question (A4, question 4).
The fence threshold uses the 60 s floor of `COMMIT_FENCE_TIMEOUT`, because there is no interval to scale (`consumer.rs:2647`, `:2686-2688`).
The FIFO, broadcast and DLQ entry points refuse `PerRecord` as they refuse the interval, because they commit per message or never.

Compatibility.
Additive.
Lands in 0.16.0.

**Tests shove gains.**
Unit: the gate tests (`consumer.rs:8220`) gain a `PerRecord` case that is always due.
Unit: `PerRecord` with a prefetch of two and concurrent processing is refused at configuration time.
Broker harness on the mock cluster: the OffsetCommit request count equals the completion count for `PerRecord`, and stays at two for `Interval` over six records.
Broker harness: a member killed right after a confirmed completion under `PerRecord` replays nothing on restart.
Broker harness: a rejected commit under `PerRecord` is re-offered and the fence still fires.
Broker harness: a confirmation that never arrives ends the wait at the bound, and the loop keeps polling meanwhile.

#### A2.f In-place redelivery order on stop and during a cooperative rebalance

**What shove does today.**
A `Retry` or `Defer` on an in-place topic waits inside the handler's task, holding its permit (`consumer.rs:1694-1708`).
The wait selects on the shutdown token and returns `Cancelled` with nothing completed (`consumer.rs:1747-1758`).
The task then drops its permit and returns (`consumer.rs:5032-5040`).
The receive loop tracks a record before it waits for a permit (`consumer.rs:4684`, `:4883-4909`).
`acquire_permit_while_polling` is a biased select that reads the permit before the shutdown token (`consumer.rs:4041-4054`).
So a record held there can take the permit a cancelled wait frees, and reach the handler during the drain.
The loop's own select is unbiased, and a freed permit can resume the paused assignment (`consumer.rs:4503`, `:4601-4611`).
The broadcast loop waits for its permit outside any shutdown select (`consumer.rs:6809-6813`).
A shutdown cancels a deferred redelivery, which drops the permit (`consumer.rs:6885-6898`, `:6952-6956`).
The waiting record then reaches the handler before the drain arm runs (`consumer.rs:6577-6587`).
On a rebalance the tracker drops the partition's state on revoke and on assign (`consumer.rs:487-520`, `:453-463`).
Nothing tells an in-place task that its partition was revoked.
The permit-wait helper has no rebalance receiver (`consumer.rs:4030-4087`), and the loop applies rebalance events only at the top of its own pass (`consumer.rs:4400-4405`).
A revoke that arrives during a permit wait is therefore not seen while the sole permit is held by an in-place wait.
After a revoke and a reassign, librdkafka refetches from the committed offset, so the same record arrives again while the old task still waits.
`Completion` carries a partition, an offset and a discard, and no assignment epoch (`consumer.rs:835-842`).
The maintainer's round-2 review of pull request 211 named that gap: an old handler's completion can remove the new delivery of the same offset.
The pause covers the assignment as it stands at pause time (`consumer.rs:2268-2284`).
The broadcast test `defer_redelivers_in_place_before_later_records` pins the order `[1, 1, 2]` while the loop runs (`tests/kafka_broadcast_integration.rs:1388-1395`).
No shove test covers the order on stop or across a rebalance.
Which path lets a later record through during a live cooperative rebalance on a worker is UNVERIFIED in shove code.
The downstream consumer observed it about once in ten runs of its own rebalance test (Part B).

**What a consumer must do today.**
A consumer that needs partition order must track the pending offsets per partition itself and defer unseen any record delivered above a pending one.
On a worker it must read the group's committed offset to tell its own pending record from another member's completion.
Part B names the downstream implementation and its tests.

**Proposed shove change.**
No new API.
One invariant, stated in the Kafka page and the `RetryStrategy` rustdoc:
While a record of partition P waits in place, no later record of P reaches the handler.
That holds on stop, on a cancelled wait, and across a rebalance.
The invariant is for a consumer with one permit: `prefetch_count` at one, or `concurrent_processing` off.
With more permits, today's loop pauses only when every permit is held and one holder waits (`consumer.rs:4487-4496`).
Later records of the same partition then run beside a wait, and part 2 keeps that concurrent behaviour and documents it.

Behaviour.
On shutdown, the loop takes no new record: the permit-wait helper reads the shutdown token before the permit, and drops a record in hand.
The loop's own select becomes `biased`.
The order is the shutdown arm, the fault arm, the housekeeping and commit wakes, the completions, the resume arm, and then `recv()`.
A cancelled wait that frees a permit can therefore not win against the shutdown arm in the same pass.
The dropped record is tracked and never completed, so the position stays below it and a restart redelivers it.
The broadcast loop selects on the shutdown token around its permit wait (`consumer.rs:6809-6813`) and drops the record in hand too.
On a revoke, every in-place wait on the revoked partition ends with a new `InPlaceEnd::Revoked`, nothing completed.
The revoke signal is one `CancellationToken` per assigned partition, created on the assign event and cancelled on the revoke event in `apply_rebalance_events`.
A task clones the token of its record's partition when its wait starts, and drops the clone when the wait ends.
The loop drops its own handle once the partition is revoked and every task of that epoch has ended.
A reassign therefore gets a fresh token.
Every polling wait drains the rebalance events too: the permit wait, the registry stall, and the confirmation wait of part 3.
A revoke therefore reaches the in-place task during any wait, not only at the top of the loop.
The new owner, or this member after a reassign, then receives the record once from the committed offset.
The loop keeps an `in_place_pending` map of partition to lowest waiting offset.
Before it spawns a handler for `(P, o)`, it checks that no lower offset of P is waiting.
If one is, the loop pauses partition P alone, with `pause` on a one-partition list, and puts the record back once.
The pause ends when P's wait ends, so the other partitions keep flowing and no record is put back twice.
Today's `pause_assignment` pauses every held partition (`consumer.rs:2268-2293`), which part 2 keeps for the all-permits-held case and narrows here to one partition.
`Completion` gains the assignment epoch the maintainer asked for, so a stale completion cannot remove a new delivery.

Compatibility.
A behaviour fix with no API change.
A record already fetched at shutdown is no longer handled during the drain, and is redelivered after the restart.
Lands in 0.16.0.

**Tests shove gains.**
Unit: the permit-wait helper under a cancelled token returns `None` although a permit is free.
Unit: the token is cancelled and a permit is freed in the same tick.
The loop takes the shutdown arm, in the paused state and in the unpaused state.
Unit: the `in_place_pending` map refuses `(P, 3)` while `(P, 2)` waits, and admits `(Q, 0)`.
Unit: an epoch-one completion of `(P, 7)` cannot remove the epoch-two delivery of `(P, 7)` from the in-flight set.
Broker harness, worker: defer offset 2 forever, fetch offset 3, and stop the member.
The handler never saw 3, and the restart delivers `[2, 3]`.
Broker harness, broadcast: a first call past the handler timeout, a stop during the in-place wait, and the handler saw one call (mirrors `faults.rs:177-202`).
Broker harness, cooperative rebalance: a second member joins and leaves while a head record is deferred.
The handler order on that partition stays ascending (mirrors `worker_offsets.rs:476-591`).
Broker harness, forced revoke: a LeaveGroup request on the member's behalf ends its in-place wait.
The new owner completes the record.
The returning partition then delivers the next record once (mirrors `worker_offsets.rs:604-690`).
Broker harness: a revoke arrives while the sole permit is held by an in-place wait.
The wait ends within the rebalance timeout and frees the permit.
The `put_back_probe` counters under `test-support` (`consumer.rs:959-985`) already expose the put-back paths for these assertions.

### A3. The PR split and the release plan

Six parts, in the plan 021 style: one pull request per risk class, stacked only where a part reads another.
The plan file `plans/022-kafka-transport-guarantees.md` rides in part 1, as plan 021 rode in pull request 210.

| Part | Title, in the repository's commit style | Scope | Tests | Depends on |
|---|---|---|---|---|
| 1 | `fix(kafka): return the final commit failure of a stopping consumer instead of a clean exit` | A2.b: `ShoveError::Commit`, the shutdown re-offer of every position, the fatal path with `RunReport`, the respawn block, docs | mock-cluster reject and deadline tests, the re-offer unit test, the frozen-broker and spawn-failure assertions, the no-respawn test | none |
| 2 | `fix(kafka): keep the record behind an in-place wait unread on stop and end the wait on revoke` | A2.f: the biased select order, rebalance events in every polling wait, the broadcast permit select, `InPlaceEnd::Revoked`, `in_place_pending`, the completion epoch | the broker tests and unit tests of A2.f | none |
| 3 | `feat(kafka): commit on every completion with CommitPolicy::PerRecord` | A2.e: `CommitPolicy`, both setters, the drain, the refusals | gate unit test, mock-cluster request count, kill-and-restart test | none |
| 4 | `feat(consumer): a handler panic policy that ends the consumer with the record unsettled` | A2.d: `HandlerPanic`, the shared verdict, 17 panic boundaries across six backends | in-process unit tests, Kafka group, broadcast, in-place, batch and autoscaling tests | part 1, for the fatal path |
| 5 | `feat(broadcast): resume a reconnected subscription from each partition's last position` | A2.a: `BroadcastReconnect`, the Kafka position map, the Redis mapping, the refusals on NATS, RabbitMQ and the in-process broker, docs | position-map unit tests, pause-and-unpause broker tests at three starts | part 2, which fixes the drain the resume relies on |
| 6 | `feat(topic): bind a topic at run time from a topology value` | A2.c option 1: `TopicShape`, the `Arc<QueueTopology>` internals, the `_on` entry points | registration unit tests, runtime `external()` broker test | none |

Each part carries its docs sentences and its `plans/README.md` row is the maintainer's to add.
Parts 1 and 2 are fixes and go first.
Parts 3, 4 and 5 are additive features with unchanged defaults.
Part 6 is the largest and can trail the release.

Release plan.
One minor release, 0.16.0, after part 5, with part 6 in it when it is ready.
The reason: no part changes a default.
shove also ships breaking changes in minors with a `!` marker, as 0.15.0 did with pull requests 210, 212 and 213.
A 1.0 candidate is warranted only if the maintainer flips a default: `Resume`, `Fail`, or the `Topic` trait of option 2.
Those flips are the open questions below, not decisions of this plan.

### A4. Open questions for Zannis

1. Broadcast reconnect.
   Option A: `BroadcastReconnect::Resume` as an opt-in with `ConfiguredStart` the default.
   Option B: `Resume` as the new default, with `ConfiguredStart` the opt-out.
   Option C: a Kafka-only `BroadcastStart::Offsets(BTreeMap<i32, i64>)` that the caller feeds.
   Trade-off: A keeps the Kafka 0.15 behaviour byte for byte, and B fixes the silent gap for every user.
   C keeps the tracking in the caller.
   Also: does NATS join in this plan with a start-sequence policy, or later?
   Also: keep one librdkafka consumer through transport errors, and end the subscription on permanent errors as today?

2. Final commit failure.
   Option A: the additive `RunReport` sibling, with `SupervisorOutcome` unchanged and the fatal errors beside it.
   Option B: a `fatal` field on `SupervisorOutcome`, which breaks struct-literal constructors and its `Copy`.
   Option C: `#[non_exhaustive]` on `SupervisorOutcome` plus a builder, then the field.
   Trade-off: A doubles the run entry points, B and C give one type but break every existing constructor.
   Also: should the `Deadline` kind read as unknown rather than failed in the exit code?
   Also: count acknowledged offsets dropped on a revoke in the typed report, or keep them as accepted redelivery?

3. Handler panic policy.
   Option A: `HandlerPanic { Retry, Fail }`.
   Option B: `with_handler_panic_outcome(Outcome)`, mirroring the timeout knob, plus a `Fail` variant.
   Trade-off: A is two clear states, B is symmetric with the timeout knob but lets a `Defer` loop forever.
   Also: tally a `Fail` under `panics` or under `errors`, and land all six backends in one part or Kafka first?

4. Per-record commit mode.
   Option A: issue the commit and wait for its callback before the next record, bounded and still polling.
   Option B: issue the commit on every completion and continue at once.
   Trade-off: A gives the confirmation a non-idempotent handler wants and costs one round trip per record, B keeps throughput and keeps a replay window.
   Also: `CommitPolicy` enum, or a second setter `with_commit_per_record()` beside `with_commit_interval`?
   Also: refuse more than one permit under `PerRecord`, or commit the safe position on every completion?

5. Runtime topology.
   Option 1: the additive `TopicShape` and `_on` entry points in 0.16.
   Option 2: `Topic::topology()` returns `Arc<QueueTopology>` in a 1.0 candidate.
   Trade-off: option 1 doubles the entry points and keeps every user compiling, option 2 is one API and breaks every topic.
   Also: where does the duplicate-queue refusal live when the name is a `String`?

6. In-place order across a rebalance.
   Option A: a revoke ends the in-place wait, and the new owner redelivers.
   Option B: the member finishes its wait and lets the coordinator reject the stale commit.
   Trade-off: A frees the permit and the partition at once, B keeps the handler's attempt but risks two members on one record.
   Also: does the `Completion` epoch you noted on pull request 211 belong to part 2 or to its own part?

7. Release.
   Option A: 0.16.0 for parts 1 to 6.
   Option B: 0.16.0 for parts 1 to 5 and part 6 with the 1.0 candidate.

### A5. Risks and the compatibility story

Existing shove consumers see no change unless they opt in, with two exceptions.
A failed final commit now ends the member with an error, so `exit_code()` becomes 1 where it was 0 (part 1).
A record fetched during the drain is redelivered after the restart instead of handled during the drain (part 2).
That fix applies to single-permit consumers, and multi-permit consumers keep today's concurrent behaviour (part 2).
Both are documented in the release note as at-least-once semantics made visible.

Risk table:

| Part | Risk | Why | Mitigation |
|---|---|---|---|
| 1 | LOW | one return path, one error variant | the mock-cluster tests and the frozen-broker test |
| 2 | HIGH | the receive loop's select order and a new revoke signal to tasks | the four broker tests, the `put_back_probe` counters, a soak of the rebalance test |
| 3 | MED | a confirmation wait inside the receive loop | the bounded wait, prefetch guidance in the docs, the request-count test |
| 4 | MED | 17 panic boundaries and three settling paths must agree | one shared verdict type in `src/routing.rs`, one test per boundary and route |
| 5 | MED | positions across reconnects and new partitions | the position-map unit tests and the pause-and-unpause tests at three starts |
| 6 | MED | the `'static` to `Arc` refactor touches 37 read sites | the blanket-impl coherence check first, the old entry points as doc tests |

Cross-backend rule.
Part 4 changes a delivery-semantics decision, so it must land on every backend and on the two settling paths that bypass `route_outcome`.
Those are broadcast settling (`src/backend/broadcast.rs:247-281`) and batch settling (`src/backend/batch_consumer.rs`).
The per-feature clippy legs catch an item that is dead under one backend set.

Documentation debt.
`kafka.mdx:430-431`, `:445-469`, `broadcast.mdx:61`, `:145-146`, and `src/consumer.rs:555-556` and `:917-918` each state a behaviour this plan changes.
Each part quotes the sentence it makes true or false, as pull request 198 and plan 021 did.

### A6. Known patterns and alternatives

librdkafka, version 2.12.1 as pinned by `rdkafka-sys 4.10.0+2.12.1` (`Cargo.lock:3626-3627`).
Its consumer keeps an in-memory offset store, and `enable.auto.offset.store` fills it with the last message passed to the application.
For at-least-once, the application disables that store and calls `rd_kafka_offsets_store()` after processing.
Only greater offsets are committed, so a store of 9 after a commit of 10 is ignored.
`auto.commit.interval.ms` defaults to 5000 and `max.poll.interval.ms` to 300000.
shove's `PartitionTracker` is a user-land offset store with the same rule: the position never lowers (`consumer.rs:302-320`).
Part 3 follows the store-then-commit pattern and adds a commit on every store.
Part 5 follows the manual `assign()` pattern, where the application owns the position of a groupless consumer.

rust-rdkafka 0.39.0 (`Cargo.lock:3607-3608`).
`CommitMode::Sync` blocks until the broker finishes the commit, and `Async` enqueues the request and returns.
`commit_message` commits every lower offset of the partition too.
`StreamConsumer` must be polled at least every `max.poll.interval.ms` or librdkafka leaves the group.
That rule is why shove pauses instead of parking its loop (`consumer.rs:2268-2284`), and part 2 keeps it.
Part 3 keeps the asynchronous request and adds a confirmation wait through the callback, because a synchronous commit blocks the runtime thread without a bound.

Apache Kafka, KIP-429, accepted in 2.4.0.
The cooperative protocol revokes only the partitions that change owner, and a member keeps consuming the partitions it keeps.
The Kafka 3.9 javadoc says a consumer commits the offsets of the partitions taken away in `ConsumerRebalanceListener.onPartitionsRevoked`.
KIP-429 itself states no commit rule for that callback.
shove commits nothing before a revoke, and relies on the new owner's redelivery instead (`consumer.rs:453-463`).
Plan 021 left a pre-revoke commit out of scope, and this plan keeps it out.
Part 2 departs from the KIP's model in one way: it ends the in-place wait on revoke instead of finishing it.

Apache Kafka 3.9 `KafkaConsumer` javadoc.
The committed offset is the offset of the next record to read, so the application adds one.
`pause()` suspends fetching and the consumer keeps its heartbeat, and `resume()` undoes it.
Part 2 keeps shove's pause-and-poll shape on that primitive.

Spring Kafka 4.1.1.
`AckMode::RECORD` commits when the listener returns, and `MANUAL_IMMEDIATE` commits when the listener acknowledges.
`syncCommits` is true by default.
`DefaultErrorHandler` seeks the failed record back and redelivers it, then recovers it after the back-off, by default to a log.
`CommonContainerStoppingErrorHandler` stops the container when the listener throws, and the failed record is replayed on restart.
Part 3 with the confirmation wait is `RECORD` with `syncCommits`.
Part 4's `Fail` is the container-stopping handler, and shove's `Retry` default is the `DefaultErrorHandler`.

franz-go v1.22.1.
The default balancer is cooperative-sticky.
The default group autocommits every 5 s, commits in `OnPartitionsRevoked`, and issues a blocking commit when it leaves the group.
`BlockRebalanceOnPoll` with `AllowRebalance` holds a rebalance while records are in flight.
`CommitRecords` and `CommitUncommittedOffsets` commit explicitly.
Part 1 follows the blocking commit on leave and adds the missing error report.
Part 2 takes the other road from `BlockRebalanceOnPoll`: it lets the rebalance run and ends the wait.

Sarama v1.61.1, tag `c0f1530`.
A revoked `ConsumeClaim` must return within `Config.Consumer.Group.Rebalance.Timeout`, or Kafka may remove the member and later commits may fail (`consumer_group.go:69-71`).
Under a cooperative strategy a revoked claim has its `Messages` channel closed, and retained claims keep running (`consumer_group.go:62-67`).
The handler must finish processing and mark offsets within that timeout after a claim is revoked (`consumer_group_session.go:525`, `:549-550`).
`MarkMessage` marks the next offset and `Commit` writes the marks (`consumer_group_session.go:52`, `:42`, `:247-248`).
Part 2 follows Sarama's revoke rule: the in-place wait ends on revoke, and the record is left to the new owner.
Part 3 follows the mark-then-commit split, where the mark is shove's completion.

kafka-go v0.4.51.
`FetchMessage` and `CommitMessages` split the read from the commit.
`CommitInterval` at zero means synchronous commits, and a non-zero value flushes on that interval.
Part 3 mirrors that pair: `Interval(d)` and `PerRecord`.

Kafka Streams, KIP-671, accepted.
`StreamsUncaughtExceptionHandler` answers an uncaught exception with `REPLACE_THREAD`, `SHUTDOWN_CLIENT`, the default, or `SHUTDOWN_APPLICATION`.
`REPLACE_THREAD` starts a new thread, `SHUTDOWN_CLIENT` stops every thread of that client, and `SHUTDOWN_APPLICATION` spreads the shutdown through the rebalance protocol.
shove's `Retry` default is `REPLACE_THREAD` without a new thread, because the member lives on.
Part 4's `Fail` is `SHUTDOWN_CLIENT`: the owning run stops, no member is replaced, and the process decides from the exit code.
`SHUTDOWN_APPLICATION` has no shove analogue, and this plan adds none.

Side-effect boundaries, three patterns.
An idempotent handler absorbs a replay, which is the at-least-once contract shove documents (`kafka.mdx:461-469`).
The Kafka 3.9 javadoc section "Storing Offsets Outside Kafka" stores the offset beside the results in one database transaction.
It needs `enable.auto.commit=false` and a `seek()` on restart, and gives exactly-once against that store.
KIP-98 transactions make Kafka writes and the consumer offsets one atomic unit through `sendOffsetsToTransaction`.
They serve consume-transform-produce into Kafka, and cover no side effect outside Kafka.
Part 3's `PerRecord` sits between the three: a confirmed completion never replays.
Only the record in flight at a crash, or one completed but not yet confirmed, can replay, and `PerRecord` removes no duplicate of that record.
A handler with a non-idempotent side effect outside Kafka needs the external-store pattern.
shove can support it later with the record coordinates already on `MessageMetadata` and a `seek` at start.

Alternatives considered and set aside.
A per-pod consumer group for fan-out gives offsets for free but leaves broker state on every restart (`broadcast.mdx:59`).
Committing inside `pre_rebalance` before a revoke would shrink duplicates but plan 007 deferred it and plan 021 kept it out.
A timestamp-based resume in shove, as the downstream consumer does now, needs `LogAppendTime` and one second of slack, and the maintainer rejected it.

### A7. Sources

Repository paths, shove at `v0.15.0`:

- `src/backends/kafka/consumer.rs`, `src/backends/kafka/consumer_group.rs`, `src/backends/kafka/constants.rs`, `src/backends/kafka/topology.rs`, `src/backends/kafka/publisher.rs`, `src/backends/kafka/offset_reset.rs`
- `src/broadcast.rs`, `src/backend/broadcast.rs`, `src/consumer.rs`, `src/consumer_group.rs`, `src/consumer_supervisor.rs`, `src/routing.rs`, `src/topic.rs`, `src/macros.rs`, `src/topology.rs`, `src/error.rs`, `src/backend/topology.rs`
- `docs/pages/backends/kafka.mdx`, `docs/pages/concepts/broadcast.mdx`, `docs/pages/concepts/handlers.mdx`
- `tests/kafka_integration.rs`, `tests/kafka_broadcast_integration.rs`, `tests/kafka_rebalance.rs`
- `plans/021-kafka-external-topics-and-broadcast-start.md`, `plans/README.md`, `Cargo.toml`, `Cargo.lock`

Pull requests and reviews:

- https://github.com/zannis/shove/pull/210, https://github.com/zannis/shove/pull/211, https://github.com/zannis/shove/pull/212, https://github.com/zannis/shove/pull/213, https://github.com/zannis/shove/pull/214
- https://github.com/zannis/shove/pull/211#pullrequestreview-5304157855

Library documentation:

- https://github.com/confluentinc/librdkafka/blob/v2.12.1/CONFIGURATION.md
- https://github.com/confluentinc/librdkafka/blob/v2.12.1/INTRODUCTION.md
- https://docs.rs/rdkafka/0.39.0/rdkafka/consumer/trait.Consumer.html
- https://docs.rs/rdkafka/0.39.0/rdkafka/consumer/enum.CommitMode.html
- https://docs.rs/rdkafka/0.39.0/rdkafka/consumer/stream_consumer/struct.StreamConsumer.html
- https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
- https://cwiki.apache.org/confluence/display/KAFKA/KIP-671%3A+Introduce+Kafka+Streams+Specific+Uncaught+Exception+Handler
- https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
- https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
- https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html
- https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html
- https://github.com/twmb/franz-go/blob/master/docs/producing-and-consuming.md
- https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo
- https://pkg.go.dev/github.com/segmentio/kafka-go
- https://github.com/IBM/sarama/blob/v1.61.1/consumer_group.go
- https://github.com/IBM/sarama/blob/v1.61.1/consumer_group_session.go

use std::time::Duration;

use crate::batch::BatchFailure;

/// Errors that can occur during pub/sub operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ShoveError {
    /// Failed to serialize or deserialize a message.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Connection-level failure (channel closed, timeout, network error).
    #[error("connection error: {0}")]
    Connection(String),

    /// Topology declaration or validation failed.
    #[error("topology error: {0}")]
    Topology(String),

    /// Input validation failed (e.g. message too large, reserved header).
    #[error("validation error: {0}")]
    Validation(String),

    /// A `Codec` failed to encode or decode a payload.
    ///
    /// `codec` is the codec's stable `NAME`. `source` is the codec-specific
    /// error (e.g. `prost::EncodeError`, `prost::DecodeError`).
    #[error("codec error in {codec}: {source}")]
    Codec {
        codec: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// An error from an external SDK or backend that doesn't map to a known category.
    /// Treated as non-retryable so it surfaces immediately to the operator.
    #[error("unknown backend error: {0}")]
    Unknown(String),

    /// A [`Publisher::publish_batch`] call partially succeeded: the backend
    /// confirmed at least one record and did not confirm at least one other.
    ///
    /// The payload names the indices that still need re-publishing, so a
    /// caller can retry only those instead of re-producing the whole batch.
    /// A batch that fails as a *whole* does not use this variant — it returns
    /// the same bare error it always has.
    ///
    /// Boxed to keep `ShoveError` small: it is returned by value from every
    /// fallible call in the crate.
    ///
    /// [`Publisher::publish_batch`]: crate::publisher::Publisher::publish_batch
    #[error("batch publish: {0}")]
    PartialBatch(Box<BatchFailure>),

    /// The final offset commit of a stopping consumer did not land.
    ///
    /// The Kafka receive loop returns this from its shutdown arm when the
    /// synchronous commit it issues after the handler drain is rejected,
    /// misses the shutdown deadline, or has no thread to run on. The member
    /// ends with this error instead of a clean exit: a group run counts it
    /// under [`SupervisorOutcome::errors`](crate::SupervisorOutcome::errors),
    /// ends on it without an external stop, and returns it in
    /// [`RunReport::fatal`](crate::RunReport::fatal). It is fatal
    /// ([`is_fatal`](Self::is_fatal)) and not retryable, so a consumer that
    /// reconnects on transient errors returns it instead.
    ///
    /// The records behind the uncommitted positions are redelivered to the
    /// next member of the group, which is at-least-once delivery made
    /// visible. The error is not raised for a position the member never
    /// tried to commit: an acknowledged offset on a partition a rebalance
    /// revoked is dropped with the partition and redelivered by its new
    /// owner, silently, as before.
    #[error(
        "final offset commit on '{topic}' did not land for {}: {kind}",
        format_offsets(offsets)
    )]
    Commit {
        /// The topic the member consumed.
        topic: String,
        /// The offsets the consumer tried to commit, per partition: one
        /// `(partition, offset)` pair per partition the member held, the
        /// offset exclusive as Kafka commits it. Never empty: a member with
        /// nothing to commit has no commit to fail.
        offsets: Vec<(i32, i64)>,
        /// Rejected by the coordinator, or unknown after the deadline.
        kind: CommitFailure,
    },
}

/// Why the final offset commit of a stopping consumer did not land; the
/// `kind` of [`ShoveError::Commit`].
///
/// `#[non_exhaustive]`: match with a wildcard arm.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CommitFailure {
    /// The coordinator answered the commit with an error, carried as text.
    /// Nothing landed. Also used, with a message that says so, when the
    /// commit thread ended without reporting a result.
    ///
    /// The text is librdkafka's rendering of the broker's answer: the error
    /// code and its description, and nothing else. It never carries a
    /// record's payload, key or headers, nor a personal or account
    /// identifier taken from one, so it is safe to log and to return as is.
    #[error("rejected: {0}")]
    Rejected(String),
    /// The commit had no answer within the shutdown deadline it carries.
    /// The result is unknown: the detached commit thread may still land it
    /// after the consumer has returned.
    #[error("no answer within the {0:?} shutdown deadline; the result is unknown")]
    Deadline(Duration),
    /// No thread could be spawned to run the commit, so nothing was
    /// committed and the consumer's close moved off the runtime by itself.
    #[error("no thread could be spawned for the commit; nothing was committed")]
    NoThread,
}

/// `[p0@o0, p1@o1]`, the `offsets` of [`ShoveError::Commit`] in its message.
fn format_offsets(offsets: &[(i32, i64)]) -> String {
    let pairs: Vec<String> = offsets
        .iter()
        .map(|(partition, offset)| format!("{partition}@{offset}"))
        .collect();
    format!("[{}]", pairs.join(", "))
}

impl ShoveError {
    /// Returns `true` for transient errors that may succeed on retry (connection
    /// failures). Non-transient errors (topology, serialization) are returned
    /// immediately so callers don't waste time retrying.
    ///
    /// A [`PartialBatch`](Self::PartialBatch) delegates to the backend error
    /// behind it: re-publishing the outstanding records is worth attempting
    /// exactly when that error was.
    pub fn is_retryable(&self) -> bool {
        match self {
            ShoveError::Connection(_) => true,
            ShoveError::PartialBatch(f) => f.source().is_retryable(),
            _ => false,
        }
    }

    /// Returns `true` for an error that ends the run owning the consumer it
    /// came from: a consumer group's or a broadcast subscriber's
    /// `run_until_timeout` cancels its siblings, drains and returns when a
    /// member ends with one, and autoscaling does not replace that member.
    /// Every other error ends the member alone, as before.
    ///
    /// True for [`Commit`](Self::Commit). A fatal error is never retryable.
    pub fn is_fatal(&self) -> bool {
        matches!(self, ShoveError::Commit { .. })
    }
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, ShoveError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_serialization_error() {
        let json_err = serde_json::from_str::<String>("not json").unwrap_err();
        let err = ShoveError::Serialization(json_err);
        let msg = err.to_string();
        assert!(msg.starts_with("serialization error:"), "got: {msg}");
    }

    #[test]
    fn display_connection_error() {
        let err = ShoveError::Connection("channel closed".into());
        assert_eq!(err.to_string(), "connection error: channel closed");
    }

    #[test]
    fn display_topology_error() {
        let err = ShoveError::Topology("missing exchange".into());
        assert_eq!(err.to_string(), "topology error: missing exchange");
    }

    #[test]
    fn from_serde_json_error() {
        let json_err = serde_json::from_str::<String>("{}").unwrap_err();
        let err: ShoveError = json_err.into();
        assert!(matches!(err, ShoveError::Serialization(_)));
    }

    #[test]
    fn display_codec_error() {
        let inner: Box<dyn std::error::Error + Send + Sync> = "boom".into();
        let err = ShoveError::Codec {
            codec: "protobuf",
            source: inner,
        };
        let msg = err.to_string();
        assert!(msg.contains("codec error in protobuf"), "got: {msg}");
    }

    #[test]
    fn codec_error_is_not_retryable() {
        let inner: Box<dyn std::error::Error + Send + Sync> = "boom".into();
        let err = ShoveError::Codec {
            codec: "json",
            source: inner,
        };
        assert!(!err.is_retryable());
    }

    fn commit_error() -> ShoveError {
        ShoveError::Commit {
            topic: "orders".into(),
            offsets: vec![(0, 8), (3, 12)],
            kind: CommitFailure::Rejected("Broker: Group authorization failed".into()),
        }
    }

    #[test]
    fn display_commit_error_names_the_topic_the_offsets_and_the_kind() {
        assert_eq!(
            commit_error().to_string(),
            "final offset commit on 'orders' did not land for [0@8, 3@12]: \
             rejected: Broker: Group authorization failed"
        );
        let deadline = ShoveError::Commit {
            topic: "orders".into(),
            offsets: vec![(0, 8)],
            kind: CommitFailure::Deadline(Duration::from_secs(20)),
        };
        assert_eq!(
            deadline.to_string(),
            "final offset commit on 'orders' did not land for [0@8]: no answer within the 20s \
             shutdown deadline; the result is unknown"
        );
        assert_eq!(
            CommitFailure::NoThread.to_string(),
            "no thread could be spawned for the commit; nothing was committed"
        );
    }

    /// A failed final commit ends the owning run and is never retried: a
    /// reconnect would rejoin the group with the position still uncommitted
    /// and read as a clean member.
    #[test]
    fn commit_error_is_fatal_and_not_retryable() {
        let err = commit_error();
        assert!(err.is_fatal());
        assert!(!err.is_retryable());
    }

    /// Only `Commit` is fatal: a connection error keeps reconnecting and a
    /// topology error keeps ending the member alone.
    #[test]
    fn every_other_error_is_not_fatal() {
        for err in [
            ShoveError::Connection("channel closed".into()),
            ShoveError::Topology("missing exchange".into()),
            ShoveError::Validation("too large".into()),
            ShoveError::Unknown("boom".into()),
        ] {
            assert!(!err.is_fatal(), "{err}");
        }
    }

    /// `PartialBatch` has no retryability of its own — it inherits the
    /// backend error's. A connection blip is worth re-publishing for; a
    /// topology error is not, no matter how many records got through.
    #[test]
    fn partial_batch_delegates_retryability_to_its_source() {
        use crate::batch::BatchReport;

        let retryable = BatchReport::prefix(1, 3, ShoveError::Connection("channel closed".into()))
            .resolve(3)
            .result
            .unwrap_err();
        assert!(matches!(retryable, ShoveError::PartialBatch(_)));
        assert!(retryable.is_retryable());

        let permanent = BatchReport::prefix(1, 3, ShoveError::Topology("missing queue".into()))
            .resolve(3)
            .result
            .unwrap_err();
        assert!(matches!(permanent, ShoveError::PartialBatch(_)));
        assert!(!permanent.is_retryable());
    }
}

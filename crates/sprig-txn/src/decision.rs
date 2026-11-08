//! Decision records: the `__sprig_dtx` table (chapter 18, "Decision
//! table").
//!
//! Exactly one outcome is recorded per distributed transaction, by
//! whichever insert of its dtid commits first ("Deciding by insert race").

use std::fmt;

use sprig_common::ids::{ClusterId, ShardId};

use crate::xid::{AnchorRef, Dtid, bqual};

/// The decision table's name inside every shard schema.
pub const DTX_TABLE: &str = "__sprig_dtx";

/// `CREATE TABLE` for the decision table (chapters 15 and 21 create it with
/// every shard schema; the coordinator's tests and the resolver create it
/// when missing).
#[must_use]
pub fn create_table_sql(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS `{schema}`.`{DTX_TABLE}` (\
         dtid BINARY(16) NOT NULL, \
         outcome ENUM('commit','abort','pending') NOT NULL, \
         decider BINARY(16) NULL, \
         coordinator VARCHAR(64) NOT NULL, \
         participants VARBINARY(4096) NULL, \
         created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
         PRIMARY KEY (dtid), KEY (created_at)) ENGINE=InnoDB"
    )
}

/// A recorded outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Decision {
    /// Committed: the last resource committed with this row.
    Commit,
    /// Aborted: the resolver or a coordinator won the race.
    Abort,
    /// The home's pointer to the last resource's anchor, where the real
    /// decision is made.
    Pending,
}

impl Decision {
    /// The `ENUM` value.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Decision::Commit => "commit",
            Decision::Abort => "abort",
            Decision::Pending => "pending",
        }
    }

    /// From the `ENUM` value.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "commit" => Some(Decision::Commit),
            "abort" => Some(Decision::Abort),
            "pending" => Some(Decision::Pending),
            _ => None,
        }
    }
}

impl fmt::Display for Decision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How a participant takes part.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ParticipantKind {
    /// A prepared XA branch, finished in phase 2.
    Xa,
    /// The last resource: committed with the decision row.
    Last,
}

/// One writing branch, as the decision row lists it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Participant {
    /// The cluster.
    pub cluster: ClusterId,
    /// Every shard the branch touched; the first is in the bqual.
    pub shards: Vec<ShardId>,
    /// Prepared or last resource.
    pub kind: ParticipantKind,
}

impl Participant {
    /// The branch's bqual.
    #[must_use]
    pub fn bqual(&self) -> String {
        bqual(
            &self.cluster,
            self.shards.first().copied().unwrap_or(ShardId(0)),
        )
    }
}

/// Encodes participants as `x|l:<cluster>:<shard>,<shard>;...`.
#[must_use]
pub fn encode_participants(ps: &[Participant]) -> Vec<u8> {
    ps.iter()
        .map(|p| {
            let k = match p.kind {
                ParticipantKind::Xa => 'x',
                ParticipantKind::Last => 'l',
            };
            let shards: Vec<String> = p.shards.iter().map(ToString::to_string).collect();
            format!("{k}:{}:{}", p.cluster, shards.join(","))
        })
        .collect::<Vec<_>>()
        .join(";")
        .into_bytes()
}

/// Decodes [`encode_participants`]; malformed entries are skipped.
#[must_use]
pub fn decode_participants(b: &[u8]) -> Vec<Participant> {
    let Ok(s) = std::str::from_utf8(b) else {
        return Vec::new();
    };
    s.split(';')
        .filter_map(|e| {
            let mut it = e.splitn(3, ':');
            let kind = match it.next()? {
                "x" => ParticipantKind::Xa,
                "l" => ParticipantKind::Last,
                _ => return None,
            };
            let cluster = ClusterId::new(it.next()?).ok()?;
            let shards = it
                .next()?
                .split(',')
                .filter(|x| !x.is_empty())
                .map(|x| x.parse().map(ShardId))
                .collect::<Result<Vec<_>, _>>()
                .ok()?;
            Some(Participant {
                cluster,
                shards,
                kind,
            })
        })
        .collect()
}

/// One row of `__sprig_dtx`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionRow {
    /// The transaction.
    pub dtid: Dtid,
    /// The outcome.
    pub outcome: Decision,
    /// For `pending`: where the last resource's row is.
    pub decider: Option<AnchorRef>,
    /// The gateway id, or `resolver`.
    pub coordinator: String,
    /// The writing branches (on `commit` rows).
    pub participants: Vec<Participant>,
    /// Age of the row in seconds, when read back.
    pub age_seconds: Option<u64>,
}

impl DecisionRow {
    /// A row with no decider and no participants.
    #[must_use]
    pub fn new(dtid: Dtid, outcome: Decision, coordinator: &str) -> Self {
        Self {
            dtid,
            outcome,
            decider: None,
            coordinator: coordinator.chars().take(64).collect(),
            participants: Vec::new(),
            age_seconds: None,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn participants_round_trip() {
        let ps = vec![
            Participant {
                cluster: ClusterId::new("mum1-c7").unwrap(),
                shards: vec![ShardId(17), ShardId(18)],
                kind: ParticipantKind::Xa,
            },
            Participant {
                cluster: ClusterId::new("mum1-g2").unwrap(),
                shards: vec![ShardId(5)],
                kind: ParticipantKind::Last,
            },
        ];
        let b = encode_participants(&ps);
        assert_eq!(
            std::str::from_utf8(&b).unwrap(),
            "x:mum1-c7:17,18;l:mum1-g2:5"
        );
        assert_eq!(decode_participants(&b), ps);
        assert_eq!(ps[0].bqual(), "cmum1-c7.s17");
        assert!(decode_participants(b"junk;;q:x:1").is_empty());
    }

    #[test]
    fn ddl_names_the_table() {
        let s = create_table_sql("t1_d1_g1_s1");
        assert!(s.contains("`t1_d1_g1_s1`.`__sprig_dtx`"));
        assert!(s.contains("PRIMARY KEY (dtid)"));
    }
}

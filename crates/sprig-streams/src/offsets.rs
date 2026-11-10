//! Committing a consumer group's offset inside the caller's transaction
//! (chapter 45, "Exactly-once consume and write"; build step 15):
//! `sprig.commit_offset` from SQL, `db.commitOffset` from logic, and the
//! position of a trigger on a stream (chapter 41).
//!
//! `stream_offsets` lives in the tenant catalog, which a tenant's backend
//! accounts can't reach (chapter 29 grants them only shard schemas). So
//! the write is a procedure in the catalog, [`PROCEDURE`], created by
//! `sprig_ctl` with `SQL SECURITY DEFINER`, which the session calls on its
//! own backend connection, inside its transaction. The right to call it
//! comes from a MariaDB role per tenant, [`role`], holding only `EXECUTE`
//! on that procedure; the gateway grants the role to an account the first
//! time it commits an offset, and activates it (`SET ROLE`) for the one
//! call. The reconciler of chapter 29 reads and revokes only grants made
//! directly to `t<tenant>_u<user>` accounts, so it leaves the role alone.
//!
//! ```text
//! commit(group, stream, partition, offset, expect):
//!   expect absent:  upsert the row (Kafka's OffsetCommit semantics)
//!   expect present: compare and set: the row must hold `expect`, or the
//!                   call fails with 39877 (a trigger's position moved)
//! ```
//!
//! The compare-and-set gives a trigger exactly-once runs without leases:
//! the update locks the row, so two runs of one record serialize in
//! MariaDB, and the second finds the position moved and rolls back.

use mysql_async::prelude::Queryable;
use sprig_common::ids::TenantId;
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

/// The procedure's name in `t<tenant>_sprig`.
pub const PROCEDURE: &str = "__sprig_commit_offset";

/// The fan-in reader's procedure (R9): [`PROCEDURE`] with the member whose
/// instance the position is on (`stream_offsets.instance_tenant_id`).
pub const PROCEDURE_FAN_IN: &str = "__sprig_commit_offset_fi";

/// Consumer groups of triggers on streams start with this.
pub const LOGIC_GROUP_PREFIX: &str = "__sprig_logic_";

/// Errno the procedure raises when a compare-and-set finds another offset
/// (`ER_SPRIG_LOGIC_TRIGGER_POSITION_MOVED`).
pub const MOVED_ERRNO: u16 = 39877;

/// The consumer group of the trigger on a stream with binding `binding`
/// (chapter 41: `__sprig_logic_<binding id>`).
#[must_use]
pub fn logic_group(binding: u64) -> String {
    format!("{LOGIC_GROUP_PREFIX}{binding}")
}

/// The binding of a trigger's consumer group.
#[must_use]
pub fn binding_of_group(group: &str) -> Option<u64> {
    group.strip_prefix(LOGIC_GROUP_PREFIX)?.parse().ok()
}

/// The tenant's role that may call [`PROCEDURE`].
#[must_use]
pub fn role(tenant: TenantId) -> String {
    format!("t{tenant}_sprig_offsets")
}

/// `CREATE PROCEDURE IF NOT EXISTS` of [`PROCEDURE`] in catalog `schema`
/// (run as `sprig_ctl`, who becomes its definer).
#[must_use]
pub fn procedure_sql(schema: &str) -> String {
    let t = format!("{}.stream_offsets", quote(schema));
    format!(
        "CREATE PROCEDURE IF NOT EXISTS {}.{} (\n\
         IN p_group VARCHAR(255), IN p_stream BIGINT UNSIGNED, IN p_partition INT UNSIGNED,\n\
         IN p_offset BIGINT UNSIGNED, IN p_epoch INT UNSIGNED, IN p_meta VARCHAR(1024),\n\
         IN p_expect BIGINT)\n\
         MODIFIES SQL DATA SQL SECURITY DEFINER\n\
         COMMENT 'SprigDB: chapter 45 commit_offset inside the caller''s transaction'\n\
         BEGIN\n\
           IF p_expect IS NULL OR p_expect < 0 THEN\n\
             INSERT INTO {t} (group_name, stream_id, partition_no, offset_no, leader_epoch,\n\
                              metadata, committed_at)\n\
               VALUES (p_group, p_stream, p_partition, p_offset, p_epoch, p_meta, UTC_TIMESTAMP(3))\n\
               ON DUPLICATE KEY UPDATE offset_no = VALUES(offset_no),\n\
                 leader_epoch = VALUES(leader_epoch), metadata = VALUES(metadata),\n\
                 committed_at = VALUES(committed_at);\n\
           ELSE\n\
             UPDATE {t} SET offset_no = p_offset, committed_at = UTC_TIMESTAMP(3)\n\
               WHERE group_name = p_group AND stream_id = p_stream\n\
                 AND partition_no = p_partition AND offset_no = p_expect;\n\
             IF ROW_COUNT() = 0 THEN\n\
               SIGNAL SQLSTATE 'HY000' SET MYSQL_ERRNO = {MOVED_ERRNO},\n\
                 MESSAGE_TEXT = 'ER_SPRIG_LOGIC_TRIGGER_POSITION_MOVED: the consumer group is no longer at this offset';\n\
             END IF;\n\
           END IF;\n\
         END",
        quote(schema),
        quote(PROCEDURE),
    )
}

/// `CREATE PROCEDURE IF NOT EXISTS` of [`PROCEDURE_FAN_IN`] in catalog
/// `schema`: a compare-and-set (or an upsert without `p_expect`) of a
/// fan-in reader's position on member `p_instance`'s instance.
#[must_use]
pub fn procedure_fan_in_sql(schema: &str) -> String {
    let t = format!("{}.stream_offsets", quote(schema));
    format!(
        "CREATE PROCEDURE IF NOT EXISTS {}.{} (\n\
         IN p_group VARCHAR(255), IN p_instance BIGINT UNSIGNED, IN p_stream BIGINT UNSIGNED,\n\
         IN p_partition INT UNSIGNED, IN p_offset BIGINT UNSIGNED, IN p_expect BIGINT)\n\
         MODIFIES SQL DATA SQL SECURITY DEFINER\n\
         COMMENT 'SprigDB: chapter 45 fan-in positions inside the caller''s transaction'\n\
         BEGIN\n\
           IF p_expect IS NULL OR p_expect < 0 THEN\n\
             INSERT INTO {t} (group_name, instance_tenant_id, stream_id, partition_no,\n\
                              offset_no, committed_at)\n\
               VALUES (p_group, p_instance, p_stream, p_partition, p_offset, UTC_TIMESTAMP(3))\n\
               ON DUPLICATE KEY UPDATE offset_no = VALUES(offset_no),\n\
                 committed_at = VALUES(committed_at);\n\
           ELSE\n\
             UPDATE {t} SET offset_no = p_offset, committed_at = UTC_TIMESTAMP(3)\n\
               WHERE group_name = p_group AND instance_tenant_id = p_instance\n\
                 AND stream_id = p_stream AND partition_no = p_partition\n\
                 AND offset_no = p_expect;\n\
             IF ROW_COUNT() = 0 THEN\n\
               SIGNAL SQLSTATE 'HY000' SET MYSQL_ERRNO = {MOVED_ERRNO},\n\
                 MESSAGE_TEXT = 'ER_SPRIG_LOGIC_TRIGGER_POSITION_MOVED: the consumer group is no longer at this offset';\n\
             END IF;\n\
           END IF;\n\
         END",
        quote(schema),
        quote(PROCEDURE_FAN_IN),
    )
}

/// The procedure `c` calls, made if it is missing.
#[must_use]
pub fn procedure_for(schema: &str, c: &Commit) -> String {
    if c.instance == 0 {
        procedure_sql(schema)
    } else {
        procedure_fan_in_sql(schema)
    }
}

/// One commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    /// The consumer group.
    pub group: String,
    /// The member whose instance `stream` is (a fan-in reader, R9); 0 for
    /// the tenant's own stream.
    pub instance: u64,
    /// The stream.
    pub stream: u64,
    /// The partition.
    pub partition: u32,
    /// The next offset to read.
    pub offset: u64,
    /// The leader epoch, if known.
    pub epoch: Option<u32>,
    /// Kafka's metadata.
    pub metadata: Option<String>,
    /// Compare and set: the offset the row must hold.
    pub expect: Option<u64>,
}

/// A SQL string literal of `s` that means the same under every `sql_mode`.
fn text_literal(s: &str) -> String {
    use std::fmt::Write as _;
    if s.is_empty() {
        return "''".to_owned();
    }
    let mut out = String::with_capacity(12 + s.len() * 2);
    out.push_str("_utf8mb4 X'");
    for b in s.as_bytes() {
        let _ = write!(out, "{b:02X}");
    }
    out.push('\'');
    out
}

/// The `CALL` a session runs (physical names) for `c` in catalog `schema`.
#[must_use]
pub fn call_sql(schema: &str, c: &Commit) -> String {
    let opt_u = |v: Option<u64>| v.map_or_else(|| "NULL".to_owned(), |v| v.to_string());
    if c.instance != 0 {
        return format!(
            "CALL {}.{}({}, {}, {}, {}, {}, {})",
            quote(schema),
            quote(PROCEDURE_FAN_IN),
            text_literal(&c.group),
            c.instance,
            c.stream,
            c.partition,
            c.offset,
            opt_u(c.expect),
        );
    }
    format!(
        "CALL {}.{}({}, {}, {}, {}, {}, {}, {})",
        quote(schema),
        quote(PROCEDURE),
        text_literal(&c.group),
        c.stream,
        c.partition,
        c.offset,
        opt_u(c.epoch.map(u64::from)),
        c.metadata
            .as_deref()
            .map_or_else(|| "NULL".to_owned(), text_literal),
        opt_u(c.expect),
    )
}

/// The statements (as `sprig_ctl`) that let account `user@'%'` call the
/// procedure: the procedure, the role, its one grant, the role's grant.
#[must_use]
pub fn grant_sql(schema: &str, tenant: TenantId, user: &str) -> Vec<String> {
    let r = role(tenant);
    vec![
        procedure_sql(schema),
        procedure_fan_in_sql(schema),
        format!("CREATE ROLE IF NOT EXISTS '{r}'"),
        format!(
            "GRANT EXECUTE ON PROCEDURE {}.{} TO '{r}'",
            quote(schema),
            quote(PROCEDURE)
        ),
        format!(
            "GRANT EXECUTE ON PROCEDURE {}.{} TO '{r}'",
            quote(schema),
            quote(PROCEDURE_FAN_IN)
        ),
        format!("GRANT '{r}' TO '{}'@'%'", user.replace('\'', "''")),
    ]
}

/// A `CALL sprig.commit_offset(...)` statement: its argument texts, split
/// at top-level commas. `None` for any other statement.
#[must_use]
pub fn parse_call(sql: &str) -> Option<Vec<String>> {
    let t = sql.trim().trim_end_matches(';').trim();
    let lower = t.to_ascii_lowercase();
    let rest = lower.strip_prefix("call")?;
    if !rest.starts_with(|c: char| c.is_whitespace()) {
        return None;
    }
    let off = t.len() - rest.len();
    let rest_t = t[off..].trim_start();
    let name_end = rest_t.find('(')?;
    let name: String = rest_t[..name_end]
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '`')
        .collect::<String>()
        .to_ascii_lowercase();
    if name != "sprig.commit_offset" {
        return None;
    }
    let args = rest_t[name_end + 1..].trim_end();
    let args = args.strip_suffix(')')?;
    Some(split_args(args))
}

/// Splits at commas outside parentheses, quotes and backticks.
fn split_args(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut depth = 0i32;
    let mut quote_ch: Option<char> = None;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if let Some(q) = quote_ch {
            cur.push(c);
            if c == '\\' && q != '`' {
                if let Some(n) = chars.next() {
                    cur.push(n);
                }
            } else if c == q {
                if chars.peek() == Some(&q) {
                    cur.push(q);
                    chars.next();
                } else {
                    quote_ch = None;
                }
            }
            continue;
        }
        match c {
            '\'' | '"' | '`' => {
                quote_ch = Some(c);
                cur.push(c);
            }
            '(' => {
                depth += 1;
                cur.push(c);
            }
            ')' => {
                depth -= 1;
                cur.push(c);
            }
            ',' if depth == 0 => out.push(std::mem::take(&mut cur).trim().to_owned()),
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() || !out.is_empty() {
        out.push(cur.trim().to_owned());
    }
    out
}

/// Reads a group's committed offsets of one stream, by partition (as
/// `sprig_ctl`, on the catalog).
///
/// # Errors
/// The backend's.
pub async fn positions<Q: Queryable + Send>(
    q: &mut Q,
    schema: &str,
    group: &str,
    stream: u64,
) -> Result<Vec<(u32, u64)>> {
    positions_of(q, schema, group, 0, stream).await
}

/// [`positions`] on member `instance`'s instance `stream` (a fan-in
/// reader, R9; 0 for the tenant's own stream).
///
/// # Errors
/// The backend's.
pub async fn positions_of<Q: Queryable + Send>(
    q: &mut Q,
    schema: &str,
    group: &str,
    instance: u64,
    stream: u64,
) -> Result<Vec<(u32, u64)>> {
    q.exec(
        format!(
            "SELECT partition_no, offset_no FROM {}.stream_offsets \
             WHERE group_name = ? AND instance_tenant_id = ? AND stream_id = ? \
             ORDER BY partition_no",
            quote(schema)
        ),
        (group, instance, stream),
    )
    .await
    .map_err(crate::maria::map_err)
}

/// Creates the rows of `group` that are missing, at `start` per partition
/// (`INSERT IGNORE`: a row that exists keeps its offset).
///
/// # Errors
/// The backend's.
pub async fn init_positions<Q: Queryable + Send>(
    q: &mut Q,
    schema: &str,
    group: &str,
    stream: u64,
    start: &[(u32, u64)],
) -> Result<()> {
    init_positions_of(q, schema, group, 0, stream, start).await
}

/// [`init_positions`] on member `instance`'s instance `stream`.
///
/// # Errors
/// The backend's.
pub async fn init_positions_of<Q: Queryable + Send>(
    q: &mut Q,
    schema: &str,
    group: &str,
    instance: u64,
    stream: u64,
    start: &[(u32, u64)],
) -> Result<()> {
    for (p, o) in start {
        q.exec_drop(
            format!(
                "INSERT IGNORE INTO {}.stream_offsets (group_name, instance_tenant_id, \
                 stream_id, partition_no, offset_no, leader_epoch, metadata, committed_at) \
                 VALUES (?, ?, ?, ?, ?, NULL, NULL, UTC_TIMESTAMP(3))",
                quote(schema)
            ),
            (group, instance, stream, *p, *o),
        )
        .await
        .map_err(crate::maria::map_err)?;
    }
    Ok(())
}

/// Moves `group`'s offset of one partition from `expect` to `to` outside
/// any transaction (a dead-lettered record): `false` when the row held
/// another offset.
///
/// # Errors
/// The backend's.
pub async fn advance(
    q: &mut mysql_async::Conn,
    schema: &str,
    group: &str,
    stream: u64,
    partition: u32,
    expect: u64,
    to: u64,
) -> Result<bool> {
    advance_of(q, schema, group, 0, stream, partition, expect, to).await
}

/// [`advance`] on member `instance`'s instance `stream`.
///
/// # Errors
/// The backend's.
#[allow(clippy::too_many_arguments)]
pub async fn advance_of(
    q: &mut mysql_async::Conn,
    schema: &str,
    group: &str,
    instance: u64,
    stream: u64,
    partition: u32,
    expect: u64,
    to: u64,
) -> Result<bool> {
    q.exec_drop(
        format!(
            "UPDATE {}.stream_offsets SET offset_no = ?, committed_at = UTC_TIMESTAMP(3) \
             WHERE group_name = ? AND instance_tenant_id = ? AND stream_id = ? \
             AND partition_no = ? AND offset_no = ?",
            quote(schema)
        ),
        (to, group, instance, stream, partition, expect),
    )
    .await
    .map_err(crate::maria::map_err)?;
    Ok(q.affected_rows() > 0)
}

/// Whether `e` is the procedure's compare-and-set failure.
#[must_use]
pub fn is_moved(e: &SprigError) -> bool {
    e.code() == Some(SprigCode::LogicTriggerPositionMoved)
        || matches!(e.kind, sprig_common::ErrorKind::Backend { errno, .. } if errno == MOVED_ERRNO)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups_and_roles() {
        assert_eq!(logic_group(42), "__sprig_logic_42");
        assert_eq!(binding_of_group("__sprig_logic_42"), Some(42));
        assert_eq!(binding_of_group("billing"), None);
        assert_eq!(role(TenantId(7)), "t7_sprig_offsets");
    }

    #[test]
    fn parses_calls() {
        assert_eq!(
            parse_call("CALL sprig.commit_offset('billing', 'order_events', 3, 1205);"),
            Some(vec![
                "'billing'".to_owned(),
                "'order_events'".to_owned(),
                "3".to_owned(),
                "1205".to_owned()
            ])
        );
        assert_eq!(
            parse_call("call `sprig`.`commit_offset` ( 'a,b', CONCAT('x', 'y'), 0, @o + 1 )"),
            Some(vec![
                "'a,b'".to_owned(),
                "CONCAT('x', 'y')".to_owned(),
                "0".to_owned(),
                "@o + 1".to_owned()
            ])
        );
        assert_eq!(parse_call("CALL sprig.mq_ack('x')"), None);
        assert_eq!(parse_call("SELECT 1"), None);
        assert_eq!(parse_call("CALLsprig.commit_offset(1)"), None);
    }

    #[test]
    fn call_text_is_literal() {
        let c = Commit {
            group: "g'1".into(),
            instance: 0,
            stream: 41,
            partition: 3,
            offset: 1206,
            epoch: None,
            metadata: None,
            expect: Some(1205),
        };
        assert_eq!(
            call_sql("t9_sprig", &c),
            "CALL `t9_sprig`.`__sprig_commit_offset`(_utf8mb4 X'672731', 41, 3, 1206, NULL, NULL, 1205)"
        );
        let sql = procedure_sql("t9_sprig");
        assert!(
            sql.starts_with("CREATE PROCEDURE IF NOT EXISTS `t9_sprig`.`__sprig_commit_offset`")
        );
        assert!(sql.contains("MYSQL_ERRNO = 39877"));
        let g = grant_sql("t9_sprig", TenantId(9), "t9_u3");
        assert_eq!(g[5], "GRANT 't9_sprig_offsets' TO 't9_u3'@'%'");
        assert!(g[4].contains("`__sprig_commit_offset_fi`"));
        // A fan-in reader's position names the member.
        let f = Commit {
            instance: 26_306_000_003,
            ..c
        };
        assert_eq!(
            call_sql("t9_sprig", &f),
            "CALL `t9_sprig`.`__sprig_commit_offset_fi`(_utf8mb4 X'672731', 26306000003, 41, 3, 1206, 1205)"
        );
        assert!(procedure_for("t9_sprig", &f).contains("instance_tenant_id = p_instance"));
        assert!(super::super::store::ALTER_STREAM_OFFSETS_INSTANCE.contains("ADD PRIMARY KEY (group_name, instance_tenant_id"));
    }
}

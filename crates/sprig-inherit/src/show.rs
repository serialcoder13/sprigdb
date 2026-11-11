//! What schema tools see (chapter 42, "Security notes"; chapter 07's
//! hiding rules): an inheritable object looks like the table the client
//! created. Its `_sprig_` columns and indexes are hidden by chapter 07;
//! `SHOW CREATE TABLE` gets back the primary and unique keys chapter 07
//! drops because `_sprig_space` widened them. `SELECT *` (also `mysqldump`'s
//! read) sees the session space's merged view, so a dump of a space is its
//! merged data as plain tables.
//!
//! `SHOW SPACES` and `SHOW SHARES` list the tenant's spaces and the shares
//! it grants and receives.

use bytes::Bytes;
use sprig_common::Result;
use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::resultset::column_type;
use sprig_types::Rows;

use crate::grammar::Name;
use crate::model::ShareTarget;
use crate::plan::{Ctx, state_of};
use crate::view::q;
use crate::{Handled, Runner};

fn columns(names: &[&str]) -> Vec<ColumnDef> {
    names
        .iter()
        .map(|n| ColumnDef {
            name: Bytes::copy_from_slice(n.as_bytes()),
            org_name: Bytes::copy_from_slice(n.as_bytes()),
            col_type: column_type::VAR_STRING,
            charset: 45,
            length: 1024,
            ..ColumnDef::default()
        })
        .collect()
}

/// A text result set.
pub(crate) fn text_rows(names: &[&str], rows: Vec<Vec<Option<Vec<u8>>>>) -> Rows {
    Rows {
        columns: columns(names),
        rows,
    }
}

fn row(vals: Vec<Option<String>>) -> Vec<Option<Vec<u8>>> {
    vals.into_iter()
        .map(|v| v.map(String::into_bytes))
        .collect()
}

/// Puts the key lines chapter 07 hid back into a `CREATE TABLE` text.
#[must_use]
pub fn restore_keys(text: &str, key: &[String], uniques: &[crate::model::UniqueKey]) -> String {
    let lines: Vec<&str> = text.lines().collect();
    if lines.len() < 2 {
        return text.to_owned();
    }
    let close = lines
        .iter()
        .rposition(|l| l.starts_with(')'))
        .unwrap_or(lines.len() - 1);
    let mut body: Vec<String> = lines[1..close]
        .iter()
        .map(|l| l.trim_end_matches(',').to_owned())
        .collect();
    let list = |cs: &[String]| cs.iter().map(|c| q(c)).collect::<Vec<_>>().join(",");
    let mut keys = vec![format!("  PRIMARY KEY ({})", list(key))];
    for u in uniques {
        keys.push(format!(
            "  UNIQUE KEY {} ({})",
            q(&u.name),
            list(&u.columns)
        ));
    }
    let at = body
        .iter()
        .position(|l| !l.starts_with("  `"))
        .unwrap_or(body.len());
    for (i, k) in keys.into_iter().enumerate() {
        if !body.iter().any(|l| l == &k) {
            body.insert(at + i, k);
        }
    }
    let mut out = String::from(lines[0]);
    out.push('\n');
    out.push_str(&body.join(",\n"));
    out.push('\n');
    out.push_str(&lines[close..].join("\n"));
    out
}

/// `SHOW CREATE TABLE` of an inheritable object.
///
/// # Errors
/// The statement's.
pub async fn create(n: &Name, ctx: &Ctx<'_>, r: &mut dyn Runner) -> Result<Handled> {
    let Some(res) = ctx.resolve(n)? else {
        return Ok(Handled::NotInherit);
    };
    let mut rows = r.query(&format!("SHOW CREATE TABLE {}", res.layer)).await?;
    for row in &mut rows.rows {
        if let Some(Some(v)) = row.get_mut(1) {
            let t = String::from_utf8_lossy(v).into_owned();
            *v = restore_keys(&t, &res.obj.key, &res.obj.uniques).into_bytes();
        }
    }
    Ok(Handled::Rows(rows))
}

/// `SHOW SPACES`: `Space`, `Parent`, `Depth`, `Home` (`YES` for the
/// session user's).
#[must_use]
pub fn spaces(ctx: &Ctx<'_>) -> Rows {
    let mut out = Rows {
        columns: columns(&["Space", "Parent", "Depth", "Current"]),
        rows: Vec::new(),
    };
    let mut all: Vec<crate::model::Space> = ctx.state.spaces().cloned().collect();
    if !all.iter().any(|s| s.id == crate::ROOT_SPACE)
        && let Some(root) = ctx.state.space(crate::ROOT_SPACE, ctx.tenant_name)
    {
        all.insert(0, root);
    }
    for s in all {
        let parent = s.parent.map(|p| ctx.space_name(p)).or_else(|| {
            ctx.cat
                .ancestry
                .last()
                .map(|a| format!("{} (tenant)", a.name))
        });
        out.rows.push(row(vec![
            Some(s.name),
            parent,
            Some(s.depth.to_string()),
            Some(if s.id == ctx.space { "YES" } else { "NO" }.into()),
        ]));
    }
    out
}

/// `SHOW SHARES`: the shares this tenant grants and those it receives
/// from its parent tenant.
#[must_use]
pub fn shares(ctx: &Ctx<'_>) -> Rows {
    let mut out = Rows {
        columns: columns(&[
            "Share", "Role", "Object", "From", "To", "Filter", "Take", "Columns", "Reshare",
            "Mode", "Copy", "Lag_ms",
        ]),
        rows: Vec::new(),
    };
    let obj = |id| {
        ctx.state.object(id).map_or_else(
            || id.to_string(),
            |o| format!("{}.{}", ctx.db_name(o.db).unwrap_or_default(), o.table),
        )
    };
    // A copy's state and how far behind it may be (chapter 42,
    // "Materialized copies"): now minus when the worker last had nothing
    // left to apply.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    let copy = |object, share| {
        ctx.copies
            .0
            .get(&crate::copy::CopyKey {
                tenant: ctx.cat.tenant,
                object,
                share,
            })
            .map_or((None, None), |i| {
                (
                    Some(if i.serving { "serving" } else { "seeding" }.to_owned()),
                    Some(now.saturating_sub(i.caught_up_ms).to_string()),
                )
            })
    };
    for s in ctx.state.shares() {
        let (cs, lag) = copy(s.object_id, s.id);
        let to = match s.to {
            ShareTarget::Space(c) => ctx.space_name(c),
            ShareTarget::Tenant(t) => format!("tenant {t}"),
            ShareTarget::AllChildren => "ALL CHILDREN".into(),
        };
        out.rows.push(row(vec![
            Some(s.name.clone()),
            Some("granted".into()),
            Some(obj(s.object_id)),
            Some(ctx.space_name(s.from)),
            Some(to),
            s.filter.clone(),
            s.take.clone(),
            s.columns.as_ref().map(|c| c.join(", ")),
            Some(if s.reshare { "YES" } else { "NO" }.into()),
            Some(s.mode.as_str().into()),
            cs,
            lag,
        ]));
    }
    if let Some(parent) = ctx.ancestors.last()
        && let Some(ps) = state_of(parent)
    {
        for o in ctx.state.objects() {
            let Ok(Some(up)) = ctx.up_edge(o) else {
                continue;
            };
            let _ = ps;
            let (cs, lag) = copy(o.object_id, crate::copy::UPSTREAM);
            out.rows.push(row(vec![
                Some(up.share.name.clone()),
                Some("received".into()),
                Some(obj(o.object_id)),
                Some(
                    ctx.cat
                        .ancestry
                        .last()
                        .map_or_else(String::new, |a| a.name.clone()),
                ),
                Some(ctx.tenant_name.to_owned()),
                up.share.filter.clone(),
                up.take.clone(),
                up.share.columns.as_ref().map(|c| c.join(", ")),
                Some(if up.share.reshare { "YES" } else { "NO" }.into()),
                Some(up.share.mode.as_str().into()),
                cs,
                lag,
            ]));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::UniqueKey;

    #[test]
    fn keys_come_back() {
        let t = "CREATE TABLE `products` (\n  `id` bigint(20) NOT NULL,\n  `sku` varchar(20) \
                 DEFAULT NULL,\n  KEY `k_sku` (`sku`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        let out = restore_keys(
            t,
            &["id".into()],
            &[UniqueKey {
                name: "uk_sku".into(),
                columns: vec!["sku".into()],
            }],
        );
        assert_eq!(
            out,
            "CREATE TABLE `products` (\n  `id` bigint(20) NOT NULL,\n  `sku` varchar(20) \
             DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `uk_sku` (`sku`),\n  KEY \
             `k_sku` (`sku`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        );
        let plain = "CREATE TABLE `p` (\n  `id` int(11) NOT NULL\n) ENGINE=InnoDB";
        assert_eq!(
            restore_keys(plain, &["id".into()], &[]),
            "CREATE TABLE `p` (\n  `id` int(11) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB"
        );
    }
}

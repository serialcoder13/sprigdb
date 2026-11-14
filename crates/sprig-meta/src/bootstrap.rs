//! Bootstrapping the first DC (the `sprig-meta` part).
//!
//! A controller with `[controller] bootstrap = true` that finds
//! `settings/system.deployment_id` missing writes the deployment id,
//! `dcs/<dc>` (`active`) and `ids/tenant` in one txn guarded by `Missing`.
//! Every other process checks that its `[meta] deployment_id` matches.
//!
//! In single-etcd mode the global store is
//! [`EtcdStore::with_tier`](crate::etcd::EtcdStore::with_tier) over the DC
//! store's connection.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sprig_common::ids::DcId;
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};

use crate::{Cmp, Consistency, Key, MetaStore, Op, OpResult, Record, Tier, Txn, Versioned};

/// The global setting holding the deployment id.
pub const DEPLOYMENT_ID_SETTING: &str = "system.deployment_id";

/// A setting record. `sprig-settings` owns
/// resolution; bootstrap only writes locked internal keys.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettingRecord {
    /// The value; absent when the record only sets bounds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// Bounds on every lower scope.
    #[serde(default)]
    pub bounds: Map<String, Value>,
    /// Who set it.
    pub set_by: String,
    /// When, RFC 3339 UTC; for people only.
    pub set_at: String,
    /// Why.
    #[serde(default)]
    pub reason: String,
}

impl Versioned for SettingRecord {
    const V: u32 = 1;
}

/// The fields of a DC record that bootstrap writes; later builds add endpoints and health, which are kept.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DcRecord {
    /// The DC.
    pub dc_id: DcId,
    /// `full` or `witness`.
    pub kind: String,
    /// `joining`, `active`, `draining`, `retired`, or a failover state.
    pub state: String,
    /// Raised each time the DC takes a new DC lease.
    pub dc_epoch: u64,
    /// Country, jurisdiction and other tags.
    #[serde(default)]
    pub tags: Map<String, Value>,
    /// Whether a global etcd voter runs in this DC.
    #[serde(default)]
    pub global_voter: bool,
}

impl Versioned for DcRecord {
    const V: u32 = 1;
}

/// `ids/tenant`: the next tenant id. Tenant id 0 means "no parent".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantIdCounter {
    /// The id the next tenant gets.
    pub next: u64,
}

impl Versioned for TenantIdCounter {
    const V: u32 = 1;
}

/// What [`bootstrap_first_dc`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bootstrap {
    /// This call wrote the deployment.
    Created,
    /// The deployment already existed with the same id.
    AlreadyDone,
}

/// The global keys bootstrap writes, for `dc`.
///
/// # Errors
/// Internal if a key can't be built.
pub fn bootstrap_keys(dc: &DcId) -> Result<[Key; 3]> {
    let g = Tier::Global;
    Ok([
        g.key(&format!("settings/{DEPLOYMENT_ID_SETTING}"))?,
        g.key(&format!("dcs/{dc}"))?,
        g.key("ids/tenant")?,
    ])
}

/// Writes the first DC's deployment, once.
///
/// # Errors
/// 39998 `ER_SPRIG_CONFIG_INVALID` if the store holds another deployment id,
/// or `dcs/<dc>` or `ids/tenant` exist without one; internal if `global`
/// isn't the global tier; the store's errors.
pub async fn bootstrap_first_dc(
    global: &dyn MetaStore,
    clock: &dyn Clock,
    deployment_id: &str,
    dc: &DcId,
) -> Result<Bootstrap> {
    if global.tier() != &Tier::Global {
        return Err(SprigError::internal("bootstrap needs the global tier"));
    }
    if deployment_id.is_empty() {
        return Err(SprigError::new(
            SprigCode::ConfigInvalid,
            "[meta] deployment_id is empty",
        ));
    }
    let [dep_key, dc_key, ids_key] = bootstrap_keys(dc)?;
    let set_at = rfc3339(clock.wall());
    let mut bounds = Map::new();
    bounds.insert("locked".to_owned(), Value::Bool(true));
    let dep = Record::new(SettingRecord {
        value: Some(Value::String(deployment_id.to_owned())),
        bounds,
        set_by: "bootstrap".to_owned(),
        set_at,
        reason: "first DC bootstrap".to_owned(),
    });
    let dc_rec = Record::new(DcRecord {
        dc_id: dc.clone(),
        kind: "full".to_owned(),
        state: "active".to_owned(),
        dc_epoch: 0,
        tags: Map::new(),
        global_voter: true,
    });
    let ids = Record::new(TenantIdCounter { next: 1 });
    let put = |key: &Key, value: Vec<u8>| Op::Put {
        key: key.clone(),
        value,
        lease: None,
    };
    let txn = Txn {
        when: vec![
            Cmp::Missing(dep_key.clone()),
            Cmp::Missing(dc_key.clone()),
            Cmp::Missing(ids_key.clone()),
        ],
        then: vec![
            put(&dep_key, dep.encode()?),
            put(&dc_key, dc_rec.encode()?),
            put(&ids_key, ids.encode()?),
        ],
        otherwise: vec![Op::Get(dep_key.clone())],
    };
    let r = global.txn(txn).await?;
    if r.succeeded {
        tracing::info!(%dc, deployment_id, "bootstrapped the first DC");
        return Ok(Bootstrap::Created);
    }
    match r.results.first() {
        Some(OpResult::Get(Some(kv))) => {
            check_id(&kv.value, deployment_id)?;
            Ok(Bootstrap::AlreadyDone)
        }
        _ => Err(SprigError::new(
            SprigCode::ConfigInvalid,
            format!("{dc_key} or {ids_key} exist without {dep_key}: refusing to bootstrap"),
        )),
    }
}

/// Checks that the deployment's id matches `[meta] deployment_id`.
///
/// # Errors
/// 39998 `ER_SPRIG_CONFIG_INVALID` if it differs or the deployment isn't
/// bootstrapped; the store's errors.
pub async fn verify_deployment_id(global: &dyn MetaStore, deployment_id: &str) -> Result<()> {
    let key = Tier::Global.key(&format!("settings/{DEPLOYMENT_ID_SETTING}"))?;
    match global.get(&key, Consistency::Linearizable).await? {
        Some(kv) => check_id(&kv.value, deployment_id),
        None => Err(SprigError::new(
            SprigCode::ConfigInvalid,
            "the deployment is not bootstrapped yet",
        )),
    }
}

fn check_id(stored: &[u8], expected: &str) -> Result<()> {
    let rec = Record::<SettingRecord>::decode(stored)?;
    match rec.body.value.as_ref().and_then(Value::as_str) {
        Some(id) if id == expected => Ok(()),
        Some(id) => Err(SprigError::new(
            SprigCode::ConfigInvalid,
            format!("[meta] deployment_id {expected:?} doesn't match this deployment's {id:?}"),
        )),
        None => Err(SprigError::internal(format!(
            "{DEPLOYMENT_ID_SETTING} has no string value"
        ))),
    }
}

/// Formats a time as RFC 3339 UTC with second precision, as records store
/// wall-clock times (`set_at`).
#[must_use]
#[allow(clippy::many_single_char_names)] // the algorithm's usual names
pub fn rfc3339(t: SystemTime) -> String {
    let secs = t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (h, m, s) = (rem / 3600, rem % 3600 / 60, rem % 60);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + u64::from(mo <= 2);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn formats_rfc3339() {
        assert_eq!(rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        let t = UNIX_EPOCH + Duration::from_secs(1_802_338_200);
        assert_eq!(rfc3339(t), "2027-02-11T09:30:00Z");
        let leap = UNIX_EPOCH + Duration::from_secs(951_782_400);
        assert_eq!(rfc3339(leap), "2000-02-29T00:00:00Z");
    }
}

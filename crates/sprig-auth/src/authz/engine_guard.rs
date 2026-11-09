//! The engine-switch guard (chapter 29, "Choosing the engine"): a change
//! of `auth.authorizer`, at any scope, is refused unless every live
//! gateway reports that it knows the engine.
//!
//! Gateways advertise the engines they know ([`super::authorizer::KNOWN`])
//! in their registration record, `/sprig/dc/<dc_id>/gateways/<id>`
//! (chapter 04), field `authorizers`. The record is bound to the gateway's
//! lease, so the records listed are the live gateways. A record without
//! the field comes from a build older than the guard, which knows
//! `mysql_grants` only.
//!
//! [`EngineGuard`] is a [`ValueGuard`]: the settings writers (etcd scopes
//! and catalog scopes) call it for every `Set` of a key, so SQL's
//! `SPRIG SET`, the Admin API and `sprigctl settings set` all pass through
//! it. It looks at every DC in `/sprig/g/dcs/` (and its own) through the
//! DC tier it is given, which reaches every DC in single-etcd mode (R0).
//! A failure to read the records refuses the change. A name this build
//! doesn't know either is left to the key's bounds (39080), so the guard
//! only speaks for engines the writer's own build has.
//!
//! The gateway's runtime refusal stays: a gateway that doesn't know the
//! configured engine refuses the tenant's sessions everything. After the
//! guard it is reached only by a gateway started later from an older
//! build (a rollback).

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_common::ids::DcId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{ListOpts, MetaStore, Record, Tier, TierView, Versioned};
use sprig_settings::{GuardFuture, Value, ValueGuard};

use super::authorizer::{KNOWN, MYSQL_GRANTS};
use super::settings::AUTHORIZER;

/// What the guard reads of a gateway's registration record (the rest of
/// it is `sprig_gateway::registry::GatewayRecord`'s).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayEngines {
    /// The gateway's id.
    pub gateway_id: String,
    /// The engines it knows; a record from before the field knows
    /// `mysql_grants` only.
    #[serde(default = "before_the_field")]
    pub authorizers: Vec<String>,
}

impl Versioned for GatewayEngines {
    const V: u32 = 1;
}

fn before_the_field() -> Vec<String> {
    vec![MYSQL_GRANTS.to_owned()]
}

/// What the guard reads of a DC record.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct DcState {
    #[serde(default)]
    state: String,
}

impl Versioned for DcState {
    const V: u32 = 1;
}

/// Refuses `auth.authorizer` values some live gateway doesn't know.
#[derive(Clone)]
pub struct EngineGuard {
    global: Arc<dyn MetaStore>,
    dc: Arc<dyn MetaStore>,
}

impl std::fmt::Debug for EngineGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineGuard")
            .field("global", self.global.tier())
            .field("dc", self.dc.tier())
            .finish()
    }
}

/// Every key of a listing, all pages.
async fn list_all(store: &dyn MetaStore, rel: &str) -> Result<Vec<sprig_meta::KeyValue>> {
    let prefix = store.tier().key(rel)?;
    let mut out = Vec::new();
    let mut opts = ListOpts::default();
    loop {
        let page = store.list(&prefix, opts.clone()).await?;
        out.extend(page.kvs.iter().cloned());
        if !page.more {
            return Ok(out);
        }
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
    }
}

impl EngineGuard {
    /// A guard reading DCs from `global` and gateway records through `dc`
    /// (a DC tier store, viewed as each DC's tier).
    #[must_use]
    pub fn new(global: Arc<dyn MetaStore>, dc: Arc<dyn MetaStore>) -> Self {
        Self { global, dc }
    }

    /// The live gateways of every DC that don't know `engine`, as
    /// `<gateway id> (dc <dc>)`.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn gateways_without(&self, engine: &str) -> Result<Vec<String>> {
        let mut dcs: Vec<DcId> = Vec::new();
        if let Tier::Dc(own) = self.dc.tier() {
            dcs.push(own.clone());
        }
        for kv in list_all(self.global.as_ref(), "dcs/").await? {
            let Some(id) = kv
                .key
                .as_str()
                .rsplit('/')
                .next()
                .and_then(|s| DcId::new(s).ok())
            else {
                continue;
            };
            let retired =
                Record::<DcState>::decode(&kv.value).is_ok_and(|r| r.body.state == "retired");
            if !retired && !dcs.contains(&id) {
                dcs.push(id);
            }
        }
        let mut missing = Vec::new();
        for dc in dcs {
            let tier = TierView::over(&self.dc, Tier::Dc(dc.clone()));
            for kv in list_all(tier.as_ref(), "gateways/").await? {
                match Record::<GatewayEngines>::decode(&kv.value) {
                    Ok(r) if r.body.authorizers.iter().any(|a| a == engine) => {}
                    Ok(r) => missing.push(format!("{} (dc {dc})", r.body.gateway_id)),
                    Err(_) => missing.push(format!("{} (dc {dc}, unreadable record)", kv.key)),
                }
            }
        }
        Ok(missing)
    }

    /// `Ok` if every live gateway knows `engine`.
    ///
    /// # Errors
    /// 39368 `ER_SPRIG_AUTHORIZER_UNKNOWN_TO_GATEWAYS` naming the gateways
    /// that don't; the store's errors.
    pub async fn check_engine(&self, engine: &str) -> Result<()> {
        let missing = self.gateways_without(engine).await?;
        if missing.is_empty() {
            return Ok(());
        }
        Err(unknown_to_gateways(engine, &missing))
    }
}

/// 39368 `ER_SPRIG_AUTHORIZER_UNKNOWN_TO_GATEWAYS`.
#[must_use]
pub fn unknown_to_gateways(engine: &str, gateways: &[String]) -> SprigError {
    SprigError::new(
        SprigCode::AuthorizerUnknownToGateways,
        format!(
            "ER_SPRIG_AUTHORIZER_UNKNOWN_TO_GATEWAYS: auth.authorizer '{engine}' isn't known to \
             {} live gateway(s): {}; upgrade them first",
            gateways.len(),
            gateways.join(", ")
        ),
    )
}

impl ValueGuard for EngineGuard {
    fn check<'a>(&'a self, key: &'a str, value: &'a Value) -> GuardFuture<'a> {
        Box::pin(async move {
            if key != AUTHORIZER.key {
                return Ok(());
            }
            // A name this build doesn't know either is left to the
            // key's bounds (39080).
            let Value::Str(engine) = value else {
                return Ok(());
            };
            if !KNOWN.contains(&engine.as_str()) {
                return Ok(());
            }
            self.check_engine(engine).await
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_meta::{MemStore, Op, Txn};

    use super::*;

    async fn put(store: &dyn MetaStore, key: &str, json: serde_json::Value) {
        let key = store.tier().key(key).unwrap();
        store
            .txn(Txn {
                when: vec![],
                then: vec![Op::Put {
                    key,
                    value: serde_json::to_vec(&json).unwrap(),
                    lease: None,
                }],
                otherwise: vec![],
            })
            .await
            .unwrap();
    }

    fn stores() -> (Arc<dyn MetaStore>, Arc<dyn MetaStore>) {
        let mem: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Global));
        let dc = TierView::over(&mem, Tier::Dc(DcId::new("mum1").unwrap()));
        (mem, dc)
    }

    #[tokio::test]
    async fn every_live_gateway_must_know_the_engine() {
        let (global, dc) = stores();
        let guard = EngineGuard::new(Arc::clone(&global), Arc::clone(&dc));
        // No gateway: nothing to refuse.
        guard.check_engine("supabase").await.unwrap();
        put(
            dc.as_ref(),
            "gateways/gw-a",
            serde_json::json!({"v": 1, "gateway_id": "gw-a", "slot": 1,
                "authorizers": ["mysql_grants", "supabase"]}),
        )
        .await;
        guard.check_engine("supabase").await.unwrap();
        // A second DC with a gateway from before the field.
        put(
            global.as_ref(),
            "dcs/pune1",
            serde_json::json!({"v": 1, "state": "active"}),
        )
        .await;
        let pune = TierView::over(&dc, Tier::Dc(DcId::new("pune1").unwrap()));
        put(
            pune.as_ref(),
            "gateways/gw-old",
            serde_json::json!({"v": 1, "gateway_id": "gw-old", "slot": 2}),
        )
        .await;
        let e = guard.check_engine("supabase").await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::AuthorizerUnknownToGateways));
        assert!(e.message.contains("gw-old (dc pune1)"), "{e}");
        assert!(!e.message.contains("gw-a"), "{e}");
        // The old build knows mysql_grants, so switching back is allowed.
        guard.check_engine(MYSQL_GRANTS).await.unwrap();
        // Through the settings hook: only auth.authorizer is guarded, and
        // only for engines this build knows (others fail the bounds).
        let v = Value::Str("supabase".into());
        guard.check(AUTHORIZER.key, &v).await.unwrap();
        put(
            pune.as_ref(),
            "gateways/gw-none",
            serde_json::json!({"v": 1, "gateway_id": "gw-none", "slot": 3, "authorizers": []}),
        )
        .await;
        let mysql = Value::Str(MYSQL_GRANTS.into());
        let e = guard.check(AUTHORIZER.key, &mysql).await.unwrap_err();
        assert!(e.message.contains("gw-none (dc pune1)"), "{e}");
        assert!(!e.message.contains("gw-old"), "{e}");
        guard.check("auth.grants_on_drop", &mysql).await.unwrap();
        // A retired DC's records don't count.
        put(
            global.as_ref(),
            "dcs/pune1",
            serde_json::json!({"v": 1, "state": "retired"}),
        )
        .await;
        guard.check_engine("supabase").await.unwrap();
    }
}

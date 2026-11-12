//! Server-side JavaScript for MongoDB commands (chapter 30, "Server-side
//! JavaScript"; decisions S110–S113): the scope [`sprig_doc::serverjs`]
//! runs `$where`, `$function` and `$accumulator` in, on SprigDB's QuickJS
//! (`sprig_logic_js::mongo`).
//!
//! A command gets a scope when its body names server-side JavaScript
//! ([`sprig_doc::serverjs::mentions_js`]), and every `getMore` does (a
//! cursor's `$where` runs as its batches are read), while the tenant's
//! `mongo.server_js` is on. The scope's budgets are the logic layer's:
//! `logic.function_budget_ms` of JavaScript time per command and a heap
//! of `logic.memory_limit_mb`.

use std::sync::Arc;

use bson::{Bson, Document};
use sprig_catalog::Freshness;
use sprig_common::ids::TenantId;
use sprig_doc::engine::SqlRunner as _;
use sprig_doc::serverjs::{JsCall, JsFailure, Runner, Scope};
use sprig_settings::{Apply, HardBounds, Scopes, Setting};

use super::conn::Conn;

/// `mongo.server_js`: MongoDB's `security.javascriptEnabled`.
pub static SERVER_JS: Setting<bool> = Setting {
    key: "mongo.server_js",
    default: true,
    scopes: Scopes::TREE,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: false,
    owner: 30,
    doc: "Run server-side JavaScript ($where, $function, $accumulator) in MongoDB commands",
};
sprig_settings::register!(SERVER_JS);

/// SprigDB's QuickJS, as the document engine's runner.
#[derive(Debug, Clone, Copy)]
pub struct Qjs;

impl Runner for Qjs {
    fn call(&self, c: &JsCall<'_>) -> Result<Bson, JsFailure> {
        sprig_logic_js::mongo::call(&sprig_logic_js::mongo::Call {
            op: c.op,
            src: c.src,
            this: c.this,
            args: c.args,
            as_bool: c.as_bool,
            deadline: c.deadline,
            memory_bytes: c.memory_bytes,
        })
        .map_err(|f| match f {
            sprig_logic_js::mongo::Failure::Interrupted => JsFailure::Interrupted,
            sprig_logic_js::mongo::Failure::Error(c, m) => JsFailure::Error(c, m),
        })
    }
}

/// The scope of command `name`, or `None` (no JavaScript in it, or
/// `mongo.server_js` off).
pub(crate) async fn scope_for(c: &Conn, name: &str, body: &Document) -> Option<Arc<Scope>> {
    if name != "getMore" && !sprig_doc::serverjs::mentions_js(body) {
        return None;
    }
    let mut limits = sprig_logic_js::Limits::default();
    if let Some(svc) = &c.shared.d.tenants {
        let tenant = c.sql.as_ref().map(|s| s.tenant()).filter(|t| *t != 0)?;
        let cat = svc
            .cache
            .get(TenantId(tenant), Freshness::Cached { min_version: 0 })
            .await
            .ok()?;
        let ctx = cat.setting_ctx(&svc.dc);
        if !svc.settings.get(&SERVER_JS, &ctx, Some(&cat.settings)) {
            return None;
        }
        limits = sprig_logic::settings::limits(&svc.settings, &ctx, Some(&cat.settings)).0;
    }
    Some(Scope::new(
        Arc::new(Qjs),
        limits.wall,
        usize::try_from(limits.memory_bytes).unwrap_or(usize::MAX),
    ))
}

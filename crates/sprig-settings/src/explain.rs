//! Explaining a resolution: every scope of the chain with its row, the
//! bounds in force when the walk reached it, and what happened to its value.
//!
//! [`explain_layered`] walks the same chain as
//! [`resolve_layered`](crate::resolve_layered), one step per scope of the
//! context (scopes the key doesn't accept included, marked), plus the
//! built-in default, so `sprigctl settings get` and the Admin API can show
//! where a value came from and which bounds limited it:
//!
//! ```text
//! auto_index.min_runs for object acme.billing.orders  = 20
//!   object   acme.billing.orders    20       set by alice 2027-04-02  ◄ effective
//!   database acme.billing           (unset)
//!   tenant   acme                   50
//!   cluster  fra1-c7                (n/a: scope not accepted)
//!   dc       fra1                   (unset)
//!   global                          50       bounds: min 10, max 1000
//!   default                         50
//! ```
//!
//! The steps never disagree with resolution: [`Explanation::resolved`] is
//! what [`resolve_layered`](crate::resolve_layered) returns for the same
//! rows, and the tests check the steps against it.

use crate::registry::AnySetting;
use crate::resolve::{Bounds, Entry, Ignored, MemSettings, Resolved, TenantSettings, def, walk};
use crate::scope::{Scope, ScopeKind, SettingCtx};
use crate::value::Value;
use sprig_common::Result;

/// What happened to one scope's row during resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepOutcome {
    /// The key doesn't accept this scope; any row here is not read.
    NotAccepted,
    /// No row at this scope.
    Unset,
    /// A row with bounds only: it limits narrower scopes.
    BoundsOnly,
    /// This scope supplied the effective value.
    Effective,
    /// Its value was used, then a narrower scope's value replaced it (for
    /// the default: some scope set a value).
    Overridden,
    /// Its value was skipped.
    Ignored(Ignored),
}

/// One scope of the chain.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainStep {
    /// The scope kind (ancestors carry their tenant id).
    pub kind: ScopeKind,
    /// The row's address; `None` for the built-in default.
    pub scope: Option<Scope>,
    /// Whether the key accepts this scope.
    pub accepted: bool,
    /// The row, if one is stored (also for a scope not accepted).
    pub entry: Option<Entry>,
    /// Hard bounds and every wider scope's bounds, as the walk reached this
    /// scope: what this scope's value had to fit.
    pub bounds_in_force: Bounds,
    /// What happened to the value.
    pub outcome: StepOutcome,
    /// The value after clamping, when this scope's value was used and
    /// clamping changed it.
    pub clamped_to: Option<Value>,
}

/// A resolution with its chain.
#[derive(Debug, Clone, PartialEq)]
pub struct Explanation {
    /// The key.
    pub key: &'static str,
    /// The result, as [`resolve_layered`](crate::resolve_layered) gives it.
    pub resolved: Resolved,
    /// The built-in default first, then every scope of the context from the
    /// widest to the narrowest (the order resolution walks).
    pub steps: Vec<ExplainStep>,
}

/// Explains `key` in `ctx` over the same two layers as
/// [`resolve_layered`](crate::resolve_layered): global, dc and cluster rows
/// from `etcd`, the tenant's rows from `tenant`.
///
/// # Errors
/// 39081 if the key is unknown or internal; `Internal` if `ctx` names
/// another tenant than `tenant`'s.
pub fn explain_layered(
    etcd: &MemSettings<'_>,
    tenant: Option<&TenantSettings>,
    key: &str,
    ctx: &SettingCtx<'_>,
) -> Result<Explanation> {
    let def = def(etcd.registry(), key)?;
    let bound;
    let ctx = match tenant {
        Some(ts) => {
            bound = ts.bind(ctx)?;
            &bound
        }
        None => ctx,
    };
    let entry = |s: &Scope| {
        tenant
            .and_then(|ts| ts.lookup(s, def.key()))
            .or_else(|| etcd.rows().get(&(s.clone(), def.key())))
    };
    let resolved = walk(def, ctx, entry);
    let steps = steps(def, ctx, entry, &resolved);
    Ok(Explanation {
        key: def.key(),
        resolved,
        steps,
    })
}

/// The walk of [`walk`] again, recording each scope.
fn steps<'e>(
    def: &dyn AnySetting,
    ctx: &SettingCtx<'_>,
    entry: impl Fn(&Scope) -> Option<&'e Entry>,
    resolved: &Resolved,
) -> Vec<ExplainStep> {
    let scopes = def.scopes();
    let mut bounds: Bounds = def.hard().into();
    let mut out = vec![ExplainStep {
        kind: ScopeKind::Default,
        scope: None,
        accepted: true,
        entry: None,
        bounds_in_force: bounds.clone(),
        outcome: StepOutcome::Overridden,
        clamped_to: None,
    }];
    // The index of the step whose value is current as the walk goes.
    let mut used: usize = 0;
    for (kind, scope) in ctx.chain() {
        let accepted = scopes.contains(kind);
        let e = entry(&scope).cloned();
        let mut step = ExplainStep {
            kind,
            scope: Some(scope),
            accepted,
            entry: e.clone(),
            bounds_in_force: bounds.clone(),
            outcome: StepOutcome::Unset,
            clamped_to: None,
        };
        let Some(e) = e.filter(|_| accepted) else {
            if !accepted {
                step.outcome = StepOutcome::NotAccepted;
            }
            out.push(step);
            continue;
        };
        if bounds.locked {
            step.outcome = if e.value.is_some() {
                StepOutcome::Ignored(Ignored::Locked)
            } else {
                StepOutcome::BoundsOnly
            };
            out.push(step);
            continue;
        }
        step.outcome = StepOutcome::BoundsOnly;
        if let Some(v) = &e.value {
            match def.normalize(v) {
                None => step.outcome = StepOutcome::Ignored(Ignored::WrongType),
                Some(v) if !bounds.allowed.as_ref().is_none_or(|a| a.contains(&v)) => {
                    step.outcome = StepOutcome::Ignored(Ignored::NotAllowed);
                }
                Some(v) => {
                    let c = bounds.clamp(&v);
                    step.clamped_to = (c != v).then_some(c);
                    step.outcome = StepOutcome::Overridden;
                    used = out.len();
                }
            }
        }
        if let Some(b) = &e.bounds {
            bounds = bounds.intersect(b);
        }
        out.push(step);
    }
    if let Some(s) = out.get_mut(used) {
        s.outcome = StepOutcome::Effective;
        if s.kind == ScopeKind::Default && resolved.clamped {
            s.clamped_to = Some(resolved.value.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests;

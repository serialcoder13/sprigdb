//! Budgets of one fallback-tier invocation (chapter 41, "Budgets").
//!
//! The native KIRun runtime has no interpreter to interrupt, so the
//! engine wraps every function of its repository in a shim
//! ([`crate::kirun`]) that charges each call here:
//!
//! | Budget | Setting | Checked |
//! |---|---|---|
//! | Wall time | `logic.function_budget_ms` | every step, every loop iteration, before every `db` call |
//! | Steps | `logic.kirun_max_steps` | every step (loop bodies included) |
//! | Loop iterations | `logic.max_loop_iterations` | every iteration of each loop |
//! | Memory | `logic.memory_limit_mb` | every step's arguments and events; the context and step outputs every [`MEMORY_SCAN_EVERY`] steps |
//! | Data operations | `logic.max_data_ops` | every `Sprig.Db.*` and `Sprig.Http.*` call |
//! | Rows read | `logic.max_rows_read` | every result |
//!
//! Memory is accounted on the values the code holds (JSON sizes, as a
//! close upper estimate of their heap), not with a counting allocator:
//! invocations run on the gateway's shared executor threads, where a
//! thread-local allocator budget can't tell invocations apart.
//!
//! A budget that runs out inside a loop's generator can't fail there (a
//! generator yields events, not errors): the run is *tripped*, the loop is
//! ended, and the next step, or the end of the run, fails with the error.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::Value;
use sprig_common::{SprigCode, SprigError};

/// How often (in steps) the context and step outputs are measured whole.
pub const MEMORY_SCAN_EVERY: u64 = 256;

/// How often (in steps) a run yields to the executor, so a long run
/// without `db` calls doesn't hold a gateway thread.
pub const YIELD_EVERY: u64 = 64;

/// The limits of one invocation (chapter 41's settings).
#[derive(Debug, Clone)]
pub struct Limits {
    /// `logic.function_budget_ms` (or the trigger's or job's).
    pub wall: Duration,
    /// `logic.kirun_max_steps`.
    pub max_steps: u64,
    /// `logic.max_loop_iterations`: per loop, as the compiled tier's guard.
    pub max_loop_iterations: u64,
    /// `logic.memory_limit_mb`, in bytes.
    pub memory_bytes: u64,
    /// `logic.max_data_ops`.
    pub max_data_ops: u64,
    /// `logic.max_rows_read`.
    pub max_rows_read: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            wall: Duration::from_millis(5000),
            max_steps: 1_000_000,
            max_loop_iterations: 100_000,
            memory_bytes: 64 << 20,
            max_data_ops: 1000,
            max_rows_read: 100_000,
        }
    }
}

/// Which budget ran out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Budget {
    /// Wall time.
    Wall,
    /// Steps.
    Steps,
    /// One loop's iterations.
    Loop,
    /// Memory.
    Memory,
    /// `db` calls.
    DataOps,
    /// Rows read.
    Rows,
    /// The caller went away (`KILL QUERY`, a closed connection).
    Cancelled,
}

/// A running invocation's counters.
#[derive(Debug)]
pub struct RunState {
    limits: Limits,
    start: Instant,
    steps: AtomicU64,
    data_ops: AtomicU64,
    rows: AtomicU64,
    held: AtomicU64,
    peak: AtomicU64,
    cancel: AtomicBool,
    tripped: Mutex<Option<SprigError>>,
}

impl RunState {
    /// A run starting now.
    #[must_use]
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            start: Instant::now(),
            steps: AtomicU64::new(0),
            data_ops: AtomicU64::new(0),
            rows: AtomicU64::new(0),
            held: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            cancel: AtomicBool::new(false),
            tripped: Mutex::new(None),
        }
    }

    /// The limits.
    #[must_use]
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Steps so far.
    #[must_use]
    pub fn steps(&self) -> u64 {
        self.steps.load(Ordering::Relaxed)
    }

    /// `db` calls so far.
    #[must_use]
    pub fn data_ops(&self) -> u64 {
        self.data_ops.load(Ordering::Relaxed)
    }

    /// Rows read so far.
    #[must_use]
    pub fn rows_read(&self) -> u64 {
        self.rows.load(Ordering::Relaxed)
    }

    /// The largest value size seen (bytes, estimated).
    #[must_use]
    pub fn peak_bytes(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    /// Time since the start.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Asks the run to stop at its next step.
    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::Relaxed);
    }

    /// The error of a budget that ran out.
    #[must_use]
    pub fn error(&self, b: Budget) -> SprigError {
        let l = &self.limits;
        let what = match b {
            Budget::Wall => format!(
                "wall time: the call ran past logic.function_budget_ms = {} ms",
                l.wall.as_millis()
            ),
            Budget::Steps => format!(
                "steps: the call ran more than logic.kirun_max_steps = {} steps",
                l.max_steps
            ),
            Budget::Loop => format!(
                "loop: a loop ran more than logic.max_loop_iterations = {} iterations",
                l.max_loop_iterations
            ),
            Budget::Memory => format!(
                "memory: the call's values passed logic.memory_limit_mb = {} MB",
                l.memory_bytes >> 20
            ),
            Budget::DataOps => format!(
                "data operations: the call made more than logic.max_data_ops = {} db calls",
                l.max_data_ops
            ),
            Budget::Rows => format!(
                "rows: the call read more than logic.max_rows_read = {} rows",
                l.max_rows_read
            ),
            Budget::Cancelled => "cancelled: the caller stopped the call".to_owned(),
        };
        metrics_budget(b);
        SprigError::new(
            SprigCode::LogicBudgetExceeded,
            format!("logic budget exceeded, {what}"),
        )
    }

    /// Records a budget that ran out where it can't fail at once (a loop
    /// generator). The first one wins.
    pub fn trip(&self, e: SprigError) {
        if let Ok(mut t) = self.tripped.lock()
            && t.is_none()
        {
            *t = Some(e);
        }
    }

    /// The tripped error, if any.
    #[must_use]
    pub fn tripped(&self) -> Option<SprigError> {
        self.tripped.lock().ok().and_then(|t| t.as_ref().map(copy))
    }

    /// Wall time and cancellation.
    ///
    /// # Errors
    /// 39864.
    pub fn check_time(&self) -> Result<(), SprigError> {
        if self.cancel.load(Ordering::Relaxed) {
            return Err(self.error(Budget::Cancelled));
        }
        if self.start.elapsed() > self.limits.wall {
            return Err(self.error(Budget::Wall));
        }
        Ok(())
    }

    /// Charges one step. Returns whether the run should yield now.
    ///
    /// # Errors
    /// 39864 for a tripped run, the wall time or the step count.
    pub fn step(&self) -> Result<bool, SprigError> {
        if let Some(e) = self.tripped() {
            return Err(e);
        }
        self.check_time()?;
        let n = self.steps.fetch_add(1, Ordering::Relaxed) + 1;
        if n > self.limits.max_steps {
            return Err(self.error(Budget::Steps));
        }
        Ok(n.is_multiple_of(YIELD_EVERY))
    }

    /// Whether this step should measure the whole context.
    #[must_use]
    pub fn scan_due(&self) -> bool {
        self.steps().is_multiple_of(MEMORY_SCAN_EVERY)
    }

    /// Checks one value set (a step's arguments, an event, the context)
    /// against the memory budget.
    ///
    /// # Errors
    /// 39864.
    pub fn hold(&self, bytes: u64) -> Result<(), SprigError> {
        self.held.store(bytes, Ordering::Relaxed);
        self.peak.fetch_max(bytes, Ordering::Relaxed);
        if bytes > self.limits.memory_bytes {
            return Err(self.error(Budget::Memory));
        }
        Ok(())
    }

    /// Charges one `db` or outbound call.
    ///
    /// # Errors
    /// 39864.
    pub fn data_op(&self) -> Result<(), SprigError> {
        if let Some(e) = self.tripped() {
            return Err(e);
        }
        self.check_time()?;
        let n = self.data_ops.fetch_add(1, Ordering::Relaxed) + 1;
        if n > self.limits.max_data_ops {
            return Err(self.error(Budget::DataOps));
        }
        Ok(())
    }

    /// Charges rows read.
    ///
    /// # Errors
    /// 39864.
    pub fn read_rows(&self, n: u64) -> Result<(), SprigError> {
        let total = self.rows.fetch_add(n, Ordering::Relaxed) + n;
        if total > self.limits.max_rows_read {
            return Err(self.error(Budget::Rows));
        }
        Ok(())
    }
}

/// A copy of `e` without its source (`SprigError` isn't `Clone`).
#[must_use]
pub fn copy(e: &SprigError) -> SprigError {
    SprigError {
        kind: e.kind,
        message: e.message.clone(),
        retryable: e.retryable,
        source: None,
    }
}

fn metrics_budget(b: Budget) {
    tracing::debug!(budget = ?b, "logic budget exceeded");
}

/// An estimate of a value's heap size in bytes, stopping once it passes
/// `cap` (so measuring a huge value costs no more than the budget).
#[must_use]
pub fn size_of(v: &Value, cap: u64) -> u64 {
    let mut total = 0u64;
    let mut stack: Vec<&Value> = vec![v];
    while let Some(v) = stack.pop() {
        total += match v {
            Value::Null | Value::Bool(_) => 8,
            Value::Number(_) => 16,
            Value::String(s) => 24 + s.len() as u64,
            Value::Array(a) => {
                stack.extend(a.iter());
                24
            }
            Value::Object(o) => {
                for (k, x) in o {
                    total += 32 + k.len() as u64;
                    stack.push(x);
                }
                48
            }
        };
        if total > cap {
            return total;
        }
    }
    total
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use serde_json::json;

    #[test]
    fn sizes_grow_with_values_and_stop_at_the_cap() {
        let small = size_of(&json!({"a": 1}), u64::MAX);
        let big = size_of(&json!({"a": "x".repeat(1000)}), u64::MAX);
        assert!(big > small + 900);
        let huge = json!(vec!["y".repeat(100); 10_000]);
        assert!(size_of(&huge, 5000) < 6000);
    }

    #[test]
    fn steps_and_trips() {
        let st = RunState::new(Limits {
            max_steps: 3,
            ..Limits::default()
        });
        for _ in 0..3 {
            st.step().unwrap();
        }
        let e = st.step().unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::LogicBudgetExceeded));
        assert!(e.message.contains("kirun_max_steps = 3"), "{}", e.message);
        let st = RunState::new(Limits::default());
        st.trip(st.error(Budget::Loop));
        assert!(st.step().unwrap_err().message.contains("loop"));
    }
}

//! Residency policies (chapter 26, "Interfaces" and "Evaluating a
//! policy"; build step 1): a condition over DC tags. The evaluator lives in
//! `sprig-residency` (R9, r9more), so gateways and workers check sink
//! destinations with the same code without linking the controller role.

pub use sprig_residency::*;

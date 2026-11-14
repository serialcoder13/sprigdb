//! Residency policies: a condition over DC tags. The evaluator lives in
//! `sprig-residency`, so gateways and workers check sink
//! destinations with the same code without linking the controller role.

pub use sprig_residency::*;

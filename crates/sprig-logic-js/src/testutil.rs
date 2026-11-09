//! A scripted `db` for the engines' unit tests.

#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;
use sprig_common::{Result, SprigError};

use crate::bridge::{ColKind, Column, DbBridge, DbOp, Reply, Shape};
use crate::kirun::Host;
use crate::outbound::NoOutbound;

/// Records statements; answers queries with `rows` of `(price, stock)`,
/// DML with 1, `CALL` with `event = output, value = 7`; or fails with
/// `fail`.
#[derive(Default)]
pub(crate) struct Script {
    pub(crate) seen: Mutex<Vec<String>>,
    pub(crate) rows: Vec<Vec<Option<String>>>,
    pub(crate) fail: Option<SprigError>,
}

impl Script {
    pub(crate) fn rows(rows: &[(&str, &str)]) -> Self {
        Self {
            rows: rows
                .iter()
                .map(|(p, s)| vec![Some((*p).to_owned()), Some((*s).to_owned())])
                .collect(),
            ..Self::default()
        }
    }

    pub(crate) fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }
}

#[async_trait]
impl DbBridge for Script {
    async fn exec(&self, op: DbOp) -> Result<Reply> {
        self.seen.lock().unwrap().push(op.sql.clone());
        if let Some(e) = &self.fail {
            return Err(crate::budget::copy(e));
        }
        Ok(match op.shape {
            Shape::Exec => Reply::Affected(1),
            Shape::Call => Reply::Rows {
                columns: vec![
                    Column {
                        name: "event".into(),
                        kind: ColKind::Str,
                    },
                    Column {
                        name: "value".into(),
                        kind: ColKind::Int,
                    },
                ],
                rows: vec![vec![Some("output".into()), Some("7".into())]],
            },
            _ => Reply::Rows {
                columns: vec![
                    Column {
                        name: "price".into(),
                        kind: ColKind::Float,
                    },
                    Column {
                        name: "stock".into(),
                        kind: ColKind::Int,
                    },
                ],
                rows: self.rows.clone(),
            },
        })
    }
}

pub(crate) fn host(db: Arc<dyn DbBridge>) -> Host {
    Host {
        db,
        outbound: Arc::new(NoOutbound),
        default_db: None,
        info: serde_json::json!({"user": "u1", "tenant": 42, "database": "shop", "function": "f", "version": 3}),
    }
}

pub(crate) fn class(r: &Result<(String, Value)>) -> Option<u16> {
    r.as_ref().err().map(|e| match e.kind {
        sprig_common::ErrorKind::Sprig(c) => c.number(),
        sprig_common::ErrorKind::Backend { errno, .. }
        | sprig_common::ErrorKind::Standard { errno, .. } => errno,
    })
}

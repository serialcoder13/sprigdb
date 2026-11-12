//! The diagnostic stages that read the catalog and the collection's table
//! (M2, 12 Nov 2026): `$collStats`, `$indexStats`, `$planCacheStats` and
//! (MongoDB 8.1) `$listClusterCatalog`. Their output has MongoDB's shape;
//! fields with no meaning in SprigDB are zero or absent, as chapter 30
//! lists. `$currentOp` and `$listSessions` read the gateway's sessions and
//! run there.

use bson::{Bson, DateTime, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use super::{Engine, Ns, SqlRunner, mongo_error, text};
use crate::sql::{ident, str_lit};

fn unknown(stage: &str, k: &str) -> MongoError {
    MongoError::new(
        40415,
        format!("BSON field '{stage}.{k}' is an unknown field."),
    )
}

fn obj<'a>(stage: &str, arg: &'a Bson) -> Result<&'a Document, MongoError> {
    arg.as_document().ok_or_else(|| {
        MongoError::new(
            code::TYPE_MISMATCH,
            format!(
                "BSON field '{stage}' is the wrong type '{}', expected type 'object'",
                crate::update::type_name(arg)
            ),
        )
    })
}

fn num(rows: &[Vec<Option<Vec<u8>>>], col: usize) -> i64 {
    rows.first()
        .and_then(|r| r.get(col))
        .map(|c| text(c.as_ref()))
        .and_then(|s| s.parse::<f64>().ok())
        .map_or(0, |f| {
            #[allow(clippy::cast_possible_truncation)]
            {
                f as i64
            }
        })
}

impl Engine {
    /// A diagnostic first stage's documents, or `None` for other stages.
    pub(super) async fn diag_stage(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        pipeline: &[Document],
    ) -> Result<Option<Vec<Document>>, MongoError> {
        let Some((name, arg)) = pipeline.first().and_then(|s| s.iter().next()) else {
            return Ok(None);
        };
        Ok(Some(match name.as_str() {
            "$collStats" => self.coll_stats(sql, ns, obj(name, arg)?).await?,
            "$indexStats" => {
                if let Some(k) = obj(name, arg)?.keys().next() {
                    return Err(unknown(name, k));
                }
                self.index_stats(sql, ns).await?
            }
            "$planCacheStats" => {
                for k in obj(name, arg)?.keys() {
                    if k != "allHosts" {
                        return Err(unknown(name, k));
                    }
                }
                // SprigDB keeps no MongoDB plan cache: MariaDB plans each
                // statement. An empty cache is a valid answer.
                Vec::new()
            }
            "$listClusterCatalog" => self.cluster_catalog(sql, ns, obj(name, arg)?).await?,
            "$currentOp" | "$listSessions" => {
                return Err(MongoError::new(
                    code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                    format!("{name} runs in the gateway (aggregate: 1 on admin)"),
                ));
            }
            _ => return Ok(None),
        }))
    }

    /// The collection's document count, JSON bytes, and MariaDB's data and
    /// index lengths.
    async fn table_sizes(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        table: &str,
    ) -> Result<(i64, i64, i64, i64), MongoError> {
        let rows = sql
            .query(&format!(
                "SELECT COUNT(*), COALESCE(SUM(LENGTH(doc)), 0) FROM {}.{}",
                ident(&ns.db),
                ident(table)
            ))
            .await
            .map_err(|e| mongo_error(&e))?;
        let (count, bytes) = (num(&rows, 0), num(&rows, 1));
        // MariaDB's own sizes: best effort (0 when the catalog view
        // isn't readable through this session).
        let st = sql
            .query(&format!(
                "SELECT DATA_LENGTH, INDEX_LENGTH FROM information_schema.TABLES WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {}",
                str_lit(&ns.db),
                str_lit(table)
            ))
            .await
            .unwrap_or_default();
        Ok((count, bytes, num(&st, 0), num(&st, 1)))
    }

    async fn coll_stats(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        spec: &Document,
    ) -> Result<Vec<Document>, MongoError> {
        for k in spec.keys() {
            if !matches!(
                k.as_str(),
                "latencyStats" | "storageStats" | "count" | "queryExecStats"
            ) {
                return Err(unknown("$collStats", k));
            }
        }
        let meta = self.meta(sql, ns).await?;
        let Some(m) = meta else {
            if spec.contains_key("storageStats") || spec.contains_key("count") {
                return Err(MongoError::new(
                    code::NAMESPACE_NOT_FOUND,
                    format!(
                        "Unable to retrieve storageStats in $collStats stage :: caused by :: Collection [{}.{}] not found.",
                        ns.db, ns.coll
                    ),
                ));
            }
            return Ok(vec![
                doc! {"ns": format!("{}.{}", ns.db, ns.coll), "host": "sprigdb", "localTime": DateTime::now()},
            ]);
        };
        let (count, bytes, data_len, index_len) = self.table_sizes(sql, ns, &m.table).await?;
        let mut out = doc! {
            "ns": format!("{}.{}", ns.db, ns.coll),
            "host": "sprigdb",
            "localTime": DateTime::now(),
        };
        if let Ok(l) = spec.get_document("latencyStats") {
            // Per-collection latencies aren't kept (chapter 12 keeps them
            // per statement fingerprint): zeros, MongoDB's shape.
            let z =
                || doc! {"latency": 0i64, "ops": 0i64, "queryableEncryptionLatencyMicros": 0i64};
            let mut ls = doc! {"reads": z(), "writes": z(), "commands": z(), "transactions": z()};
            if l.get_bool("histograms").unwrap_or(false) {
                for k in ["reads", "writes", "commands", "transactions"] {
                    if let Ok(d) = ls.get_document_mut(k) {
                        d.insert("histogram", Bson::Array(Vec::new()));
                    }
                }
            }
            out.insert("latencyStats", ls);
        }
        if let Ok(s) = spec.get_document("storageStats") {
            let scale = s
                .get("scale")
                .and_then(crate::filter::as_i64)
                .unwrap_or(1)
                .max(1);
            let indexes = self.list_indexes(sql, ns).await?.unwrap_or_default();
            let names: Vec<String> = indexes
                .iter()
                .filter_map(|i| i.get_str("name").ok().map(str::to_owned))
                .collect();
            let per = if names.is_empty() {
                0
            } else {
                index_len / i64::try_from(names.len()).unwrap_or(1)
            };
            let mut sizes = Document::new();
            for n in &names {
                sizes.insert(n.clone(), per / scale);
            }
            let storage = data_len.max(bytes);
            out.insert(
                "storageStats",
                doc! {
                    "size": bytes / scale,
                    "count": count,
                    "avgObjSize": if count > 0 { bytes / count } else { 0 },
                    "numOrphanDocs": 0i64,
                    "storageSize": storage / scale,
                    "freeStorageSize": 0i64,
                    "capped": false,
                    "nindexes": i32::try_from(names.len()).unwrap_or(i32::MAX),
                    "indexDetails": Document::new(),
                    "indexBuilds": Bson::Array(Vec::new()),
                    "totalIndexSize": index_len / scale,
                    "totalSize": (storage + index_len) / scale,
                    "indexSizes": sizes,
                    "scaleFactor": i32::try_from(scale).unwrap_or(1),
                    "sprig": {"engine": "MariaDB (InnoDB)", "table": m.table.clone(), "jsonBytes": bytes, "dataLength": data_len, "indexLength": index_len},
                },
            );
        }
        if spec.contains_key("count") {
            out.insert("count", count);
        }
        if spec.contains_key("queryExecStats") {
            out.insert(
                "queryExecStats",
                doc! {"collectionScans": {"total": 0i64, "nonTailable": 0i64}},
            );
        }
        Ok(vec![out])
    }

    async fn index_stats(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
    ) -> Result<Vec<Document>, MongoError> {
        let Some(list) = self.list_indexes(sql, ns).await? else {
            return Ok(Vec::new());
        };
        // Index use isn't counted per index: `ops` is 0 since the
        // gateway's start (MongoDB counts from mongod's start too).
        let since = *START.get_or_init(DateTime::now);
        Ok(list
            .into_iter()
            .map(|spec| {
                doc! {
                    "name": spec.get("name").cloned().unwrap_or(Bson::Null),
                    "key": spec.get("key").cloned().unwrap_or(Bson::Null),
                    "host": "sprigdb",
                    "accesses": {"ops": 0i64, "since": since},
                    "spec": spec,
                }
            })
            .collect())
    }

    async fn cluster_catalog(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        spec: &Document,
    ) -> Result<Vec<Document>, MongoError> {
        for k in spec.keys() {
            if !matches!(k.as_str(), "shards" | "balancingConfiguration") {
                return Err(unknown("$listClusterCatalog", k));
            }
        }
        if !ns.coll.is_empty() {
            return Err(MongoError::new(
                code::INVALID_NAMESPACE,
                "$listClusterCatalog must be run against the database with {aggregate: 1}",
            ));
        }
        let dbs = if ns.db == "admin" {
            self.list_databases(sql).await?
        } else {
            vec![ns.db.clone()]
        };
        let mut out = Vec::new();
        for db in dbs {
            for c in self.list_collections(sql, &db, &doc! {}, false).await? {
                let name = c.get_str("name").unwrap_or_default().to_owned();
                let mut d = doc! {"db": db.clone(), "ns": format!("{db}.{name}")};
                for k in ["type", "options", "info", "idIndex"] {
                    if let Some(v) = c.get(k) {
                        d.insert(k, v.clone());
                    }
                }
                // SprigDB places tenants, not collections: none is sharded
                // in MongoDB's sense.
                d.insert("sharded", false);
                if spec.get_bool("shards").unwrap_or(false) {
                    d.insert("shards", Bson::Array(Vec::new()));
                }
                if spec.get_bool("balancingConfiguration").unwrap_or(false) {
                    d.insert("balancingEnabled", false);
                }
                out.push(d);
            }
        }
        Ok(out)
    }
}

static START: std::sync::OnceLock<DateTime> = std::sync::OnceLock::new();

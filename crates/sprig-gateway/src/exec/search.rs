//! Full-text search in a tenant session (chapter 39, "DDL", "Translating
//! queries"; R5 round 5), on 3306 and 3307:
//!
//! - **DDL**: `CREATE | ALTER | DROP SEARCH INDEX`, `SHOW SEARCH INDEXES`,
//!   `CREATE | DROP SEARCH ANALYZER`, and MySQL's `CREATE FULLTEXT INDEX`
//!   and `ALTER TABLE ... ADD FULLTEXT` converted to search indexes when
//!   `search.fulltext_ddl = convert` (the default).
//! - **`MATCH (cols) AGAINST ('text' [mode])`**: searched in the table's
//!   covering search index; each call is replaced by SQL over the table's
//!   key (`pk IN (...)` where it is a condition, `CASE pk WHEN ... THEN
//!   score ... END` where it is a value), and the statement runs as any
//!   other, so privileges, row-level security and the router apply to the
//!   rows returned. A table with no search index keeps MariaDB's own
//!   `FULLTEXT` behaviour (decision S7).
//! - **`sprig_search('index', 'query', options)`**: replaced by a derived
//!   table over the indexed object, `SELECT <key>, score, highlights,
//!   _shard FROM <object> WHERE <key> IN (...)`, read with the user's
//!   privileges.

// Rewriting walks many short-lived names (q, t, k, s) and long statement
// arms, as the rest of `exec` does.
#![allow(
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::items_after_statements
)]

use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_auth::authz::{Action, SessionGrants, Target};
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::database_key;
use sprig_common::ids::{DbId, ObjectId};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_search::ddl::SearchDdl;
use sprig_search::def::{FieldDef, FieldKind, Flavor, IndexDef, Model, Source};
use sprig_search::query::{SearchQuery, mysql_natural, parse_mysql_boolean, parse_query_string};
use sprig_search::schema::Key;
use sprig_search::sqlscan::{self, Mode};
use sprig_search::{Hit, highlight};

use super::PassSession;
use super::tenant::Step;
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues, Wire};
use crate::search::{Consistency, SearchService, catalog as scat};

/// Collects a sub-statement's text rows.
#[allow(clippy::struct_field_names)]
struct Rows {
    wire: Wire,
    columns: usize,
    rows: Vec<Vec<Option<Vec<u8>>>>,
    error: Option<SprigError>,
    done: bool,
}

impl Rows {
    fn new(wire: Wire) -> Self {
        Self {
            wire,
            columns: 0,
            rows: Vec::new(),
            error: None,
            done: false,
        }
    }
}

#[async_trait]
impl ResultSink for Rows {
    fn wire(&self) -> Wire {
        self.wire
    }
    async fn begin_rows(&mut self, columns: &[ColumnDef], _format: RowFormat) -> Result<()> {
        self.columns = columns.len();
        Ok(())
    }
    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        if let RowValues::Text(v) = values {
            self.rows
                .push(v.iter().map(|c| c.map(<[u8]>::to_vec)).collect());
        }
        Ok(())
    }
    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        let r = sprig_proto_mysql::resultset::decode_text_row(&payload, self.columns.max(1))?;
        self.rows
            .push(r.into_iter().map(|c| c.map(<[u8]>::to_vec)).collect());
        Ok(())
    }
    async fn end_rows(&mut self, _end: EndOfRows) -> Result<()> {
        self.done = true;
        Ok(())
    }
    async fn ok(&mut self, _ok: OkPacket) -> Result<()> {
        self.done = true;
        Ok(())
    }
    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.error = Some(SprigError {
            kind: err.kind,
            message: err.message.clone(),
            retryable: err.retryable,
            source: None,
        });
        self.done = true;
        Ok(())
    }
    async fn column_list(&mut self, _c: &[ColumnDef], _e: EndOfRows) -> Result<()> {
        Ok(())
    }
    async fn local_infile_request(&mut self, _f: &[u8]) -> Result<()> {
        Err(SprigError::internal("LOAD DATA in a search sub-statement"))
    }
    async fn local_infile_read(&mut self) -> Result<Bytes> {
        Ok(Bytes::new())
    }
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn finished(&self) -> bool {
        self.done
    }
}

fn lit(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('\'');
    for c in s.chars() {
        match c {
            '\'' => o.push_str("''"),
            '\\' => o.push_str("\\\\"),
            c => o.push(c),
        }
    }
    o.push('\'');
    o
}

fn no_db() -> SprigError {
    SprigError::standard(1046, *b"3D000", "No database selected")
}

fn unknown_db(db: &str) -> SprigError {
    SprigError::standard(1049, *b"42000", format!("Unknown database '{db}'"))
}

fn not_found(name: &str) -> SprigError {
    SprigError::new(
        SprigCode::SearchIndexNotFound,
        format!("search index '{name}' doesn't exist"),
    )
}

fn mentions_ddl(text: &[u8]) -> bool {
    let has = |w: &[u8]| text.windows(w.len()).any(|x| x.eq_ignore_ascii_case(w));
    text.len() < 65_536 && (has(b"search") || has(b"fulltext"))
}

/// A score as a DOUBLE literal.
fn score_lit(s: f32) -> String {
    format!("{:e}", f64::from(s))
}

/// What a resolved index and its object need for a rewrite.
struct Target2 {
    def: Arc<IndexDef>,
    /// `qualifier.` for the key columns (empty when unqualified).
    qual: String,
}

impl Target2 {
    fn key_expr(&self) -> String {
        let cols: Vec<String> = self
            .def
            .key_columns
            .iter()
            .map(|c| format!("{}{}", self.qual, quote(c)))
            .collect();
        if cols.len() == 1 {
            cols[0].clone()
        } else {
            format!("({})", cols.join(", "))
        }
    }

    fn in_list(&self, hits: &[Hit]) -> Result<String> {
        if hits.is_empty() {
            return Ok("(0 = 1)".to_owned());
        }
        let mut v = Vec::with_capacity(hits.len());
        for h in hits {
            v.push(h.key.sql(self.def.model)?);
        }
        Ok(format!("({} IN ({}))", self.key_expr(), v.join(", ")))
    }

    fn score_case(&self, hits: &[Hit]) -> Result<String> {
        if hits.is_empty() {
            return Ok("0e0".to_owned());
        }
        let k = self.key_expr();
        let mut s = String::from("(CASE");
        for h in hits {
            let _ = write!(
                s,
                " WHEN {k} = {} THEN {}",
                h.key.sql(self.def.model)?,
                score_lit(h.score)
            );
        }
        s.push_str(" ELSE 0e0 END)");
        Ok(s)
    }
}

impl PassSession {
    #[allow(clippy::unused_self)] // a session method, like its neighbours
    fn search_service(&self) -> Result<&'static Arc<SearchService>> {
        crate::search::service().ok_or_else(|| {
            SprigError::new(
                SprigCode::SearchUnavailable,
                "full-text search isn't running in this gateway",
            )
        })
    }

    fn grants(&self, cat: &TenantCatalog) -> Option<Arc<dyn SessionGrants>> {
        let ts = self.tenant.as_deref()?;
        ts.principal
            .user_id
            .map(|u| crate::tenant::authz::grants_of(ts.services(), cat, Some(u)))
    }

    fn allowed(
        &self,
        cat: &TenantCatalog,
        lctn: bool,
        db: &str,
        db_id: DbId,
        object: Option<u64>,
        a: Action,
    ) -> bool {
        let Some(g) = self.grants(cat) else {
            return true;
        };
        if g.unrestricted() || g.allows(a, Target::Tenant) {
            return true;
        }
        let db_key = database_key(db, lctn);
        if g.allows(a, Target::Database { db_key: &db_key }) {
            return true;
        }
        object.is_some_and(|o| {
            g.allows(
                a,
                Target::Object {
                    db_key: &db_key,
                    db: db_id,
                    object: ObjectId(o),
                },
            )
        })
    }

    fn denied(&self, a: &str, db: &str, object: &str) -> SprigError {
        let user = self
            .tenant
            .as_deref()
            .map(|t| t.principal.user.clone())
            .unwrap_or_default();
        SprigError::standard(
            1142,
            *b"42000",
            format!("{a} command denied to user '{user}' for table '{db}.{object}'"),
        )
    }

    /// Runs `sql` through the tenant path, collecting its rows.
    async fn sub_rows(
        &mut self,
        cx: &mut ExecCtx<'_>,
        wire: Wire,
        sql: &str,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>> {
        let mut sink = Rows::new(wire);
        let text = Bytes::from(sql.to_owned());
        Box::pin(self.tenant_text(
            cx,
            &text,
            &QueryAttrs::default(),
            false,
            RowFormat::Text,
            &mut sink,
        ))
        .await?;
        match sink.error {
            Some(e) => Err(e),
            None => Ok(sink.rows),
        }
    }

    /// Search DDL: `None` when the statement isn't one.
    pub(super) async fn tenant_search_ddl(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if !mentions_ddl(text) {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        // `FULLTEXT` keys inside `CREATE TABLE` (R5 round 6): the table
        // without them, then each as a converted search index.
        match sprig_search::ddl::create_table_fulltext(sql) {
            Ok(None) => {}
            Ok(Some(c)) => {
                let Ok(svc) = self.search_service() else {
                    return Ok(None);
                };
                match svc.knobs().fulltext_ddl.as_str() {
                    "mariadb" => return Ok(None),
                    "reject" => {
                        let e = SprigError::new(
                            SprigCode::SearchUnsupported,
                            "FULLTEXT indexes are refused here (search.fulltext_ddl = reject); use CREATE SEARCH INDEX",
                        );
                        return Ok(Some(self.fail(out, &e).await?));
                    }
                    _ => {}
                }
                let wire = out.wire();
                if let Err(e) = self.sub_rows(cx, wire, &c.stripped).await {
                    return Ok(Some(self.fail(out, &e).await?));
                }
                for (name, columns) in c.keys {
                    let ddl = SearchDdl::Fulltext {
                        name,
                        db: c.db.clone(),
                        table: c.table.clone(),
                        columns,
                    };
                    if let Err(e) = self.search_ddl(cx, wire, ddl).await {
                        return Ok(Some(self.fail(out, &e).await?));
                    }
                }
                let mut status = self.status();
                if more {
                    status |= StatusFlags::MORE_RESULTS_EXISTS;
                }
                out.ok(OkPacket::new(status)).await?;
                return Ok(Some(Step {
                    errored: false,
                    status: Some(status),
                }));
            }
            Err(e) => return Ok(Some(self.fail(out, &e).await?)),
        }
        let ddl = match sprig_search::ddl::parse(sql) {
            Ok(None) => return Ok(None),
            Ok(Some(d)) => d,
            Err(e) => return Ok(Some(self.fail(out, &e).await?)),
        };
        // MySQL FULLTEXT DDL: kept for MariaDB unless converted.
        if let SearchDdl::Fulltext { .. } = &ddl {
            let Ok(svc) = self.search_service() else {
                return Ok(None);
            };
            match svc.knobs().fulltext_ddl.as_str() {
                "mariadb" => return Ok(None),
                "reject" => {
                    let e = SprigError::new(
                        SprigCode::SearchUnsupported,
                        "FULLTEXT indexes are refused here (search.fulltext_ddl = reject); use CREATE SEARCH INDEX",
                    );
                    return Ok(Some(self.fail(out, &e).await?));
                }
                _ => {}
            }
        }
        if let SearchDdl::Show { db, object } = &ddl {
            let r = self.show_search_sql(db.clone(), object.clone()).await;
            return match r {
                Ok(q) => {
                    let q = Bytes::from(q);
                    Box::pin(self.tenant_text(cx, &q, attrs, more, format, out))
                        .await
                        .map(Some)
                }
                Err(e) => Ok(Some(self.fail(out, &e).await?)),
            };
        }
        let wire = out.wire();
        let r = self.search_ddl(cx, wire, ddl).await;
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        if let Err(e) = r {
            return Ok(Some(self.fail(out, &e).await?));
        }
        out.ok(OkPacket::new(status)).await?;
        Ok(Some(Step {
            errored: false,
            status: Some(status),
        }))
    }

    async fn snapshot(&mut self) -> Result<Arc<TenantCatalog>> {
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Err(SprigError::internal("no tenant"));
        };
        ts.refresh().await
    }

    #[allow(clippy::too_many_lines)] // one arm per statement
    async fn search_ddl(&mut self, cx: &mut ExecCtx<'_>, wire: Wire, ddl: SearchDdl) -> Result<()> {
        let svc = self.search_service()?;
        let current = self.current_logical();
        let (tenant, tenant_name, user_id) = {
            let ts = self
                .tenant
                .as_deref()
                .ok_or_else(|| SprigError::internal("no tenant"))?;
            (
                ts.tenant,
                ts.principal.tenant_name.clone().unwrap_or_default(),
                ts.principal.user_id.map_or(0, |u| u.0),
            )
        };
        let cat = self.snapshot().await?;
        let lctn = svc.catalog().lctn(&cat);
        let pick = |db: Option<String>| db.or_else(|| current.clone()).ok_or_else(no_db);
        match ddl {
            SearchDdl::Create { if_not_exists, def } => {
                let def = self
                    .prepare_index(cx, wire, &cat, lctn, def, Flavor::Sql, tenant_name, user_id)
                    .await?;
                self.create_index(svc, &cat, def, if_not_exists, "user")
                    .await
            }
            SearchDdl::Fulltext {
                name,
                db,
                table,
                columns,
            } => {
                let db = pick(db)?;
                let name = name.unwrap_or_else(|| format!("ft_{}", columns.join("_")));
                let mut def =
                    IndexDef::new(&name, &db, &table, Model::Table, Flavor::FulltextCompat);
                for c in &columns {
                    def.push(FieldDef::new(c, Source::Column, FieldKind::Text))?;
                }
                let def = self
                    .prepare_index(
                        cx,
                        wire,
                        &cat,
                        lctn,
                        def,
                        Flavor::FulltextCompat,
                        tenant_name,
                        user_id,
                    )
                    .await?;
                self.create_index(svc, &cat, def, false, "migrated").await
            }
            SearchDdl::AddField { name, db, field } => {
                let db = pick(db)?;
                let old = self.index_by_name(&cat, &db, &name)?;
                self.check_alter(&cat, lctn, &db, &old)?;
                let mut def = (*old).clone();
                def.generation += 1;
                let mut f = field;
                f.added_generation = def.generation;
                if def.model != Model::Table && f.source == Source::Column {
                    f.source = Source::Path;
                }
                def.push(f)?;
                let st = scat::state(&cat);
                def.check(&st.analyzers)?;
                let db_id = svc
                    .catalog()
                    .db_id(&cat, &db)
                    .ok_or_else(|| unknown_db(&db))?;
                let v = svc.catalog().replace(tenant, db_id, &def).await?;
                self.after_change(svc, v, Some(def)).await
            }
            SearchDdl::DropField { name, db, field } => {
                let db = pick(db)?;
                let old = self.index_by_name(&cat, &db, &name)?;
                self.check_alter(&cat, lctn, &db, &old)?;
                let mut def = (*old).clone();
                let before = def.fields.len();
                def.fields.retain(|f| !f.name.eq_ignore_ascii_case(&field));
                if def.fields.len() == before {
                    return Err(SprigError::new(
                        SprigCode::SearchFieldType,
                        format!("search index '{name}' has no field '{field}'"),
                    ));
                }
                def.generation += 1;
                def.check(&scat::state(&cat).analyzers)?;
                let db_id = svc
                    .catalog()
                    .db_id(&cat, &db)
                    .ok_or_else(|| unknown_db(&db))?;
                let v = svc.catalog().replace(tenant, db_id, &def).await?;
                self.after_change(svc, v, Some(def)).await
            }
            SearchDdl::Rebuild { name, db } => {
                let db = pick(db)?;
                let old = self.index_by_name(&cat, &db, &name)?;
                self.check_alter(&cat, lctn, &db, &old)?;
                let mut def = (*old).clone();
                def.generation += 1;
                let db_id = svc
                    .catalog()
                    .db_id(&cat, &db)
                    .ok_or_else(|| unknown_db(&db))?;
                let v = svc.catalog().replace(tenant, db_id, &def).await?;
                svc.forget(tenant, def.index_id);
                self.after_change(svc, v, Some(def)).await
            }
            SearchDdl::Drop {
                if_exists,
                name,
                db,
            } => {
                let db = pick(db)?;
                let old = match self.index_by_name(&cat, &db, &name) {
                    Ok(d) => d,
                    Err(_) if if_exists => return Ok(()),
                    Err(e) => return Err(e),
                };
                let db_id = svc
                    .catalog()
                    .db_id(&cat, &db)
                    .ok_or_else(|| unknown_db(&db))?;
                if !self.allowed(&cat, lctn, &db, db_id, Some(old.object_id), Action::Index) {
                    return Err(self.denied("INDEX", &db, &old.object));
                }
                let v = svc.catalog().drop_index(tenant, old.index_id).await?;
                svc.forget(tenant, old.index_id);
                self.after_change(svc, v, None).await
            }
            SearchDdl::CreateAnalyzer {
                if_not_exists,
                analyzer,
            } => {
                if !self.allowed(&cat, lctn, "", DbId(0), None, Action::Create) {
                    return Err(SprigError::standard(
                        1227,
                        *b"42000",
                        "Access denied; you need the CREATE privilege for CREATE SEARCH ANALYZER",
                    ));
                }
                if let Some(v) = svc
                    .catalog()
                    .analyzer(tenant, Some(&analyzer), None, if_not_exists)
                    .await?
                {
                    self.after_change(svc, v, None).await?;
                }
                Ok(())
            }
            SearchDdl::DropAnalyzer { if_exists, name } => {
                if !self.allowed(&cat, lctn, "", DbId(0), None, Action::Drop) {
                    return Err(SprigError::standard(
                        1227,
                        *b"42000",
                        "Access denied; you need the DROP privilege for DROP SEARCH ANALYZER",
                    ));
                }
                if let Some(v) = svc
                    .catalog()
                    .analyzer(tenant, None, Some(&name), if_exists)
                    .await?
                {
                    self.after_change(svc, v, None).await?;
                }
                Ok(())
            }
            SearchDdl::Show { .. } => Ok(()),
        }
    }

    fn check_alter(&self, cat: &TenantCatalog, lctn: bool, db: &str, d: &IndexDef) -> Result<()> {
        let svc = self.search_service()?;
        let db_id = svc.catalog().db_id(cat, db).ok_or_else(|| unknown_db(db))?;
        if self.allowed(cat, lctn, db, db_id, Some(d.object_id), Action::Index) {
            Ok(())
        } else {
            Err(self.denied("INDEX", db, &d.object))
        }
    }

    fn index_by_name(&self, cat: &TenantCatalog, db: &str, name: &str) -> Result<Arc<IndexDef>> {
        let svc = self.search_service()?;
        let db_id = svc.catalog().db_id(cat, db).ok_or_else(|| unknown_db(db))?;
        scat::state(cat)
            .by_name(db_id.0, name)
            .ok_or_else(|| not_found(name))
    }

    /// Fills in an index's object: model, backing table, key columns and
    /// their types, the definer; checks columns, kinds and privileges.
    #[allow(clippy::too_many_arguments)]
    async fn prepare_index(
        &mut self,
        cx: &mut ExecCtx<'_>,
        wire: Wire,
        cat: &TenantCatalog,
        lctn: bool,
        mut def: IndexDef,
        flavor: Flavor,
        tenant_name: String,
        user_id: u32,
    ) -> Result<IndexDef> {
        let svc = self.search_service()?;
        if def.db.is_empty() {
            def.db = self.current_logical().ok_or_else(no_db)?;
        }
        let db = def.db.clone();
        let db_id = svc
            .catalog()
            .db_id(cat, &db)
            .ok_or_else(|| unknown_db(&db))?;
        def.flavor = flavor;
        def.tenant_name = tenant_name;
        def.definer_user_id = user_id;
        // A collection (chapter 31) or a table.
        let coll = cat
            .ext
            .get::<crate::mongo::catalog::DocState>(crate::mongo::catalog::MODULE)
            .and_then(|s| s.get(db_id.0, &def.object).cloned());
        if let Some(m) = coll {
            def.model = Model::Collection;
            def.table.clone_from(&m.table);
            def.key_columns = vec!["_id".to_owned()];
            for f in &mut def.fields {
                if f.source == Source::Column {
                    f.source = Source::Path;
                }
            }
        } else if def.model == Model::Kv {
            // A key-value namespace (chapter 33; R5 round 6): its keys
            // table, `k` the key and `v` a string key's value.
            let q = format!(
                "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = {} \
                 AND TABLE_NAME = {} AND COLUMN_NAME IN ('k', 'type', 'v', 'expires_at')",
                lit(&db),
                lit(&def.object)
            );
            let n = self.sub_rows(cx, wire, &q).await?;
            let found = n
                .first()
                .and_then(|r| r.first().cloned().flatten())
                .is_some_and(|b| b == b"4");
            if !found {
                return Err(SprigError::new(
                    SprigCode::SearchFieldType,
                    format!("'{}' isn't a key-value namespace: VALUE fields need one", def.object),
                ));
            }
            def.table.clone_from(&def.object);
            def.key_columns = vec!["k".to_owned()];
            def.key_types = vec!["varbinary".to_owned()];
        } else {
            def.model = Model::Table;
            def.table.clone_from(&def.object);
            if def.fields.iter().any(|f| f.source != Source::Column) {
                return Err(SprigError::new(
                    SprigCode::SearchFieldType,
                    format!("'{}' is a table: fields are columns, not paths", def.object),
                ));
            }
            let q = format!(
                "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
                lit(&db),
                lit(&def.object)
            );
            let cols = self.sub_rows(cx, wire, &q).await?;
            if cols.is_empty() {
                return Err(SprigError::standard(
                    1146,
                    *b"42S02",
                    format!("Table '{db}.{}' doesn't exist", def.object),
                ));
            }
            let text = |b: &Option<Vec<u8>>| {
                String::from_utf8_lossy(b.as_deref().unwrap_or_default()).into_owned()
            };
            let pk_q = format!(
                "SELECT COLUMN_NAME FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} AND INDEX_NAME = 'PRIMARY' ORDER BY SEQ_IN_INDEX",
                lit(&db),
                lit(&def.object)
            );
            let pk: Vec<String> = self
                .sub_rows(cx, wire, &pk_q)
                .await?
                .iter()
                .map(|r| text(&r[0]))
                .collect();
            if pk.is_empty() {
                return Err(SprigError::new(
                    SprigCode::SearchUnsupported,
                    format!(
                        "'{}' has no primary key: a search index needs one",
                        def.object
                    ),
                ));
            }
            let ty = |c: &str| {
                cols.iter()
                    .find(|r| text(&r[0]).eq_ignore_ascii_case(c))
                    .map(|r| text(&r[1]))
            };
            def.key_types = pk.iter().map(|c| ty(c).unwrap_or_default()).collect();
            def.key_columns = pk;
            for f in &mut def.fields {
                let t = ty(&f.source_ref).ok_or_else(|| {
                    SprigError::standard(
                        1072,
                        *b"42000",
                        format!("Key column '{}' doesn't exist in table", f.source_ref),
                    )
                })?;
                sprig_search::def::check_column_kind(&f.source_ref, &t, f.kind)?;
            }
        }
        def.object_id = if def.model == Model::Collection {
            svc.catalog().table_id(cat, db_id, &def.table).unwrap_or(0)
        } else {
            svc.catalog().table_id(cat, db_id, &def.object).unwrap_or(0)
        };
        if !self.allowed(cat, lctn, &db, db_id, Some(def.object_id), Action::Index) {
            return Err(self.denied("INDEX", &db, &def.object));
        }
        def.check(&scat::state(cat).analyzers)?;
        Ok(def)
    }

    async fn create_index(
        &mut self,
        svc: &Arc<SearchService>,
        cat: &TenantCatalog,
        mut def: IndexDef,
        if_not_exists: bool,
        origin: &str,
    ) -> Result<()> {
        let knobs = svc.knobs();
        let tenant = self
            .tenant
            .as_deref()
            .map(|t| t.tenant)
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let db_id = svc
            .catalog()
            .db_id(cat, &def.db)
            .ok_or_else(|| unknown_db(&def.db))?;
        let Some(v) = svc
            .catalog()
            .create(
                tenant,
                db_id,
                &mut def,
                if_not_exists,
                knobs.max_per_object,
                origin,
            )
            .await?
        else {
            return Ok(());
        };
        self.after_change(svc, v, Some(def)).await
    }

    /// After a catalog change: the session sees it, and a new or changed
    /// index's copy starts here (and is waited for, within
    /// `search.wait_timeout_ms` × 10).
    async fn after_change(
        &mut self,
        svc: &Arc<SearchService>,
        v: u64,
        def: Option<IndexDef>,
    ) -> Result<()> {
        let tenant = self
            .tenant
            .as_deref()
            .map(|t| t.tenant)
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let cat = svc.catalog().snapshot(tenant, v).await?;
        if let Some(ts) = self.tenant.as_deref_mut() {
            ts.last_change = ts.last_change.max(v);
        }
        if let Some(d) = def {
            let db_id = svc
                .catalog()
                .db_id(&cat, &d.db)
                .ok_or_else(|| unknown_db(&d.db))?;
            let Some(d) = scat::state(&cat).by_name(db_id.0, &d.name) else {
                return Ok(());
            };
            let wait = svc.knobs().wait_timeout * 10;
            svc.prepare(tenant, &d, &scat::state(&cat).analyzers, wait)
                .await?;
        }
        Ok(())
    }

    /// `SHOW SEARCH INDEXES` as a `SELECT` of literal rows.
    async fn show_search_sql(
        &mut self,
        db: Option<String>,
        object: Option<String>,
    ) -> Result<String> {
        let svc = self.search_service()?;
        let cat = self.snapshot().await?;
        let lctn = svc.catalog().lctn(&cat);
        let db = db.or_else(|| self.current_logical()).ok_or_else(no_db)?;
        let db_id = svc
            .catalog()
            .db_id(&cat, &db)
            .ok_or_else(|| unknown_db(&db))?;
        let st = scat::state(&cat);
        let tenant = self.tenant.as_deref().map(|t| t.tenant);
        let mut rows = Vec::new();
        for d in st.of_db(db_id.0) {
            if object
                .as_deref()
                .is_some_and(|o| !d.object.eq_ignore_ascii_case(o))
            {
                continue;
            }
            if !self.allowed(&cat, lctn, &db, db_id, Some(d.object_id), Action::Select) {
                continue;
            }
            let state = match tenant {
                Some(t) => svc.state(t, d).await.1,
                None => "unknown".to_owned(),
            };
            for f in &d.fields {
                rows.push(format!(
                    "SELECT {} AS `Index`, {} AS `Object`, {} AS `Model`, {} AS `Flavor`, {} AS `Field`, \
                     {} AS `Kind`, {} AS `Analyzer`, {:e} AS `Boost`, {} AS `Filter`, {} AS `Sort`, \
                     {} AS `Facet`, {} AS `Generation`, {} AS `State`",
                    lit(&d.name),
                    lit(&d.object),
                    lit(d.model.as_str()),
                    lit(d.flavor.as_str()),
                    lit(&f.name),
                    lit(f.kind.as_str()),
                    lit(f.index_analyzer(&d.default_analyzer)),
                    f.boost,
                    u8::from(f.filter),
                    u8::from(f.sort),
                    u8::from(f.facet),
                    d.generation,
                    lit(&state)
                ));
            }
        }
        if rows.is_empty() {
            return Ok("SELECT '' AS `Index`, '' AS `Object`, '' AS `Model`, '' AS `Flavor`, '' AS `Field`, \
                       '' AS `Kind`, '' AS `Analyzer`, 0e0 AS `Boost`, 0 AS `Filter`, 0 AS `Sort`, 0 AS `Facet`, \
                       0 AS `Generation`, '' AS `State` FROM DUAL WHERE 0 = 1"
                .to_owned());
        }
        Ok(rows.join(" UNION ALL "))
    }

    /// `MATCH ... AGAINST` and `sprig_search()`: runs each search and the
    /// statement with them replaced; `None` when the text has none (or
    /// only `MATCH` calls on tables without search indexes).
    pub(super) async fn tenant_search_rewrite(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if !sqlscan::mentions(text) || crate::search::service().is_none() {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        let scan = match sqlscan::scan(sql) {
            Ok(s) if s.matches.is_empty() && s.searches.is_empty() => return Ok(None),
            Ok(s) => s,
            Err(e) => return Ok(Some(self.fail(out, &e).await?)),
        };
        let wire = out.wire();
        let rewritten = match self.rewrite_search(cx, wire, sql, &scan).await {
            Ok(None) => return Ok(None),
            Ok(Some(s)) => s,
            Err(e) => return Ok(Some(self.fail(out, &e).await?)),
        };
        let rewritten = Bytes::from(rewritten);
        Box::pin(self.tenant_text(cx, &rewritten, attrs, more, format, out))
            .await
            .map(Some)
    }

    /// `COM_STMT_PREPARE` of a text whose `MATCH ... AGAINST` runs on a
    /// search index (R5 round 6): the text to prepare on the backend for its
    /// metadata, with each such call replaced by an expression of the same
    /// placeholders (`(CONCAT(?) IS NOT NULL)` as a condition, `(0e0 *
    /// (CONCAT(?) IS NULL))` as a value); `None` when no `MATCH` is on a
    /// search index (MariaDB's `FULLTEXT` takes it).
    pub(super) async fn search_prepare_text(&mut self, sql: &[u8]) -> Result<Option<String>> {
        if !sqlscan::mentions(sql) || crate::search::service().is_none() {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(sql) else {
            return Ok(None);
        };
        let scan = sqlscan::scan(sql)?;
        if scan.matches.is_empty() {
            return Ok(None);
        }
        let svc = self.search_service()?;
        let cat = self.snapshot().await?;
        let lctn = svc.catalog().lctn(&cat);
        let current = self.current_logical();
        let st = scat::state(&cat);
        let mut edits: Vec<((usize, usize), String)> = Vec::new();
        for m in &scan.matches {
            let qual = m.columns.iter().find_map(|(q, _)| q.clone());
            let cols: Vec<String> = m.columns.iter().map(|(_, c)| c.clone()).collect();
            let indexed = scan.tables.iter().any(|t| {
                let named = match &qual {
                    Some(q) => {
                        t.alias.as_deref().is_some_and(|a| a.eq_ignore_ascii_case(q))
                            || (t.alias.is_none() && t.table.eq_ignore_ascii_case(q))
                    }
                    None => true,
                };
                named
                    && t.db
                        .clone()
                        .or_else(|| current.clone())
                        .and_then(|db| svc.catalog().db_id(&cat, &db))
                        .is_some_and(|db_id| {
                            st.on_object(db_id.0, &t.table, lctn)
                                .iter()
                                .any(|d| d.covers(&cols).is_some())
                        })
            });
            if !indexed {
                continue;
            }
            let params = sqlscan::placeholders(&sql[m.span.0..m.span.1]).len();
            let rep = if m.condition {
                if params == 0 {
                    "(1 = 1)".to_owned()
                } else {
                    format!("(CONCAT({}) IS NOT NULL)", vec!["?"; params].join(", "))
                }
            } else if params == 0 {
                "0e0".to_owned()
            } else {
                format!("(0e0 * (CONCAT({}) IS NULL))", vec!["?"; params].join(", "))
            };
            edits.push((m.span, rep));
        }
        if edits.is_empty() {
            return Ok(None);
        }
        let mut out = String::with_capacity(sql.len());
        let mut last = 0;
        for ((a, b), rep) in edits {
            out.push_str(&sql[last..a]);
            out.push_str(&rep);
            last = b;
        }
        out.push_str(&sql[last..]);
        Ok(Some(out))
    }

    #[allow(clippy::too_many_lines)]
    async fn rewrite_search(
        &mut self,
        cx: &mut ExecCtx<'_>,
        wire: Wire,
        sql: &str,
        scan: &sqlscan::Scan,
    ) -> Result<Option<String>> {
        let svc = self.search_service()?;
        let knobs = svc.knobs();
        let cat = self.snapshot().await?;
        let lctn = svc.catalog().lctn(&cat);
        let current = self.current_logical();
        let tenant = self
            .tenant
            .as_deref()
            .map(|t| t.tenant)
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let st = scat::state(&cat);
        // (span, replacement)
        let mut edits: Vec<((usize, usize), String)> = Vec::new();
        for m in &scan.matches {
            // Candidate tables: by qualifier, else every table named.
            let qual = m.columns.iter().find_map(|(q, _)| q.clone());
            let cands: Vec<&sqlscan::TableRef> = scan
                .tables
                .iter()
                .filter(|t| match &qual {
                    Some(q) => {
                        t.alias
                            .as_deref()
                            .is_some_and(|a| a.eq_ignore_ascii_case(q))
                            || (t.alias.is_none() && t.table.eq_ignore_ascii_case(q))
                    }
                    None => true,
                })
                .collect();
            let cols: Vec<String> = m.columns.iter().map(|(_, c)| c.clone()).collect();
            let mut any_index = false;
            let mut chosen: Option<(Arc<IndexDef>, &sqlscan::TableRef)> = None;
            for t in &cands {
                let Some(db) = t.db.clone().or_else(|| current.clone()) else {
                    continue;
                };
                let Some(db_id) = svc.catalog().db_id(&cat, &db) else {
                    continue;
                };
                let defs = st.on_object(db_id.0, &t.table, lctn);
                any_index |= !defs.is_empty();
                let best = defs
                    .into_iter()
                    .filter_map(|d| d.covers(&cols).map(|x| (x, d)))
                    .min_by_key(|(x, _)| *x);
                if let Some((_, d)) = best {
                    chosen = Some((d, t));
                    break;
                }
            }
            let Some((def, tref)) = chosen else {
                if !any_index {
                    // MariaDB's FULLTEXT (decision S7).
                    return Ok(None);
                }
                return Err(SprigError::new(
                    SprigCode::SearchNoIndexForMatch,
                    format!("no search index covers MATCH ({})", cols.join(", ")),
                ));
            };
            let Some(text) = m.text.as_deref() else {
                return Err(SprigError::new(
                    SprigCode::SearchUnsupported,
                    "AGAINST needs a string literal on a search index (placeholders aren't supported yet)",
                ));
            };
            let query = match m.mode {
                Mode::Natural => mysql_natural(text, &cols),
                Mode::Boolean => parse_mysql_boolean(text, &cols)?,
                Mode::Expansion => {
                    return Err(SprigError::new(
                        SprigCode::SearchUnsupported,
                        "WITH QUERY EXPANSION is not supported by search indexes",
                    ));
                }
            };
            let db = tref
                .db
                .clone()
                .or_else(|| current.clone())
                .ok_or_else(no_db)?;
            let db_id = svc
                .catalog()
                .db_id(&cat, &db)
                .ok_or_else(|| unknown_db(&db))?;
            if !self.allowed(&cat, lctn, &db, db_id, Some(def.object_id), Action::Select) {
                return Err(self.denied("SELECT", &db, &def.object));
            }
            // A converted MySQL FULLTEXT index answers as InnoDB's does:
            // with every committed row (decision S9).
            let consistency = if def.flavor == Flavor::FulltextCompat {
                Consistency::Latest
            } else {
                knobs.consistency
            };
            let hits = self
                .run_search(svc, tenant, &cat, &def, query, None, consistency)
                .await?;
            let qual = match (
                &tref.alias,
                scan.tables.len() > 1 || m.columns.iter().any(|(q, _)| q.is_some()),
            ) {
                (Some(a), true) => format!("{}.", quote(a)),
                (None, true) => format!("{}.", quote(&tref.table)),
                _ => String::new(),
            };
            let tgt = Target2 { def, qual };
            let rep = if m.condition {
                tgt.in_list(&hits)?
            } else {
                tgt.score_case(&hits)?
            };
            edits.push((m.span, rep));
        }
        for s in &scan.searches {
            let rep = self
                .search_table(cx, wire, svc, tenant, &cat, lctn, s)
                .await?;
            edits.push((s.span, rep));
        }
        edits.sort_by_key(|e| e.0.0);
        let mut out = String::with_capacity(sql.len() + 256);
        let mut last = 0;
        for ((a, b), rep) in edits {
            out.push_str(&sql[last..a]);
            out.push_str(&rep);
            last = b;
        }
        out.push_str(&sql[last..]);
        Ok(Some(out))
    }

    /// One search through the index's copy here, after the consistency
    /// wait: every hit up to `search.max_window`, or the best `limit`.
    #[allow(clippy::too_many_arguments)]
    async fn run_search(
        &self,
        svc: &Arc<SearchService>,
        tenant: sprig_common::ids::TenantId,
        cat: &TenantCatalog,
        def: &Arc<IndexDef>,
        query: SearchQuery,
        limit: Option<usize>,
        consistency: Consistency,
    ) -> Result<Vec<Hit>> {
        run_search(svc, tenant, cat, def, query, limit, consistency).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn search_table(
        &mut self,
        cx: &mut ExecCtx<'_>,
        wire: Wire,
        svc: &Arc<SearchService>,
        tenant: sprig_common::ids::TenantId,
        cat: &TenantCatalog,
        lctn: bool,
        call: &sqlscan::SearchCall,
    ) -> Result<String> {
        let (db, name) = match call.index.split_once('.') {
            Some((d, n)) => (d.to_owned(), n.to_owned()),
            None => (
                self.current_logical().ok_or_else(no_db)?,
                call.index.clone(),
            ),
        };
        let def = self.index_by_name(cat, &db, &name)?;
        let db_id = svc
            .catalog()
            .db_id(cat, &db)
            .ok_or_else(|| unknown_db(&db))?;
        if !self.allowed(cat, lctn, &db, db_id, Some(def.object_id), Action::Select) {
            return Err(self.denied("SELECT", &db, &def.object));
        }
        let opts = match &call.options {
            Some(o) => sqlscan::options_json(o)?,
            None => serde_json::Value::Null,
        };
        let knobs = svc.knobs();
        let limit = opts
            .get("limit")
            .and_then(serde_json::Value::as_u64)
            .map_or(10, |n| usize::try_from(n).unwrap_or(usize::MAX))
            .min(knobs.max_window);
        let consistency = match opts.get("consistency").and_then(serde_json::Value::as_str) {
            Some(c) => Consistency::parse(c)?,
            None => knobs.consistency,
        };
        let query = parse_query_string(&call.query, &[])?;
        let highlight: Vec<String> = opts
            .get("highlight")
            .and_then(serde_json::Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        let hits = run_search(
            svc,
            tenant,
            cat,
            &def,
            query.clone(),
            Some(limit),
            consistency,
        )
        .await?;
        let tgt = Target2 {
            def: Arc::clone(&def),
            qual: String::new(),
        };
        let keys: Vec<String> = def.key_columns.iter().map(|c| quote(c)).collect();
        let table = format!("{}.{}", quote(&def.db), quote(&def.table));
        // Highlights from the text the user can read.
        let hl = if highlight.is_empty() || hits.is_empty() {
            "NULL".to_owned()
        } else {
            self.highlights(cx, wire, svc, &def, &query, &hits, &highlight, &table)
                .await?
        };
        Ok(format!(
            "(SELECT {}, {} AS score, {} AS highlights, 0 AS _shard FROM {table} WHERE {})",
            keys.join(", "),
            tgt.score_case(&hits)?,
            hl,
            tgt.in_list(&hits)?
        ))
    }

    /// A `CASE` giving each hit's highlights as JSON, from the fields'
    /// text read as the user.
    #[allow(clippy::too_many_arguments)]
    async fn highlights(
        &mut self,
        cx: &mut ExecCtx<'_>,
        wire: Wire,
        svc: &Arc<SearchService>,
        def: &Arc<IndexDef>,
        query: &SearchQuery,
        hits: &[Hit],
        fields: &[String],
        table: &str,
    ) -> Result<String> {
        if def.model != Model::Table {
            return Ok("NULL".to_owned());
        }
        let knobs = svc.knobs();
        let tgt = Target2 {
            def: Arc::clone(def),
            qual: String::new(),
        };
        let keys: Vec<String> = def.key_columns.iter().map(|c| quote(c)).collect();
        let fdefs: Vec<&FieldDef> = fields
            .iter()
            .filter_map(|n| {
                def.fields
                    .iter()
                    .find(|f| f.name.eq_ignore_ascii_case(n) && f.kind.is_text())
            })
            .collect();
        if fdefs.is_empty() {
            return Ok("NULL".to_owned());
        }
        let cols: Vec<String> = fdefs.iter().map(|f| quote(&f.source_ref)).collect();
        let q = format!(
            "SELECT {}, {} FROM {table} WHERE {}",
            keys.join(", "),
            cols.join(", "),
            tgt.in_list(hits)?
        );
        let rows = self.sub_rows(cx, wire, &q).await?;
        let schema = sprig_search::schema::IndexSchema::build(def, &[])?;
        let terms = query_terms(query, &schema);
        let nk = keys.len();
        let mut case = String::from("(CASE");
        for r in rows {
            let parts: Vec<sprig_search::KeyPart> = (0..nk)
                .map(|i| {
                    let ty = def.key_types.get(i).map_or("", String::as_str);
                    crate::search::runtime::key_part_from_text(
                        r[i].as_deref().unwrap_or_default(),
                        ty,
                    )
                })
                .collect();
            let key = Key::of_parts(&parts);
            let mut obj = serde_json::Map::new();
            for (j, f) in fdefs.iter().enumerate() {
                let Some(text) = r.get(nk + j).and_then(Option::as_deref) else {
                    continue;
                };
                let text = String::from_utf8_lossy(text);
                let an = schema
                    .analyzers
                    .get(f.index_analyzer(&def.default_analyzer))?;
                if let Some(fr) = highlight::fragment(an, &text, &terms, knobs.highlight_chars) {
                    obj.insert(f.name.clone(), serde_json::json!([fr]));
                }
            }
            let _ = write!(
                case,
                " WHEN {} = {} THEN {}",
                tgt.key_expr(),
                key.sql(def.model)?,
                lit(&serde_json::Value::Object(obj).to_string())
            );
        }
        case.push_str(" ELSE NULL END)");
        Ok(case)
    }
}

/// The analyzed terms of a query (for highlights): every token of its
/// text, as each text field's query analyzer gives it.
pub(crate) fn query_terms(q: &SearchQuery, schema: &sprig_search::IndexSchema) -> HashSet<String> {
    let mut out = HashSet::new();
    fn walk(q: &SearchQuery, schema: &sprig_search::IndexSchema, out: &mut HashSet<String>) {
        match q {
            SearchQuery::Match { text, .. }
            | SearchQuery::Phrase { text, .. }
            | SearchQuery::Prefix { text, .. } => {
                for a in schema.analyzers.iter() {
                    for t in a.query_tokens(text) {
                        out.insert(t.text);
                    }
                }
            }
            SearchQuery::Bool {
                must,
                should,
                filter,
                ..
            } => {
                for x in must.iter().chain(should).chain(filter) {
                    walk(x, schema, out);
                }
            }
            SearchQuery::Boost(x, _) => walk(x, schema, out),
            _ => {}
        }
    }
    walk(q, schema, &mut out);
    out
}

/// One search through a copy of the index (on a worker, or in this
/// gateway; shared by SQL, the document engine and the Search API): waits
/// as `consistency` asks, then every hit (up to `search.max_window`, 39827
/// past it) or the best `limit`.
///
/// # Errors
/// 39824, 39825, 39826, 39827; the query's errors.
pub(crate) async fn run_search(
    svc: &Arc<SearchService>,
    tenant: sprig_common::ids::TenantId,
    cat: &TenantCatalog,
    def: &Arc<IndexDef>,
    query: SearchQuery,
    limit: Option<usize>,
    consistency: Consistency,
) -> Result<Vec<Hit>> {
    let knobs = svc.knobs();
    let op = match limit {
        Some(k) => sprig_search::rpc::Op::Top {
            k,
            scoring: knobs.scoring,
        },
        None => sprig_search::rpc::Op::All {
            max: knobs.max_window,
        },
    };
    let started = std::time::Instant::now();
    let r = svc
        .query(
            tenant,
            def,
            &scat::state(cat).analyzers,
            query,
            vec![op],
            consistency,
        )
        .await
        .and_then(|(mut a, _)| match a.pop() {
            Some(sprig_search::rpc::Answer::Hits(h)) => Ok(h),
            _ => Err(SprigError::internal("a search copy answered without hits")),
        });
    metrics::histogram!("sprig_search_query_duration_seconds", "phase" => "search")
        .record(started.elapsed().as_secs_f64());
    metrics::counter!(
        "sprig_search_queries_total",
        "flavor" => def.flavor.as_str(),
        "result" => if r.is_ok() { "ok" } else { "error" }
    )
    .increment(1);
    if let Err(e) = &r
        && e.code() == Some(SprigCode::SearchWindowExceeded)
    {
        metrics::counter!("sprig_search_window_exceeded_total").increment(1);
    }
    r
}

/// A bound parameter as a SQL literal (prepared statements whose `MATCH
/// ... AGAINST (?)` runs on a search index are run as text at execute).
pub(super) fn param_literal(p: &sprig_proto_mysql::command::BoundParam) -> String {
    use sprig_proto_mysql::Value;
    use sprig_proto_mysql::command::ParamValue;
    let ParamValue::Value(v) = &p.value else {
        return "NULL".to_owned();
    };
    match v {
        Value::Null => "NULL".to_owned(),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        Value::Float(f) => format!("{:e}", f64::from(*f)),
        Value::Double(f) => format!("{f:e}"),
        Value::Bytes(b) => match std::str::from_utf8(b) {
            Ok(t) => lit(t),
            Err(_) => {
                let mut h = String::from("X'");
                for x in b.iter() {
                    let _ = write!(h, "{x:02X}");
                }
                h.push('\'');
                h
            }
        },
        Value::DateTime(d) => format!(
            "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}'",
            d.year, d.month, d.day, d.hour, d.minute, d.second, d.micro
        ),
        Value::Time(t) => format!(
            "'{}{}:{:02}:{:02}.{:06}'",
            if t.negative { "-" } else { "" },
            u64::from(t.days) * 24 + u64::from(t.hour),
            t.minute,
            t.second,
            t.micro
        ),
    }
}

/// Text rows as binary rows (a prepared statement whose `MATCH` runs on a
/// search index executes as text; its client reads the binary protocol).
pub(super) struct ToBinary<'a> {
    pub(super) inner: &'a mut dyn ResultSink,
    columns: Vec<ColumnDef>,
}

impl<'a> ToBinary<'a> {
    pub(super) fn new(inner: &'a mut dyn ResultSink) -> Self {
        Self {
            inner,
            columns: Vec::new(),
        }
    }

    async fn text(&mut self, v: &[Option<&[u8]>]) -> Result<()> {
        let values: Vec<sprig_proto_mysql::Value> = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| super::router::binary::text_value(c, v.get(i).copied().flatten()))
            .collect();
        self.inner.row(RowValues::Binary(&values)).await
    }
}

#[async_trait]
impl ResultSink for ToBinary<'_> {
    fn wire(&self) -> Wire {
        self.inner.wire()
    }
    async fn begin_rows(&mut self, columns: &[ColumnDef], _format: RowFormat) -> Result<()> {
        self.columns = columns.to_vec();
        self.inner.begin_rows(columns, RowFormat::Binary).await
    }
    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        match values {
            RowValues::Text(v) => self.text(v).await,
            b @ RowValues::Binary(_) => self.inner.row(b).await,
        }
    }
    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        let r = sprig_proto_mysql::resultset::decode_text_row(&payload, self.columns.len().max(1))?;
        let v: Vec<Option<&[u8]>> = r.iter().map(|c| c.as_deref()).collect();
        self.text(&v).await
    }
    async fn end_rows(&mut self, end: EndOfRows) -> Result<()> {
        self.inner.end_rows(end).await
    }
    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.inner.ok(ok).await
    }
    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.inner.error(err).await
    }
    async fn column_list(&mut self, c: &[ColumnDef], e: EndOfRows) -> Result<()> {
        self.inner.column_list(c, e).await
    }
    async fn local_infile_request(&mut self, f: &[u8]) -> Result<()> {
        self.inner.local_infile_request(f).await
    }
    async fn local_infile_read(&mut self) -> Result<Bytes> {
        self.inner.local_infile_read().await
    }
    async fn flush(&mut self) -> Result<()> {
        self.inner.flush().await
    }
    fn finished(&self) -> bool {
        self.inner.finished()
    }
}

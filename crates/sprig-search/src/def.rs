//! Search index definitions:
//! what a search index covers, as the catalog keeps it and every front end
//! creates it: SQL's `CREATE SEARCH INDEX` ([`crate::ddl`]), MongoDB's
//! `createSearchIndexes` ([`IndexDef::from_atlas`]) and `text` indexes
//! ([`IndexDef::from_mongo_text`]), and MySQL `FULLTEXT` indexes converted
//! with the `mysql_compat` analyzer.

use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use sprig_common::{Result, SprigCode, SprigError};

use crate::analyze;

/// The data model of the indexed object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Model {
    /// A SQL table: fields are columns.
    Table,
    /// A collection: fields are paths in the document.
    Collection,
    /// A key-value namespace: fields are paths in the value.
    Kv,
}

impl Model {
    /// The catalog's name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Collection => "collection",
            Self::Kv => "kv",
        }
    }
}

/// How the index was declared, which sets defaults and query semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Flavor {
    /// `CREATE SEARCH INDEX`.
    Sql,
    /// A MongoDB `text` index (`createIndexes`).
    MongoText,
    /// An Atlas-style search index (`createSearchIndexes`).
    MongoSearch,
    /// A MySQL `FULLTEXT` index converted to a search index.
    FulltextCompat,
}

impl Flavor {
    /// The catalog's name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sql => "sql",
            Self::MongoText => "mongo_text",
            Self::MongoSearch => "mongo_search",
            Self::FulltextCompat => "fulltext_compat",
        }
    }
}

/// A field's kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldKind {
    /// Analyzed text with positions.
    Text,
    /// The exact value: filters, sorts, facets.
    Keyword,
    /// A number (f64).
    Number,
    /// A date.
    Date,
    /// A boolean.
    Bool,
    /// Edge n-grams, for type-ahead.
    Autocomplete,
    /// Trigrams, for `LIKE` acceleration.
    Substring,
    /// Hierarchical facet paths.
    Facet,
}

impl FieldKind {
    /// The DDL keyword.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Keyword => "keyword",
            Self::Number => "number",
            Self::Date => "date",
            Self::Bool => "bool",
            Self::Autocomplete => "autocomplete",
            Self::Substring => "substring",
            Self::Facet => "facet",
        }
    }

    /// From a DDL keyword (any case).
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_lowercase().as_str() {
            "text" | "string" => Self::Text,
            "keyword" | "token" => Self::Keyword,
            "number" | "numeric" => Self::Number,
            "date" => Self::Date,
            "bool" | "boolean" => Self::Bool,
            "autocomplete" => Self::Autocomplete,
            "substring" => Self::Substring,
            "facet" => Self::Facet,
            _ => return None,
        })
    }

    /// Whether the field is analyzed text (it scores with BM25).
    #[must_use]
    pub fn is_text(self) -> bool {
        matches!(self, Self::Text | Self::Autocomplete | Self::Substring)
    }
}

/// Where a field's values come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// A column of a table.
    Column,
    /// A path in a document (`notes`, `customer.name`).
    Path,
    /// A path in a key-value value.
    KvValue,
    /// Every string of the document (`dynamic: true`, `$**`).
    Dynamic,
}

/// One field of an index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldDef {
    /// Order in the Tantivy schema (`f<field_no>`).
    pub field_no: u16,
    /// The name in queries: the column, or the dotted path.
    pub name: String,
    /// Where values come from.
    pub source: Source,
    /// The column name or the dotted path.
    pub source_ref: String,
    /// The kind.
    pub kind: FieldKind,
    /// Index-time analyzer (text kinds); `None` for the index's default.
    #[serde(default)]
    pub analyzer: Option<String>,
    /// Query-time analyzer; `None` for the index-time one.
    #[serde(default)]
    pub query_analyzer: Option<String>,
    /// Score multiplier.
    #[serde(default = "one")]
    pub boost: f64,
    /// Kept as a fast field for filters.
    #[serde(default)]
    pub filter: bool,
    /// Kept as a fast field for sorts.
    #[serde(default)]
    pub sort: bool,
    /// Counted by facets.
    #[serde(default)]
    pub facet: bool,
    /// The generation that added it.
    #[serde(default)]
    pub added_generation: u32,
}

fn one() -> f64 {
    1.0
}

impl FieldDef {
    /// A field with defaults.
    #[must_use]
    pub fn new(name: &str, source: Source, kind: FieldKind) -> Self {
        Self {
            field_no: 0,
            name: name.to_owned(),
            source,
            source_ref: name.to_owned(),
            kind,
            analyzer: None,
            query_analyzer: None,
            boost: 1.0,
            filter: false,
            sort: false,
            facet: false,
            added_generation: 1,
        }
    }

    /// The index-time analyzer, given the index default.
    #[must_use]
    pub fn index_analyzer<'a>(&'a self, default: &'a str) -> &'a str {
        match self.kind {
            FieldKind::Keyword => "keyword",
            FieldKind::Autocomplete => self.analyzer.as_deref().unwrap_or("autocomplete"),
            FieldKind::Substring => "trigram",
            _ => self.analyzer.as_deref().unwrap_or(default),
        }
    }

    /// Whether it needs a fast field.
    #[must_use]
    pub fn fast(&self) -> bool {
        self.filter || self.sort || self.facet
    }
}

/// A search index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexDef {
    /// `objects.object_id` of the index (0 before it is created).
    #[serde(default)]
    pub index_id: u64,
    /// Its name, unique per database.
    pub name: String,
    /// The logical database.
    pub db: String,
    /// The indexed table, collection or namespace.
    pub object: String,
    /// Its `objects.object_id` (0 when not known).
    #[serde(default)]
    pub object_id: u64,
    /// The model.
    pub model: Model,
    /// The flavor.
    pub flavor: Flavor,
    /// The analyzer of text fields that name none.
    pub default_analyzer: String,
    /// `dynamic: true` (Atlas) or a `$**` text index: every string.
    #[serde(default)]
    pub dynamic: bool,
    /// The table holding the rows (a collection's backing table; the
    /// object itself for a table).
    #[serde(default)]
    pub table: String,
    /// The object's primary key columns (tables; `_id` for collections,
    /// `k` for namespaces).
    #[serde(default)]
    pub key_columns: Vec<String>,
    /// Their `information_schema` data types (tables), so keys read as
    /// text and keys read from the binlog encode alike.
    #[serde(default)]
    pub key_types: Vec<String>,
    /// The tenant's name when the index was created (the definer's
    /// principal is looked up by it).
    #[serde(default)]
    pub tenant_name: String,
    /// The fields.
    pub fields: Vec<FieldDef>,
    /// Raised when the Tantivy schema changes.
    #[serde(default = "gen1")]
    pub generation: u32,
    /// Partitions (key-hash buckets) per copy.
    #[serde(default = "parts")]
    pub partitions: u32,
    /// The user whose privileges build it (its creator).
    #[serde(default)]
    pub definer_user_id: u32,
    /// The definition as given (DDL text, or the BSON as JSON).
    #[serde(default)]
    pub given: Json,
}

fn gen1() -> u32 {
    1
}

fn parts() -> u32 {
    2
}

fn bad(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::SearchQuerySyntax, msg)
}

fn unsupported(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::SearchUnsupported, msg)
}

impl IndexDef {
    /// An empty definition.
    #[must_use]
    pub fn new(name: &str, db: &str, object: &str, model: Model, flavor: Flavor) -> Self {
        Self {
            index_id: 0,
            name: name.to_owned(),
            db: db.to_owned(),
            object: object.to_owned(),
            object_id: 0,
            model,
            flavor,
            default_analyzer: match flavor {
                Flavor::MongoText => "mongo_english".to_owned(),
                Flavor::FulltextCompat => "mysql_compat".to_owned(),
                _ => "standard".to_owned(),
            },
            dynamic: false,
            table: object.to_owned(),
            key_types: Vec::new(),
            tenant_name: String::new(),
            key_columns: match model {
                Model::Table => Vec::new(),
                Model::Collection => vec!["_id".to_owned()],
                Model::Kv => vec!["k".to_owned()],
            },
            fields: Vec::new(),
            generation: 1,
            partitions: parts(),
            definer_user_id: 0,
            given: Json::Null,
        }
    }

    /// Adds a field, numbering it.
    ///
    /// # Errors
    /// 39829 for a duplicate (name, kind).
    pub fn push(&mut self, mut f: FieldDef) -> Result<()> {
        if self
            .fields
            .iter()
            .any(|x| x.name == f.name && x.kind == f.kind)
        {
            return Err(SprigError::new(
                SprigCode::SearchFieldType,
                format!(
                    "field '{}' is declared twice as {}",
                    f.name,
                    f.kind.as_str()
                ),
            ));
        }
        f.field_no = self
            .fields
            .iter()
            .map(|x| x.field_no + 1)
            .max()
            .unwrap_or(0);
        if f.added_generation == 0 {
            f.added_generation = self.generation;
        }
        self.fields.push(f);
        Ok(())
    }

    /// Every analyzer name the index uses (index and query time).
    #[must_use]
    pub fn analyzer_names(&self) -> Vec<String> {
        let mut v = vec![self.default_analyzer.clone()];
        for f in &self.fields {
            v.push(f.index_analyzer(&self.default_analyzer).to_owned());
            if let Some(q) = &f.query_analyzer {
                v.push(q.clone());
            }
        }
        v.push("keyword".to_owned());
        v.sort();
        v.dedup();
        v
    }

    /// Checks the analyzers exist (built-in or in `customs`).
    ///
    /// # Errors
    /// 39828.
    pub fn check(&self, customs: &[analyze::CustomAnalyzer]) -> Result<()> {
        if self.fields.is_empty() && !self.dynamic {
            return Err(bad(format!("search index '{}' has no fields", self.name)));
        }
        let names = self.analyzer_names();
        analyze::Analyzers::resolve(names.iter().map(String::as_str), customs)?;
        Ok(())
    }

    /// The text fields queries search when they name none.
    #[must_use]
    pub fn default_fields(&self) -> Vec<&FieldDef> {
        self.fields
            .iter()
            .filter(|f| matches!(f.kind, FieldKind::Text))
            .collect()
    }

    /// The fields named `name` (one per kind).
    #[must_use]
    pub fn fields_named(&self, name: &str) -> Vec<&FieldDef> {
        self.fields.iter().filter(|f| f.name == name).collect()
    }

    /// Whether a `MATCH(columns)` is covered: every column is a text field.
    /// The extra text fields it has (fewer is better), or `None`.
    #[must_use]
    pub fn covers(&self, columns: &[String]) -> Option<usize> {
        let text: Vec<&FieldDef> = self.fields.iter().filter(|f| f.kind.is_text()).collect();
        let all = columns
            .iter()
            .all(|c| text.iter().any(|f| f.name.eq_ignore_ascii_case(c)));
        all.then(|| text.len().saturating_sub(columns.len()))
    }

    /// The definition of an Atlas search index (`createSearchIndexes`,
    /// `type: "search"`) on collection `coll` of `db`.
    ///
    /// # Errors
    /// 39822 for a malformed definition, 39823 for `embeddedDocuments`
    /// and other types not offered, 39828 for an unknown analyzer.
    pub fn from_atlas(name: &str, db: &str, coll: &str, def: &Json) -> Result<Self> {
        let mut d = Self::new(name, db, coll, Model::Collection, Flavor::MongoSearch);
        d.given = def.clone();
        if let Some(a) = def.get("analyzer").and_then(Json::as_str) {
            d.default_analyzer = analyze::from_atlas(a);
        }
        let search_analyzer = def
            .get("searchAnalyzer")
            .and_then(Json::as_str)
            .map(analyze::from_atlas);
        let mappings = def
            .get("mappings")
            .ok_or_else(|| bad("a search index definition needs 'mappings'"))?;
        d.dynamic = mappings
            .get("dynamic")
            .and_then(Json::as_bool)
            .unwrap_or(false);
        if let Some(fields) = mappings.get("fields") {
            let obj = fields
                .as_object()
                .ok_or_else(|| bad("'mappings.fields' must be an object"))?;
            for (k, v) in obj {
                atlas_field(&mut d, k, v, search_analyzer.as_deref())?;
            }
        }
        if d.fields.is_empty() && !d.dynamic {
            return Err(bad(
                "a search index needs 'mappings.dynamic: true' or at least one field",
            ));
        }
        Ok(d)
    }

    /// The definition of a MongoDB `text` index: `key` is the index's
    /// key pattern as (path, value) pairs, `weights` (path, weight),
    /// `language` its `default_language`.
    ///
    /// # Errors
    /// 39823 for a compound key with non-text parts or a
    /// `language_override`; 39828 for an unknown language.
    pub fn from_mongo_text(
        name: &str,
        db: &str,
        coll: &str,
        key: &[(String, Json)],
        weights: &[(String, f64)],
        language: Option<&str>,
        language_override: Option<&str>,
    ) -> Result<Self> {
        let mut d = Self::new(name, db, coll, Model::Collection, Flavor::MongoText);
        if let Some(l) = language {
            let l = l.to_ascii_lowercase();
            d.default_analyzer = if l == "none" {
                "mongo_none".to_owned()
            } else {
                let lang = analyze::language(&l).ok_or_else(|| {
                    SprigError::new(
                        SprigCode::SearchAnalyzerUnknown,
                        format!("unsupported language: \"{l}\" for text index version 3"),
                    )
                })?;
                format!("mongo_{}", format!("{lang:?}").to_ascii_lowercase())
            };
        }
        if language_override.is_some_and(|o| o != "language") {
            return Err(unsupported(
                "text indexes with a language_override are not supported",
            ));
        }
        for (path, v) in key {
            if v.as_str() != Some("text") {
                // MongoDB's own `_fts`/`_ftsx` form, as listIndexes shows it.
                if path == "_fts" || path == "_ftsx" {
                    continue;
                }
                return Err(unsupported(
                    "compound text indexes with non-text keys are not supported",
                ));
            }
            if path == "$**" {
                d.dynamic = true;
                continue;
            }
            let mut f = FieldDef::new(path, Source::Path, FieldKind::Text);
            f.boost = 1.0;
            d.push(f)?;
        }
        for (path, w) in weights {
            if path == "$**" {
                continue;
            }
            if let Some(f) = d.fields.iter_mut().find(|f| &f.name == path) {
                f.boost = *w;
            } else {
                let mut f = FieldDef::new(path, Source::Path, FieldKind::Text);
                f.boost = *w;
                d.push(f)?;
            }
        }
        if d.fields.is_empty() && !d.dynamic {
            return Err(bad("a text index needs at least one text key"));
        }
        Ok(d)
    }
}

fn atlas_field(
    d: &mut IndexDef,
    path: &str,
    v: &Json,
    search_analyzer: Option<&str>,
) -> Result<()> {
    if let Some(arr) = v.as_array() {
        for x in arr {
            atlas_field(d, path, x, search_analyzer)?;
        }
        return Ok(());
    }
    let t = v.get("type").and_then(Json::as_str).unwrap_or("document");
    let kind = match t {
        "string" => FieldKind::Text,
        "token" | "objectId" | "uuid" | "stringFacet" => FieldKind::Keyword,
        "number" | "numberFacet" => FieldKind::Number,
        "date" | "dateFacet" => FieldKind::Date,
        "boolean" => FieldKind::Bool,
        "autocomplete" => FieldKind::Autocomplete,
        "document" => {
            if let Some(fields) = v.get("fields").and_then(Json::as_object) {
                for (k, x) in fields {
                    atlas_field(d, &format!("{path}.{k}"), x, search_analyzer)?;
                }
            }
            if v.get("dynamic").and_then(Json::as_bool) == Some(true) {
                d.dynamic = true;
            }
            return Ok(());
        }
        "embeddedDocuments" | "geo" | "knnVector" | "vector" => {
            return Err(unsupported(format!(
                "search field type '{t}' is not supported (path '{path}')"
            )));
        }
        other => return Err(bad(format!("unknown search field type '{other}'"))),
    };
    let mut f = FieldDef::new(path, Source::Path, kind);
    if let Some(a) = v.get("analyzer").and_then(Json::as_str) {
        f.analyzer = Some(analyze::from_atlas(a));
    }
    f.query_analyzer = v
        .get("searchAnalyzer")
        .and_then(Json::as_str)
        .map(analyze::from_atlas)
        .or_else(|| search_analyzer.map(str::to_owned));
    if kind == FieldKind::Keyword || t.ends_with("Facet") {
        f.filter = true;
        f.facet = true;
    }
    if matches!(kind, FieldKind::Number | FieldKind::Date | FieldKind::Bool) {
        f.filter = true;
        f.sort = true;
        f.facet = t.ends_with("Facet") || kind != FieldKind::Bool;
    }
    // `token` and `stringFacet` (or `number` and `numberFacet`) on one
    // path are one field with both uses.
    if let Some(x) = d
        .fields
        .iter_mut()
        .find(|x| x.name == f.name && x.kind == f.kind)
    {
        x.filter |= f.filter;
        x.sort |= f.sort;
        x.facet |= f.facet;
        return Ok(());
    }
    d.push(f)
}

/// Whether a SQL column of `data_type` (as `information_schema` names it)
/// can be a field of `kind`.
///
/// # Errors
/// 39829 when it can't.
pub fn check_column_kind(column: &str, data_type: &str, kind: FieldKind) -> Result<()> {
    let t = data_type.to_ascii_lowercase();
    let texty =
        t.contains("char") || t.contains("text") || t == "json" || t == "enum" || t == "set";
    let numeric = matches!(
        t.as_str(),
        "tinyint"
            | "smallint"
            | "mediumint"
            | "int"
            | "integer"
            | "bigint"
            | "decimal"
            | "numeric"
            | "float"
            | "double"
            | "real"
            | "bit"
            | "year"
    );
    let datey = matches!(t.as_str(), "date" | "datetime" | "timestamp" | "time");
    let ok = match kind {
        FieldKind::Text | FieldKind::Autocomplete | FieldKind::Substring | FieldKind::Facet => {
            texty
        }
        FieldKind::Keyword => texty || numeric || datey || t.contains("binary") || t == "uuid",
        FieldKind::Number | FieldKind::Bool => numeric,
        FieldKind::Date => datey,
    };
    if ok {
        Ok(())
    } else {
        Err(SprigError::new(
            SprigCode::SearchFieldType,
            format!(
                "column '{column}' of type {data_type} can't be a {} field",
                kind.as_str()
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn atlas_mapping() {
        let d = IndexDef::from_atlas(
            "default",
            "shop",
            "orders",
            &json!({"mappings": {"dynamic": false, "fields": {
                "notes": {"type": "string", "analyzer": "lucene.english"},
                "tags": {"type": "token"},
                "customer": {"type": "document", "fields": {"name": {"type": "autocomplete"}}},
                "price": [{"type": "number"}, {"type": "token"}, {"type": "numberFacet"}],
                "brand": [{"type": "token"}, {"type": "stringFacet"}]
            }}}),
        )
        .unwrap();
        let names: Vec<_> = d.fields.iter().map(|f| (f.name.as_str(), f.kind)).collect();
        assert!(names.contains(&("notes", FieldKind::Text)));
        assert!(names.contains(&("customer.name", FieldKind::Autocomplete)));
        assert!(names.contains(&("price", FieldKind::Number)));
        assert!(names.contains(&("price", FieldKind::Keyword)));
        let notes = d.fields.iter().find(|f| f.name == "notes").unwrap();
        assert_eq!(notes.analyzer.as_deref(), Some("english"));
        let brand: Vec<_> = d.fields.iter().filter(|f| f.name == "brand").collect();
        assert_eq!(brand.len(), 1);
        assert!(brand[0].facet && brand[0].filter);
        d.check(&[]).unwrap();
    }

    #[test]
    fn atlas_refusals() {
        let e = IndexDef::from_atlas(
            "x",
            "d",
            "c",
            &json!({"mappings": {"fields": {"a": {"type": "embeddedDocuments"}}}}),
        )
        .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::SearchUnsupported));
    }

    #[test]
    fn mongo_text_weights_and_language() {
        let d = IndexDef::from_mongo_text(
            "t",
            "d",
            "c",
            &[
                ("title".into(), json!("text")),
                ("body".into(), json!("text")),
            ],
            &[("title".into(), 10.0)],
            Some("french"),
            None,
        )
        .unwrap();
        assert_eq!(d.default_analyzer, "mongo_french");
        assert!((d.fields[0].boost - 10.0).abs() < f64::EPSILON);
        d.check(&[]).unwrap();
        let e = IndexDef::from_mongo_text(
            "t",
            "d",
            "c",
            &[("a".into(), json!(1)), ("b".into(), json!("text"))],
            &[],
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::SearchUnsupported));
    }

    #[test]
    fn covers_prefers_fewer_extra_fields() {
        let mut d = IndexDef::new("i", "d", "t", Model::Table, Flavor::Sql);
        d.push(FieldDef::new("name", Source::Column, FieldKind::Text))
            .unwrap();
        d.push(FieldDef::new("body", Source::Column, FieldKind::Text))
            .unwrap();
        assert_eq!(d.covers(&["NAME".into()]), Some(1));
        assert_eq!(d.covers(&["name".into(), "body".into()]), Some(0));
        assert_eq!(d.covers(&["other".into()]), None);
    }

    #[test]
    fn column_kinds() {
        check_column_kind("n", "varchar", FieldKind::Text).unwrap();
        check_column_kind("p", "decimal", FieldKind::Number).unwrap();
        assert!(check_column_kind("p", "int", FieldKind::Text).is_err());
    }
}

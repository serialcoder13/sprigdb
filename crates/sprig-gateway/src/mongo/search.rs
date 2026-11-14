//! Atlas's search index commands on collections: `createSearchIndexes`,
//! `updateSearchIndex` and `dropSearchIndex`, for indexes of type
//! `vectorSearch`. `$listSearchIndexes` and `$vectorSearch` are
//! aggregation stages the document engine runs.

use bson::{Bson, Document, doc};
use sprig_doc::engine::Ns;
use sprig_proto_mongo::errors::{MongoError, code};

use super::sql::SqlSession;

/// Whether `name` is one of these commands.
#[must_use]
pub fn handles(name: &str) -> bool {
    matches!(
        name,
        "createSearchIndexes" | "updateSearchIndex" | "dropSearchIndex"
    )
}

/// Runs one of these commands on collection `body[name]` of `db`.
///
/// # Errors
/// The engine's.
pub async fn command(
    engine: &sprig_doc::engine::Engine,
    sql: &mut SqlSession,
    name: &str,
    body: &Document,
    db: &str,
) -> Result<Document, MongoError> {
    let ns = Ns {
        db: db.to_owned(),
        coll: super::commands::coll_of(body, name)?,
    };
    match name {
        "createSearchIndexes" => {
            let specs = super::commands::docs_of(body, "indexes");
            if specs.is_empty() {
                return Err(MongoError::new(
                    code::BAD_VALUE,
                    "createSearchIndexes needs 'indexes'",
                ));
            }
            let created = engine.create_search_indexes(sql, &ns, &specs).await?;
            Ok(
                doc! {"indexesCreated": created.into_iter().map(Bson::Document).collect::<Vec<_>>(), "ok": 1.0},
            )
        }
        "updateSearchIndex" => {
            let n = body
                .get_str("name")
                .map_err(|_| MongoError::new(code::BAD_VALUE, "updateSearchIndex needs 'name'"))?;
            let def = body.get_document("definition").map_err(|_| {
                MongoError::new(code::BAD_VALUE, "updateSearchIndex needs 'definition'")
            })?;
            engine.update_search_index(sql, &ns, n, def).await?;
            Ok(doc! {"ok": 1.0})
        }
        _ => {
            let n = body
                .get_str("name")
                .map_err(|_| MongoError::new(code::BAD_VALUE, "dropSearchIndex needs 'name'"))?;
            engine.drop_search_index(sql, &ns, n).await?;
            Ok(doc! {"ok": 1.0})
        }
    }
}

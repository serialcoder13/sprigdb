//! `ViewBuilder` for tables (chapter 42, "The merged view"; build step 4):
//! the SQL of a space's merged view of an object, one CTE level per edge.
//!
//! ```text
//! level 0, no upstream:   own and orphan rows of the top space
//! level 0, upstream:      overlay(the parent tenant's share view, narrowed
//!                         by this tenant's TAKE, top space's layer)
//! level i:                overlay(level i-1 filtered by the edge's share
//!                         and take filters, cut to its columns, space i's layer)
//! overlay(p, s):          p LEFT JOIN s's row: IF(ovr bit, child, parent)
//!                         per field, hide rows dropped
//!                         UNION ALL s's own and orphan rows not in p
//! ```
//!
//! MariaDB has no `FULL OUTER JOIN`, so the overlay is a `LEFT JOIN` plus a
//! `NOT EXISTS` branch, as in the chapter. Key predicates (writes, orphaning)
//! are pushed into every level, because the key is the same at all levels.
//! Share filters apply to the parent level's merged values, before the
//! child's layer; unshared fields come down as `NULL`.

use std::fmt::Write as _;

use crate::model::{InheritableObject, SpaceId};

/// Backquotes an identifier.
#[must_use]
pub fn q(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

/// A string literal (space names and JSON paths only: no backslashes in
/// identifiers SprigDB names).
#[must_use]
pub fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

/// Where the top level's rows come from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Top {
    /// The top space's own rows: no parent edge.
    Base,
    /// The parent tenant's share, through the view SprigDB keeps in this
    /// tenant's schema (`__sprig_share_<id>`).
    Upstream {
        /// The view, as SQL.
        from: String,
        /// The columns the view exposes.
        columns: Vec<String>,
        /// This tenant's narrowing.
        take: Option<String>,
        /// The origin reported for its fields (the parent tenant's name).
        origin: String,
        /// A condition on `from`'s rows besides the narrowing (a copy's
        /// share, liveness and the share's current filter).
        filter: Option<String>,
    },
}

/// The share filter on the way into a level.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Edge {
    /// The parent's filter.
    pub filter: Option<String>,
    /// The child's narrowing.
    pub take: Option<String>,
    /// The shared columns; `None` = all.
    pub columns: Option<Vec<String>>,
}

/// One level: a space and the edge into it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Level {
    /// The space.
    pub space: SpaceId,
    /// Its name, for `SPRIG_ORIGIN()`.
    pub name: String,
    /// The edge from the previous level; `None` for level 0.
    pub edge: Option<Edge>,
}

/// A space's levels, top first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chain {
    /// Level 0's source.
    pub top: Top,
    /// The levels; the last is the space whose view this is.
    pub levels: Vec<Level>,
}

impl Chain {
    /// Whether the view is just the space's own rows.
    #[must_use]
    pub fn is_base(&self) -> bool {
        self.top == Top::Base && self.levels.len() == 1
    }

    /// Whether the last level inherits anything.
    #[must_use]
    pub fn has_upstream(&self) -> bool {
        !self.is_base()
    }

    /// The chain cut to its first `n` levels (a view of an ancestor
    /// space).
    #[must_use]
    pub fn upto(&self, n: usize) -> Self {
        Self {
            top: self.top.clone(),
            levels: self.levels[..n.min(self.levels.len())].to_vec(),
        }
    }
}

/// Builds the SQL of one object's merged views.
#[derive(Debug, Clone)]
pub struct ViewSql<'a> {
    /// The object.
    pub o: &'a InheritableObject,
    /// The layer table, as SQL (`` `shop`.`products` `` or `` `products` ``).
    pub layer: String,
    /// Key values to push into every level: SQL tuples, one per record
    /// (`1` or `(1, 'a')` for a two-column key).
    pub keys: Option<Vec<String>>,
    /// Carry `_sprig_o`, the origin of each field (`SPRIG_ORIGIN()`).
    pub origin: bool,
    /// Columns computed from the merged row rather than read: `(column,
    /// expression over the object's columns)` (a discriminated root's type
    /// columns, [`crate::plan::Ctx::derived`]).
    pub derived: Vec<(String, String)>,
}

const ORIGIN: &str = "`_sprig_o`";

/// The alias of every layer-table reference inside a merged view. The gateway's row-level
/// security rewrite leaves references under it alone and filters the merged
/// records instead (chapter 29, "Data inheritance"): a filter on each layer
/// row would drop an override or a hide marker and show the parent's values.
/// Clients can't write it: a statement naming a `__sprig_` name never
/// reaches the inheritance layer, so a reference under this alias in a
/// statement the layer generated is always its own.
pub const LAYER_ALIAS: &str = sprig_sql::rls::views::MERGED_LAYER_ALIAS;

/// [`LAYER_ALIAS`], quoted.
const C: &str = "`__sprig_lyr`";

impl ViewSql<'_> {
    fn key_pred(&self, alias: Option<&str>) -> String {
        let Some(keys) = &self.keys else {
            return String::new();
        };
        let col = |c: &str| match alias {
            Some(a) => format!("{a}.{}", q(c)),
            None => q(c),
        };
        let lhs = if self.o.key.len() == 1 {
            col(&self.o.key[0])
        } else {
            format!(
                "({})",
                self.o
                    .key
                    .iter()
                    .map(|k| col(k))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        if keys.is_empty() {
            return " AND FALSE".into();
        }
        format!(" AND {lhs} IN ({})", keys.join(", "))
    }

    fn key_join(&self, a: &str, b: &str) -> String {
        self.o
            .key
            .iter()
            .map(|k| format!("{a}.{k} = {b}.{k}", k = q(k)))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    fn cols(&self, alias: &str) -> String {
        self.o
            .columns
            .iter()
            .map(|c| format!("{alias}.{}", q(c)))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// `JSON_OBJECT` naming `label` for every field (a row of one space).
    fn origin_all(&self, label: &str) -> String {
        let parts: Vec<String> = self
            .o
            .values()
            .map(|c| format!("{}, {label}", lit(c)))
            .collect();
        format!("JSON_OBJECT({})", parts.join(", "))
    }

    fn label(name: &str, alias: &str) -> String {
        format!(
            "IF({alias}.`_sprig_state` = 'orphan', {}, {})",
            lit(&format!("{name} (orphaned)")),
            lit(name)
        )
    }

    /// `overlay(p, s)` as a CTE body.
    fn overlay(&self, p: &str, space: SpaceId, name: &str) -> String {
        let mut first = Vec::with_capacity(self.o.columns.len() + 1);
        for c in &self.o.columns {
            if self.o.is_key(c) {
                first.push(format!("p.{}", q(c)));
            } else {
                first.push(format!(
                    "IF({C}.`_sprig_ovr` & {bit}, {C}.{col}, p.{col}) AS {col}",
                    bit = self.o.bit(c),
                    col = q(c)
                ));
            }
        }
        let mut second = self.cols(C);
        if self.origin {
            let parts: Vec<String> = self
                .o
                .values()
                .map(|c| {
                    format!(
                        "{}, IF({C}.`_sprig_ovr` & {}, {}, JSON_VALUE(p.{ORIGIN}, {}))",
                        lit(c),
                        self.o.bit(c),
                        Self::label(name, C),
                        lit(&format!("$.\"{c}\""))
                    )
                })
                .collect();
            first.push(format!("JSON_OBJECT({}) AS {ORIGIN}", parts.join(", ")));
            let _ = write!(second, ", {}", self.origin_all(&Self::label(name, C)));
        }
        format!(
            "SELECT {first} FROM {p} p LEFT JOIN {layer} {C} ON {join} AND {C}.`_sprig_space` = \
             {s} WHERE {C}.`_sprig_state` IS NULL OR {C}.`_sprig_state` <> 'hide' \
             UNION ALL SELECT {second} FROM {layer} {C} WHERE {C}.`_sprig_space` = {s} \
             AND {C}.`_sprig_state` IN ('own', 'orphan'){kp} \
             AND NOT EXISTS (SELECT 1 FROM {p} p WHERE {join})",
            first = first.join(", "),
            layer = self.layer,
            join = self.key_join(C, "p"),
            s = space.0,
            kp = self.key_pred(Some(C)),
        )
    }

    /// The parent rows of level `i` (`i >= 1`): level `i - 1` filtered by
    /// the edge and cut to its columns.
    fn parent_rows(&self, prev: &str, e: &Edge) -> String {
        let mut cols: Vec<String> = self
            .o
            .columns
            .iter()
            .map(|c| {
                let shared = self.o.is_key(c)
                    || e.columns
                        .as_ref()
                        .is_none_or(|cs| cs.iter().any(|x| x.eq_ignore_ascii_case(c)));
                if shared {
                    q(c)
                } else {
                    format!("NULL AS {}", q(c))
                }
            })
            .collect();
        if self.origin {
            cols.push(ORIGIN.into());
        }
        format!(
            "SELECT {} FROM {prev} WHERE TRUE{}{}",
            cols.join(", "),
            e.filter
                .as_deref()
                .map(|f| format!(" AND ({f})"))
                .unwrap_or_default(),
            e.take
                .as_deref()
                .map(|f| format!(" AND ({f})"))
                .unwrap_or_default(),
        )
    }

    /// The CTEs of `chain` and the name of the last one.
    fn ctes(&self, chain: &Chain) -> (Vec<String>, String) {
        let mut out = Vec::new();
        let mut prev = String::new();
        for (i, lv) in chain.levels.iter().enumerate() {
            let l = format!("`__sprig_l{i}`");
            if i == 0 {
                match &chain.top {
                    Top::Base => {
                        let mut cols = self.o.columns.iter().map(|c| q(c)).collect::<Vec<_>>();
                        if self.origin {
                            cols.push(format!(
                                "{} AS {ORIGIN}",
                                self.origin_all(&format!(
                                    "IF(`_sprig_state` = 'orphan', {}, {})",
                                    lit(&format!("{} (orphaned)", lv.name)),
                                    lit(&lv.name)
                                ))
                            ));
                        }
                        out.push(format!(
                            "{l} AS (SELECT {} FROM {} {C} WHERE `_sprig_space` = {} AND \
                             `_sprig_state` IN ('own', 'orphan'){})",
                            cols.join(", "),
                            self.layer,
                            lv.space.0,
                            self.key_pred(None)
                        ));
                    }
                    Top::Upstream {
                        from,
                        columns,
                        take,
                        origin,
                        filter,
                    } => {
                        let mut cols: Vec<String> = self
                            .o
                            .columns
                            .iter()
                            .map(|c| {
                                if columns.iter().any(|x| x.eq_ignore_ascii_case(c)) {
                                    q(c)
                                } else {
                                    format!("NULL AS {}", q(c))
                                }
                            })
                            .collect();
                        if self.origin {
                            cols.push(format!("{} AS {ORIGIN}", self.origin_all(&lit(origin))));
                        }
                        let p = "`__sprig_p0`".to_owned();
                        out.push(format!(
                            "{p} AS (SELECT {} FROM {from} WHERE TRUE{}{}{})",
                            cols.join(", "),
                            filter
                                .as_deref()
                                .map(|f| format!(" AND {f}"))
                                .unwrap_or_default(),
                            take.as_deref()
                                .map(|f| format!(" AND ({f})"))
                                .unwrap_or_default(),
                            self.key_pred(None)
                        ));
                        out.push(format!("{l} AS ({})", self.overlay(&p, lv.space, &lv.name)));
                    }
                }
            } else {
                let p = format!("`__sprig_p{i}`");
                let e = lv.edge.clone().unwrap_or_default();
                out.push(format!("{p} AS ({})", self.parent_rows(&prev, &e)));
                out.push(format!("{l} AS ({})", self.overlay(&p, lv.space, &lv.name)));
            }
            prev = l;
        }
        (out, prev)
    }

    fn select_list(&self) -> String {
        let mut cols = self.o.columns.iter().map(|c| q(c)).collect::<Vec<_>>();
        if self.origin {
            cols.push(ORIGIN.into());
        }
        for (c, e) in &self.derived {
            cols.push(format!("{e} AS {}", q(c)));
        }
        cols.join(", ")
    }

    /// The names of [`ViewSql::view`]'s columns, quoted and listed (a
    /// query over the view names them rather than `*`: chapter 29's swap
    /// refuses `*` beside an all-columns policy view).
    #[must_use]
    pub fn output_columns(&self) -> String {
        let mut cols = self.o.columns.iter().map(|c| q(c)).collect::<Vec<_>>();
        if self.origin {
            cols.push(ORIGIN.into());
        }
        for (c, _) in &self.derived {
            cols.push(q(c));
        }
        cols.join(", ")
    }

    /// The merged view of `chain`'s last space, as a query.
    #[must_use]
    pub fn view(&self, chain: &Chain) -> String {
        if chain.is_base() && !self.origin {
            return format!(
                "SELECT {} FROM {} {C} WHERE `_sprig_space` = {} AND `_sprig_state` IN ('own', \
                 'orphan'){}",
                self.select_list(),
                self.layer,
                chain.levels[0].space.0,
                self.key_pred(None)
            );
        }
        let (ctes, last) = self.ctes(chain);
        format!(
            "WITH {} SELECT {} FROM {last}",
            ctes.join(", "),
            self.select_list()
        )
    }

    /// The rows the last space inherits (its parent's view through the
    /// edge, or the upstream share), as a query; `None` without upstream.
    #[must_use]
    pub fn inherited(&self, chain: &Chain) -> Option<String> {
        let n = chain.levels.len();
        if n == 1 {
            return match &chain.top {
                Top::Base => None,
                Top::Upstream { .. } => {
                    let (ctes, _) = self.ctes(chain);
                    // The first CTE is the upstream rows.
                    Some(format!(
                        "WITH {} SELECT {} FROM `__sprig_p0`",
                        ctes[0],
                        self.select_list()
                    ))
                }
            };
        }
        let (mut ctes, _) = self.ctes(chain);
        // Drop the last level's overlay; keep its parent rows.
        ctes.pop();
        Some(format!(
            "WITH {} SELECT {} FROM `__sprig_p{}`",
            ctes.join(", "),
            self.select_list(),
            n - 1
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::fixtures;

    fn chain() -> Chain {
        Chain {
            top: Top::Base,
            levels: vec![
                Level {
                    space: SpaceId(1),
                    name: "platform".into(),
                    edge: None,
                },
                Level {
                    space: SpaceId(7),
                    name: "acme".into(),
                    edge: Some(Edge {
                        filter: Some("region = 'IN'".into()),
                        ..Edge::default()
                    }),
                },
                Level {
                    space: SpaceId(12),
                    name: "pune".into(),
                    edge: Some(Edge::default()),
                },
            ],
        }
    }

    #[test]
    fn three_levels_golden() {
        let o = fixtures::products();
        let v = ViewSql {
            o: &o,
            layer: "`products`".into(),
            keys: None,
            origin: false,
            derived: Vec::new(),
        };
        let sql = v.view(&chain());
        // Every layer reference is under LAYER_ALIAS (the chapter's `c`).
        let golden = |s: &str| {
            s.replace("c.`", "`__sprig_lyr`.`")
                .replace("`products` c ", "`products` `__sprig_lyr` ")
                .replace(
                    "FROM `products` WHERE `_sprig_space` = 1",
                    "FROM `products` `__sprig_lyr` WHERE `_sprig_space` = 1",
                )
        };
        assert_eq!(
            sql,
            golden(
                "WITH `__sprig_l0` AS (SELECT `id`, `name`, `colour`, `price`, `region` FROM \
             `products` WHERE `_sprig_space` = 1 AND `_sprig_state` IN ('own', 'orphan')), \
             `__sprig_p1` AS (SELECT `id`, `name`, `colour`, `price`, `region` FROM \
             `__sprig_l0` WHERE TRUE AND (region = 'IN')), `__sprig_l1` AS (SELECT p.`id`, \
             IF(c.`_sprig_ovr` & 1, c.`name`, p.`name`) AS `name`, IF(c.`_sprig_ovr` & 2, \
             c.`colour`, p.`colour`) AS `colour`, IF(c.`_sprig_ovr` & 4, c.`price`, \
             p.`price`) AS `price`, IF(c.`_sprig_ovr` & 8, c.`region`, p.`region`) AS \
             `region` FROM `__sprig_p1` p LEFT JOIN `products` c ON c.`id` = p.`id` AND \
             c.`_sprig_space` = 7 WHERE c.`_sprig_state` IS NULL OR c.`_sprig_state` <> 'hide' \
             UNION ALL SELECT c.`id`, c.`name`, c.`colour`, c.`price`, c.`region` FROM \
             `products` c WHERE c.`_sprig_space` = 7 AND c.`_sprig_state` IN ('own', 'orphan') \
             AND NOT EXISTS (SELECT 1 FROM `__sprig_p1` p WHERE c.`id` = p.`id`)), \
             `__sprig_p2` AS (SELECT `id`, `name`, `colour`, `price`, `region` FROM \
             `__sprig_l1` WHERE TRUE), `__sprig_l2` AS (SELECT p.`id`, IF(c.`_sprig_ovr` & 1, \
             c.`name`, p.`name`) AS `name`, IF(c.`_sprig_ovr` & 2, c.`colour`, p.`colour`) AS \
             `colour`, IF(c.`_sprig_ovr` & 4, c.`price`, p.`price`) AS `price`, \
             IF(c.`_sprig_ovr` & 8, c.`region`, p.`region`) AS `region` FROM `__sprig_p2` p \
             LEFT JOIN `products` c ON c.`id` = p.`id` AND c.`_sprig_space` = 12 WHERE \
             c.`_sprig_state` IS NULL OR c.`_sprig_state` <> 'hide' UNION ALL SELECT c.`id`, \
             c.`name`, c.`colour`, c.`price`, c.`region` FROM `products` c WHERE \
             c.`_sprig_space` = 12 AND c.`_sprig_state` IN ('own', 'orphan') AND NOT EXISTS \
             (SELECT 1 FROM `__sprig_p2` p WHERE c.`id` = p.`id`)) SELECT `id`, `name`, \
             `colour`, `price`, `region` FROM `__sprig_l2`"
            )
        );
    }

    #[test]
    fn keys_push_down_and_base_is_flat() {
        let o = fixtures::products();
        let v = ViewSql {
            o: &o,
            layer: "`shop`.`products`".into(),
            keys: Some(vec!["2".into(), "5".into()]),
            origin: false,
            derived: Vec::new(),
        };
        let base = v.view(&chain().upto(1));
        assert_eq!(
            base,
            "SELECT `id`, `name`, `colour`, `price`, `region` FROM `shop`.`products` \
             `__sprig_lyr` WHERE `_sprig_space` = 1 AND `_sprig_state` IN ('own', 'orphan') AND \
             `id` IN (2, 5)"
        );
        let deep = v.view(&chain());
        assert_eq!(deep.matches("IN (2, 5)").count(), 3);
        let inh = v.inherited(&chain()).unwrap();
        assert!(inh.ends_with("FROM `__sprig_p2`"), "{inh}");
        assert!(v.inherited(&chain().upto(1)).is_none());
    }

    #[test]
    fn upstream_and_projection() {
        let o = fixtures::products();
        let v = ViewSql {
            o: &o,
            layer: "`products`".into(),
            keys: None,
            origin: true,
            derived: Vec::new(),
        };
        let c = Chain {
            top: Top::Upstream {
                from: "`__sprig_share_4`".into(),
                columns: vec!["id".into(), "name".into(), "price".into()],
                take: Some("price < 100".into()),
                origin: "r6-inh-a".into(),
                filter: None,
            },
            levels: vec![
                Level {
                    space: SpaceId(1),
                    name: "b".into(),
                    edge: None,
                },
                Level {
                    space: SpaceId(2),
                    name: "b1".into(),
                    edge: Some(Edge {
                        columns: Some(vec!["name".into()]),
                        ..Edge::default()
                    }),
                },
            ],
        };
        let sql = v.view(&c);
        assert!(sql.contains(
            "`__sprig_p0` AS (SELECT `id`, `name`, NULL AS `colour`, `price`, NULL AS `region`"
        ));
        assert!(sql.contains("FROM `__sprig_share_4` WHERE TRUE AND (price < 100))"));
        assert!(
            sql.contains("`__sprig_p1` AS (SELECT `id`, `name`, NULL AS `colour`, NULL AS `price`")
        );
        assert!(sql.contains("JSON_VALUE(p.`_sprig_o`, '$.\"colour\"')"));
        assert!(sql.ends_with("FROM `__sprig_l1`"));
    }
}

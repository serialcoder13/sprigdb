//! MySQL's geographic semantics for SRID 4326 on the MySQL persona
//! (chapter 28, "Geographic SRIDs, axis order"; chapter 36, "MySQL
//! persona rewrite"; R4).
//!
//! MySQL 8.4 reads and writes SRID 4326 coordinates latitude first and
//! measures on the WGS 84 ellipsoid; MariaDB is planar and longitude
//! first. Both store longitude in X, so stored bytes (and dumps) agree and
//! only the functions differ. Each call is rewritten in place, inner calls
//! first:
//!
//! | MySQL 8.4 | Sent to MariaDB |
//! |---|---|
//! | `ST_GeomFromText('POINT(lat lon)', 4326)` and the other text constructors | the literal's axes swapped in the gateway; a non-literal swapped with `REGEXP_REPLACE`; `'axis-order=long-lat'` keeps the order |
//! | `ST_GeomFromWKB(w, 4326)` and the other WKB constructors | `ST_GeomFromText(<swapped text of w>, 4326)` |
//! | `ST_X(g)`, `ST_Y(g)` | `ST_Y(g)`, `ST_X(g)` when `g` is SRID 4326 |
//! | `ST_Latitude(g)`, `ST_Longitude(g)` (missing in MariaDB) | `ST_Y(g)`, `ST_X(g)` |
//! | `ST_AsText(g)`, `ST_AsWKT(g)`, `ST_AsBinary(g)`, `ST_AsWKB(g)` | latitude first when `g` is SRID 4326 |
//! | `ST_Distance(a, b)` of two SRID 4326 points | the Andoyer distance as SQL ([`sprig_geo::mysql::andoyer_sql`]): MySQL's numbers to the last digit |
//! | `ST_Distance_Sphere(a, b)` of SRID 4326 points | haversine on the WGS 84 mean radius, as MySQL |
//!
//! Whether an argument is SRID 4326 is known when it is a constructor with
//! the literal SRID 4326; otherwise the SQL tests `ST_SRID(g) = 4326` at
//! run time, which repeats the argument. An argument holding a parameter
//! (`?`) can't be repeated: a distance over one fails with 39767
//! `ER_SPRIG_GEO_UNSUPPORTED_EXPRESSION` when the SRID is 4326, and the
//! other functions are left to MariaDB when the SRID isn't known.
//!
//! Not covered (listed in `decisions/mysql-persona-gaps.md`): distances
//! involving lines and polygons (MariaDB's planar answer, in degrees),
//! geodesic `ST_Contains` and friends, latitude range checks (3617), SRID
//! enforcement on columns (3643), and `ST_AsGeoJSON`'s `20.0` (MariaDB
//! prints `20`).

use super::errmap::WireError;
use crate::lex::{self, LexOptions, Token, TokenKind};

const FROM_TEXT: &[&str] = &[
    "ST_GEOMFROMTEXT",
    "ST_GEOMETRYFROMTEXT",
    "ST_POINTFROMTEXT",
    "ST_LINEFROMTEXT",
    "ST_LINESTRINGFROMTEXT",
    "ST_POLYFROMTEXT",
    "ST_POLYGONFROMTEXT",
    "ST_MPOINTFROMTEXT",
    "ST_MULTIPOINTFROMTEXT",
    "ST_MLINEFROMTEXT",
    "ST_MULTILINESTRINGFROMTEXT",
    "ST_MPOLYFROMTEXT",
    "ST_MULTIPOLYGONFROMTEXT",
    "ST_GEOMCOLLFROMTEXT",
    "ST_GEOMCOLLFROMTXT",
    "ST_GEOMETRYCOLLECTIONFROMTEXT",
];

const FROM_WKB: &[&str] = &[
    "ST_GEOMFROMWKB",
    "ST_GEOMETRYFROMWKB",
    "ST_POINTFROMWKB",
    "ST_LINEFROMWKB",
    "ST_LINESTRINGFROMWKB",
    "ST_POLYFROMWKB",
    "ST_POLYGONFROMWKB",
    "ST_MPOINTFROMWKB",
    "ST_MULTIPOINTFROMWKB",
    "ST_MLINEFROMWKB",
    "ST_MULTILINESTRINGFROMWKB",
    "ST_MPOLYFROMWKB",
    "ST_MULTIPOLYGONFROMWKB",
    "ST_GEOMCOLLFROMWKB",
    "ST_GEOMETRYCOLLECTIONFROMWKB",
];

const OTHERS: &[&str] = &[
    "ST_X",
    "ST_Y",
    "ST_LATITUDE",
    "ST_LONGITUDE",
    "ST_ASTEXT",
    "ST_ASWKT",
    "ST_ASBINARY",
    "ST_ASWKB",
    "ST_DISTANCE",
    "ST_DISTANCE_SPHERE",
];

/// The regular expression that swaps the two numbers of every coordinate
/// pair in WKT text, and its replacement.
const PAIR: &str =
    r"([-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?)\s+([-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?)";
const SWAPPED: &str = r"\2 \1";

/// A rewritten expression.
#[derive(Debug, Clone)]
struct Expr {
    text: String,
    /// Known to be SRID 4326 (a constructor with the literal SRID).
    geo: bool,
    /// Holds a `?`.
    param: bool,
}

struct Cx<'a> {
    sql: &'a [u8],
    t: Vec<Token>,
    nbe: bool,
}

impl Cx<'_> {
    fn bytes(&self, i: usize) -> &[u8] {
        self.t[i].bytes(self.sql)
    }

    fn punct(&self, i: usize, c: &[u8]) -> bool {
        self.t
            .get(i)
            .is_some_and(|t| t.kind == TokenKind::Punct && t.bytes(self.sql) == c)
    }

    fn lit(&self, s: &str) -> String {
        crate::names::rewrite::sql_string(s, self.nbe)
    }

    /// The index of the `)` closing the `(` at `open`.
    fn close(&self, open: usize) -> Option<usize> {
        let mut depth = 0;
        for i in open..self.t.len() {
            if self.punct(i, b"(") {
                depth += 1;
            } else if self.punct(i, b")") {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
        }
        None
    }

    /// Rewrites tokens `from..to` (all tokens, trivia included), copying
    /// what it doesn't change.
    fn range(&self, from: usize, to: usize) -> Result<Expr, WireError> {
        let mut out = String::new();
        let mut param = false;
        let mut geo = false;
        let mut i = from;
        let mut only_call = true;
        let mut seen = 0;
        while i < to {
            let tk = self.t[i];
            if !tk.kind.is_trivia() {
                seen += 1;
            }
            if tk.kind == TokenKind::Param {
                param = true;
            }
            if tk.kind == TokenKind::Word {
                let name = String::from_utf8_lossy(self.bytes(i)).to_ascii_uppercase();
                let next = (i + 1..to).find(|&j| !self.t[j].kind.is_trivia());
                let is_target = FROM_TEXT.contains(&name.as_str())
                    || FROM_WKB.contains(&name.as_str())
                    || OTHERS.contains(&name.as_str());
                let after_dot = i > 0 && self.punct(i - 1, b".");
                if is_target
                    && !after_dot
                    && let Some(open) = next.filter(|&j| self.punct(j, b"("))
                    && let Some(close) = self.close(open).filter(|&c| c < to)
                {
                    let args = self.args(open, close)?;
                    let original = String::from_utf8_lossy(
                        &self.sql[tk.start as usize..self.t[close].end as usize],
                    )
                    .into_owned();
                    let e = self.call(&name, &original, &args)?;
                    param |= e.param;
                    geo = e.geo;
                    out.push_str(&e.text);
                    i = close + 1;
                    // Trailing trivia may follow.
                    continue;
                }
            }
            if !tk.kind.is_trivia() {
                only_call = false;
            }
            out.push_str(&String::from_utf8_lossy(tk.bytes(self.sql)));
            i += 1;
        }
        Ok(Expr {
            text: out,
            geo: geo && only_call && seen > 0,
            param,
        })
    }

    /// The arguments of the call whose parentheses are at `open..=close`.
    fn args(&self, open: usize, close: usize) -> Result<Vec<Expr>, WireError> {
        let mut out = Vec::new();
        let mut depth = 0;
        let mut start = open + 1;
        for i in open + 1..close {
            if self.punct(i, b"(") {
                depth += 1;
            } else if self.punct(i, b")") {
                depth -= 1;
            } else if depth == 0 && self.punct(i, b",") {
                out.push(self.range(start, i)?);
                start = i + 1;
            }
        }
        if start < close || !out.is_empty() {
            out.push(self.range(start, close)?);
        }
        Ok(out)
    }

    /// The literal string an argument is, if it is one.
    fn string_arg(&self, e: &Expr) -> Option<String> {
        let o = LexOptions::mysql84().no_backslash_escapes(self.nbe);
        let toks = lex::lex(e.text.as_bytes(), &o).ok()?;
        let sig: Vec<&Token> = toks.iter().filter(|t| !t.kind.is_trivia()).collect();
        if sig.len() != 1 || sig[0].kind != TokenKind::String {
            return None;
        }
        let v = lex::string_value(sig[0].bytes(e.text.as_bytes()), self.nbe)?;
        String::from_utf8(v).ok()
    }

    fn swap_sql(&self, wkt_sql: &str) -> String {
        format!(
            "REGEXP_REPLACE({wkt_sql}, {}, {})",
            self.lit(PAIR),
            self.lit(SWAPPED)
        )
    }

    /// A runtime SRID test, or `true` when known.
    fn is_geo(e: &Expr) -> String {
        if e.geo {
            "TRUE".to_owned()
        } else {
            format!("ST_SRID({}) = 4326", e.text.trim())
        }
    }

    #[allow(clippy::too_many_lines)] // one arm per function family
    fn call(&self, name: &str, original: &str, args: &[Expr]) -> Result<Expr, WireError> {
        let param = args.iter().any(|a| a.param);
        let keep = |text: String| Expr {
            text,
            geo: false,
            param,
        };
        // The call with its (rewritten) arguments, under its own name.
        let fname = &original[..original.find('(').unwrap_or(original.len())];
        let plain = |args: &[Expr]| {
            format!(
                "{}({})",
                fname.trim_end(),
                args.iter()
                    .map(|a| a.text.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            )
        };
        let srid_4326 = |a: Option<&Expr>| a.is_some_and(|a| a.text.trim() == "4326");
        let long_lat = |a: Option<&Expr>| {
            a.and_then(|a| self.string_arg(a)).is_some_and(|o| {
                o.replace(' ', "")
                    .to_ascii_lowercase()
                    .contains("axis-order=long-lat")
            })
        };
        if FROM_TEXT.contains(&name) {
            if !srid_4326(args.get(1)) {
                return Ok(keep(plain(args)));
            }
            let wkt = if long_lat(args.get(2)) {
                args[0].text.clone()
            } else if let Some(s) = self.string_arg(&args[0]) {
                match sprig_geo::mysql::swap_wkt(&s) {
                    Some(w) => self.lit(&w),
                    None => args[0].text.clone(),
                }
            } else {
                self.swap_sql(&args[0].text)
            };
            return Ok(Expr {
                text: format!("{}({wkt}, 4326)", fname.trim_end()),
                geo: true,
                param,
            });
        }
        if FROM_WKB.contains(&name) {
            if !srid_4326(args.get(1)) {
                return Ok(keep(plain(args)));
            }
            let inner = format!("ST_AsText({}({}))", fname.trim_end(), args[0].text);
            let wkt = if long_lat(args.get(2)) {
                inner
            } else {
                self.swap_sql(&inner)
            };
            return Ok(Expr {
                text: format!("ST_GeomFromText({wkt}, 4326)"),
                geo: true,
                param,
            });
        }
        let g = args.first();
        match name {
            "ST_LATITUDE" | "ST_LONGITUDE" if args.len() == 1 => {
                let f = if name == "ST_LATITUDE" {
                    "ST_Y"
                } else {
                    "ST_X"
                };
                Ok(keep(format!("{f}({})", args[0].text)))
            }
            "ST_X" | "ST_Y" if args.len() == 1 => {
                let g = &args[0];
                let other = if name == "ST_X" { "ST_Y" } else { "ST_X" };
                if g.geo {
                    Ok(keep(format!("{other}({})", g.text)))
                } else if g.param {
                    Ok(keep(plain(args)))
                } else {
                    Ok(keep(format!(
                        "IF({}, {other}({}), {}({}))",
                        Self::is_geo(g),
                        g.text,
                        &name[..4],
                        g.text
                    )))
                }
            }
            "ST_ASTEXT" | "ST_ASWKT" | "ST_ASBINARY" | "ST_ASWKB" if !args.is_empty() => {
                let g = &args[0];
                if long_lat(args.get(1)) && g.geo {
                    return Ok(keep(format!("{}({})", fname.trim_end(), g.text)));
                }
                let text = matches!(name, "ST_ASTEXT" | "ST_ASWKT");
                let swapped = {
                    let s = self.swap_sql(&format!("ST_AsText({})", g.text));
                    if text {
                        s
                    } else {
                        format!("ST_AsBinary(ST_GeomFromText({s}))")
                    }
                };
                let own = format!(
                    "{}({})",
                    if text { "ST_AsText" } else { "ST_AsBinary" },
                    g.text
                );
                if g.geo {
                    Ok(keep(swapped))
                } else if g.param {
                    Ok(keep(own))
                } else {
                    Ok(keep(format!("IF({}, {swapped}, {own})", Self::is_geo(g))))
                }
            }
            "ST_DISTANCE" if args.len() == 2 => {
                let (a, b) = (&args[0], &args[1]);
                if a.param || b.param {
                    if a.geo || b.geo {
                        return Err(WireError::new(
                            39767,
                            *b"HY000",
                            "ER_SPRIG_GEO_UNSUPPORTED_EXPRESSION: ST_Distance on SRID 4326 \
                             with a parameter inside an argument; bind the point to a user \
                             variable or a literal",
                        ));
                    }
                    return Ok(keep(plain(args)));
                }
                let (at, bt) = (a.text.trim(), b.text.trim());
                let geodesic = sprig_geo::mysql::andoyer_sql(
                    &format!("ST_Y({at})"),
                    &format!("ST_X({at})"),
                    &format!("ST_Y({bt})"),
                    &format!("ST_X({bt})"),
                );
                let test = format!(
                    "{} AND ST_GeometryType({at}) = 'POINT' AND ST_GeometryType({bt}) = 'POINT'",
                    if a.geo {
                        Self::is_geo(b)
                    } else {
                        Self::is_geo(a)
                    }
                );
                Ok(keep(format!(
                    "IF({test}, {geodesic}, ST_Distance({at}, {bt}))"
                )))
            }
            "ST_DISTANCE_SPHERE" if args.len() == 2 || args.len() == 3 => {
                let (a, b) = (&args[0], &args[1]);
                if a.param || b.param {
                    if a.geo || b.geo {
                        return Err(WireError::new(
                            39767,
                            *b"HY000",
                            "ER_SPRIG_GEO_UNSUPPORTED_EXPRESSION: ST_Distance_Sphere on SRID \
                             4326 with a parameter inside an argument",
                        ));
                    }
                    return Ok(keep(plain(args)));
                }
                let (at, bt) = (a.text.trim(), b.text.trim());
                let radius = args.get(2).map_or_else(
                    || format!("{:e}", sprig_geo::mysql::wgs84_mean_radius()),
                    |r| r.text.clone(),
                );
                let sphere = sprig_geo::mysql::sphere_sql(
                    &format!("ST_Y({at})"),
                    &format!("ST_X({at})"),
                    &format!("ST_Y({bt})"),
                    &format!("ST_X({bt})"),
                    &radius,
                );
                let test = if a.geo {
                    Self::is_geo(b)
                } else {
                    Self::is_geo(a)
                };
                Ok(keep(format!("IF({test}, {sphere}, {})", plain(args))))
            }
            _ => {
                let _ = g;
                Ok(keep(plain(args)))
            }
        }
    }
}

/// A distance predicate the box pass (chapter 36, step 4 of
/// `rewrite_geo`, R6 round 3) narrows: `ST_Distance(<column>,
/// <constant SRID 4326 point>) < r` (or `<=`, the arguments either way
/// round) gets `MBRIntersects(<column>, <box>) AND` in front, so MariaDB
/// can read the column's `SPATIAL INDEX` for the candidates; the exact
/// Andoyer test still decides. The box ([`sprig_geo::mysql::grow`]) holds
/// every point within `r` metres; a box crossing a pole or ±180° adds
/// nothing.
fn boxes(sql: &[u8], opts: &LexOptions) -> Option<Vec<u8>> {
    let toks = lex::lex(sql, opts).ok()?;
    let sig: Vec<usize> = (0..toks.len())
        .filter(|&i| !toks[i].kind.is_trivia())
        .collect();
    let text = |i: usize| toks[sig[i]].bytes(sql);
    let is = |i: usize, k: TokenKind, b: &[u8]| {
        sig.get(i)
            .is_some_and(|&j| toks[j].kind == k && toks[j].bytes(sql).eq_ignore_ascii_case(b))
    };
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut i = 0;
    while i < sig.len() {
        if !(is(i, TokenKind::Word, b"ST_DISTANCE") && is(i + 1, TokenKind::Punct, b"(")) {
            i += 1;
            continue;
        }
        if i > 0 && is(i - 1, TokenKind::Punct, b".") {
            i += 1;
            continue;
        }
        // The call's closing parenthesis and its two arguments.
        let mut depth = 0;
        let mut close = None;
        let mut comma = None;
        for j in i + 1..sig.len() {
            if is(j, TokenKind::Punct, b"(") {
                depth += 1;
            } else if is(j, TokenKind::Punct, b")") {
                depth -= 1;
                if depth == 0 {
                    close = Some(j);
                    break;
                }
            } else if depth == 1 && is(j, TokenKind::Punct, b",") {
                if comma.is_some() {
                    comma = None;
                    close = None;
                    break;
                }
                comma = Some(j);
            }
        }
        let (Some(close), Some(comma)) = (close, comma) else {
            i += 1;
            continue;
        };
        let column = |from: usize, to: usize| -> Option<String> {
            // `c`, `t.c` or `d.t.c`, plain or quoted.
            let n = to - from;
            if n == 0 || n > 5 || n % 2 == 0 {
                return None;
            }
            let mut out = String::new();
            for j in from..to {
                let k = toks[sig[j]].kind;
                let want_name = (j - from) % 2 == 0;
                if want_name && !matches!(k, TokenKind::Word | TokenKind::QuotedIdent) {
                    return None;
                }
                if !want_name && !is(j, TokenKind::Punct, b".") {
                    return None;
                }
                out.push_str(&String::from_utf8_lossy(text(j)));
            }
            Some(out)
        };
        let point = |from: usize, to: usize| -> Option<(f64, f64)> {
            // `ST_GeomFromText('POINT(lat lon)', 4326)` or ST_PointFromText.
            if to - from < 6 {
                return None;
            }
            let name = String::from_utf8_lossy(text(from)).to_ascii_uppercase();
            if !matches!(name.as_str(), "ST_GEOMFROMTEXT" | "ST_GEOMETRYFROMTEXT" | "ST_POINTFROMTEXT")
                || !is(from + 1, TokenKind::Punct, b"(")
                || toks[sig[from + 2]].kind != TokenKind::String
                || !is(from + 3, TokenKind::Punct, b",")
                || !is(from + 4, TokenKind::Number, b"4326")
            {
                return None;
            }
            let long_lat = if is(from + 5, TokenKind::Punct, b")") && from + 6 == to {
                false
            } else if is(from + 5, TokenKind::Punct, b",")
                && toks[sig[from + 6]].kind == TokenKind::String
                && is(from + 7, TokenKind::Punct, b")")
                && from + 8 == to
            {
                let o = lex::string_value(text(from + 6), opts.no_backslash_escapes)?;
                String::from_utf8_lossy(&o)
                    .replace(' ', "")
                    .to_ascii_lowercase()
                    .contains("axis-order=long-lat")
            } else {
                return None;
            };
            let w = lex::string_value(text(from + 2), opts.no_backslash_escapes)?;
            let w = String::from_utf8(w).ok()?;
            let up = w.trim().to_ascii_uppercase();
            let inner = up.strip_prefix("POINT")?.trim().strip_prefix('(')?.strip_suffix(')')?;
            let mut nums = inner.split_whitespace().map(str::parse::<f64>);
            let (a, b) = (nums.next()?.ok()?, nums.next()?.ok()?);
            if nums.next().is_some() {
                return None;
            }
            Some(if long_lat { (b, a) } else { (a, b) })
        };
        let (a0, a1, b0, b1) = (i + 2, comma, comma + 1, close);
        let found = match (column(a0, a1), point(b0, b1)) {
            (Some(c), Some(p)) => Some((c, p)),
            _ => match (column(b0, b1), point(a0, a1)) {
                (Some(c), Some(p)) => Some((c, p)),
                _ => None,
            },
        };
        let op_ok = is(close + 1, TokenKind::Op, b"<") || is(close + 1, TokenKind::Op, b"<=");
        let r = sig
            .get(close + 2)
            .filter(|&&j| toks[j].kind == TokenKind::Number)
            .and_then(|_| String::from_utf8_lossy(text(close + 2)).parse::<f64>().ok());
        if let (Some((col, (lat, lon))), true, Some(r)) = (found, op_ok, r)
            && r.is_finite()
            && r >= 0.0
            && let Some((s, w, n, e)) = sprig_geo::mysql::grow(lat, lon, r)
        {
            let start = toks[sig[i]].start as usize;
            let end = toks[sig[close + 2]].end as usize;
            let original = String::from_utf8_lossy(&sql[start..end]).into_owned();
            let poly = format!("POLYGON(({w:e} {s:e},{e:e} {s:e},{e:e} {n:e},{w:e} {n:e},{w:e} {s:e}))");
            edits.push((
                start,
                end,
                format!(
                    "(MBRIntersects({col}, ST_GeomFromText('{poly}', 4326, 'axis-order=long-lat')) AND {original})"
                ),
            ));
            i = close + 3;
            continue;
        }
        i += 1;
    }
    if edits.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(sql.len() + 128 * edits.len());
    let mut at = 0;
    for (s, e, t) in edits {
        out.extend_from_slice(&sql[at..s]);
        out.extend_from_slice(t.as_bytes());
        at = e;
    }
    out.extend_from_slice(&sql[at..]);
    Some(out)
}

/// A nearest-neighbour query on the MySQL persona (chapter 36,
/// `nearest()`; R6 round 3): `SELECT ... FROM t [alias] [WHERE w] ORDER
/// BY ST_Distance(<column>, <constant SRID 4326 point>) [ASC] LIMIT k`
/// on one table. The gateway runs it with a growing radius `r`
/// ([`Knn::with_radius`] adds `ST_Distance(...) <= r`, which the box pass
/// turns into an R-tree read), counting matches until `k` are within `r`.
#[derive(Debug, Clone, PartialEq)]
pub struct Knn {
    /// `k`.
    pub k: u64,
    /// The `ST_Distance(...)` call as written.
    call: String,
    /// The `WHERE` condition's byte range, when there is one.
    cond: Option<(usize, usize)>,
    /// Where `ORDER` starts.
    order: usize,
}

/// The marker of statements the nearest-neighbour search built (never
/// searched again).
pub const KNN_MARK: &str = "/*sprig:knn*/ ";

impl Knn {
    /// The statement with `ST_Distance(...) <= r` added to its condition.
    #[must_use]
    pub fn with_radius(&self, sql: &[u8], r: f64) -> Vec<u8> {
        let pred = format!("{} <= {r:e}", self.call);
        let mut out = KNN_MARK.as_bytes().to_vec();
        match self.cond {
            Some((a, b)) => {
                out.extend_from_slice(&sql[..a]);
                out.extend_from_slice(b" (");
                out.extend_from_slice(&sql[a..b]);
                out.extend_from_slice(format!(") AND {pred} ").as_bytes());
                out.extend_from_slice(&sql[b..]);
            }
            None => {
                out.extend_from_slice(&sql[..self.order]);
                out.extend_from_slice(format!("WHERE {pred} ").as_bytes());
                out.extend_from_slice(&sql[self.order..]);
            }
        }
        out
    }

    /// A statement counting [`Self::with_radius`]'s rows.
    #[must_use]
    pub fn probe(&self, sql: &[u8], r: f64) -> Vec<u8> {
        let inner = self.with_radius(sql, r);
        let mut out = KNN_MARK.as_bytes().to_vec();
        out.extend_from_slice(b"SELECT COUNT(*) FROM (");
        out.extend_from_slice(inner.strip_suffix(b";").unwrap_or(&inner));
        out.extend_from_slice(b") AS sprig_knn_probe");
        out
    }
}

/// The nearest-neighbour shape of `sql`, when it has it (see [`Knn`]).
#[must_use]
pub fn knn(sql: &[u8], opts: &LexOptions) -> Option<Knn> {
    if sql.starts_with(KNN_MARK.as_bytes()) || !sql.windows(11).any(|w| w.eq_ignore_ascii_case(b"st_distance")) {
        return None;
    }
    let toks = lex::lex(sql, opts).ok()?;
    let sig: Vec<usize> = (0..toks.len()).filter(|&i| !toks[i].kind.is_trivia()).collect();
    let word = |i: usize, w: &[u8]| {
        sig.get(i).is_some_and(|&j| {
            toks[j].kind == TokenKind::Word && toks[j].bytes(sql).eq_ignore_ascii_case(w)
        })
    };
    let punct = |i: usize, c: &[u8]| {
        sig.get(i)
            .is_some_and(|&j| toks[j].kind == TokenKind::Punct && toks[j].bytes(sql) == c)
    };
    if !word(0, b"SELECT") {
        return None;
    }
    // Top-level FROM, WHERE and ORDER; anything else refuses the shape.
    let (mut from, mut wh, mut order) = (None, None, None);
    let mut depth = 0i32;
    for i in 0..sig.len() {
        if punct(i, b"(") {
            depth += 1;
            continue;
        }
        if punct(i, b")") {
            depth -= 1;
            continue;
        }
        if depth != 0 {
            continue;
        }
        if punct(i, b",") && from.is_some() && order.is_none() {
            return None;
        }
        for w in [
            b"JOIN".as_slice(), b"UNION", b"GROUP", b"HAVING", b"INTO", b"FOR", b"LOCK",
            b"EXCEPT", b"INTERSECT", b"WINDOW", b"OFFSET",
        ] {
            if word(i, w) {
                return None;
            }
        }
        if word(i, b"FROM") && from.is_none() {
            from = Some(i);
        } else if word(i, b"WHERE") && wh.is_none() && from.is_some() {
            wh = Some(i);
        } else if word(i, b"ORDER") && order.is_none() {
            order = Some(i);
        }
    }
    let (_, order) = (from?, order?);
    if !word(order + 1, b"BY") || !word(order + 2, b"ST_DISTANCE") || !punct(order + 3, b"(") {
        return None;
    }
    let mut d = 0;
    let mut close = None;
    let mut comma = None;
    for j in order + 3..sig.len() {
        if punct(j, b"(") {
            d += 1;
        } else if punct(j, b")") {
            d -= 1;
            if d == 0 {
                close = Some(j);
                break;
            }
        } else if d == 1 && punct(j, b",") {
            comma = Some(j);
        }
    }
    let (close, comma) = (close?, comma?);
    // One argument a column, the other a constant point: what the box
    // pass narrows (checked by running it on a probe predicate).
    let call = String::from_utf8_lossy(&sql[toks[sig[order + 2]].start as usize..toks[sig[close]].end as usize]).into_owned();
    let _ = comma;
    let test = format!("SELECT 1 FROM t WHERE {call} < 1000");
    boxes(test.as_bytes(), opts)?;
    let mut i = close + 1;
    if word(i, b"ASC") {
        i += 1;
    }
    if !word(i, b"LIMIT") {
        return None;
    }
    let lim = sig.get(i + 1).filter(|&&j| toks[j].kind == TokenKind::Number)?;
    let k: u64 = String::from_utf8_lossy(toks[*lim].bytes(sql)).parse().ok()?;
    let rest = &sig[i + 2..];
    if !(rest.is_empty() || (rest.len() == 1 && punct(i + 2, b";"))) || k == 0 {
        return None;
    }
    let order_start = toks[sig[order]].start as usize;
    let cond = wh.map(|w| (toks[sig[w]].end as usize, order_start));
    Some(Knn {
        k,
        call,
        cond,
        order: order_start,
    })
}

/// The MySQL persona's geographic rewrite of one statement (see the
/// module documentation). `None` when nothing changes.
///
/// # Errors
/// 39767 for a geodesic distance over a parameter.
#[must_use]
pub fn rewrite(sql: &[u8], opts: &LexOptions) -> Option<Result<Vec<u8>, WireError>> {
    if !sql.windows(3).any(|w| w.eq_ignore_ascii_case(b"st_")) {
        return None;
    }
    // Chapter 36 (R6 round 3): candidate boxes for distance predicates.
    let boxed = boxes(sql, opts);
    let original = sql;
    let sql: &[u8] = boxed.as_deref().unwrap_or(sql);
    let tokens = lex::lex(sql, opts).ok()?;
    let interesting = tokens.iter().any(|t| {
        t.kind == TokenKind::Word && {
            let n = String::from_utf8_lossy(t.bytes(sql)).to_ascii_uppercase();
            FROM_TEXT.contains(&n.as_str())
                || FROM_WKB.contains(&n.as_str())
                || OTHERS.contains(&n.as_str())
        }
    });
    if !interesting {
        return None;
    }
    let cx = Cx {
        sql,
        t: tokens,
        nbe: opts.no_backslash_escapes,
    };
    match cx.range(0, cx.t.len()) {
        Ok(e) => {
            let out = e.text.into_bytes();
            (out != original).then_some(Ok(out))
        }
        Err(e) => Some(Err(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rw(s: &str) -> String {
        match rewrite(s.as_bytes(), &LexOptions::mysql84()) {
            Some(Ok(v)) => String::from_utf8(v).unwrap(),
            None => s.to_owned(),
            Some(Err(e)) => format!("ERR {}", e.code),
        }
    }

    #[test]
    fn nearest_shapes() {
        let o = LexOptions::mysql84();
        let q = "SELECT id FROM place p WHERE kind = 'a' ORDER BY ST_Distance(p.loc, ST_GeomFromText('POINT(19 72)', 4326)) LIMIT 5";
        let k = knn(q.as_bytes(), &o).unwrap();
        assert_eq!(k.k, 5);
        let w = String::from_utf8(k.with_radius(q.as_bytes(), 1000.0)).unwrap();
        assert!(w.starts_with(KNN_MARK), "{w}");
        assert!(w.contains("WHERE ( kind = 'a' ) AND ST_Distance(p.loc, ST_GeomFromText('POINT(19 72)', 4326)) <= 1e3 ORDER BY"), "{w}");
        let p = String::from_utf8(k.probe(q.as_bytes(), 1000.0)).unwrap();
        assert!(p.contains("SELECT COUNT(*) FROM (/*sprig:knn*/ SELECT id"), "{p}");
        assert!(knn(w.as_bytes(), &o).is_none());
        let q2 = "SELECT id FROM place ORDER BY ST_Distance(loc, ST_GeomFromText('POINT(19 72)', 4326)) ASC LIMIT 3;";
        let w2 = String::from_utf8(knn(q2.as_bytes(), &o).unwrap().with_radius(q2.as_bytes(), 4000.0)).unwrap();
        assert!(w2.contains("FROM place WHERE ST_Distance(loc,"), "{w2}");
        for bad in [
            "SELECT id FROM a, b ORDER BY ST_Distance(loc, ST_GeomFromText('POINT(19 72)', 4326)) LIMIT 3",
            "SELECT id FROM a ORDER BY ST_Distance(loc, ST_GeomFromText('POINT(19 72)', 4326)) DESC LIMIT 3",
            "SELECT id FROM a ORDER BY ST_Distance(loc, ST_GeomFromText('POINT(19 72)', 4326)) LIMIT 3 OFFSET 1",
            "SELECT id FROM a ORDER BY ST_Distance(loc, ST_GeomFromText('POINT(19 72)')) LIMIT 3",
            "SELECT id FROM a ORDER BY ST_Distance(loc, ST_GeomFromText('POINT(19 72)', 4326))",
        ] {
            assert!(knn(bad.as_bytes(), &o).is_none(), "{bad}");
        }
    }

    #[test]
    fn distance_predicates_get_a_box() {
        let s = rw(
            "SELECT id FROM place WHERE ST_Distance(loc, ST_GeomFromText('POINT(19.07 72.87)', 4326)) < 5000",
        );
        assert!(s.contains("(MBRIntersects(loc, ST_GeomFromText('POLYGON(("), "{s}");
        // The box is in stored order (longitude first), around 72.87 19.07.
        assert!(s.contains("7.28") && s.contains("1.90"), "{s}");
        assert!(s.contains("AND IF("), "{s}");
        // Arguments the other way round, `<=`, a qualified column.
        let s = rw(
            "SELECT 1 FROM p WHERE ST_Distance(ST_GeomFromText('POINT(1 2)', 4326), p.loc) <= 10",
        );
        assert!(s.contains("MBRIntersects(p.loc,"), "{s}");
        // Not narrowed: `>`, a parameter radius, a pole.
        for q in [
            "SELECT 1 FROM p WHERE ST_Distance(loc, ST_GeomFromText('POINT(1 2)', 4326)) > 10",
            "SELECT 1 FROM p WHERE ST_Distance(loc, ST_GeomFromText('POINT(89.99 2)', 4326)) < 5000",
        ] {
            assert!(!rw(q).contains("MBRIntersects"), "{q}");
        }
    }

    #[test]
    fn literals_are_swapped_in_the_gateway() {
        assert_eq!(
            rw("INSERT INTO s VALUES (1, ST_GeomFromText('POINT(18.9 72.8)', 4326))"),
            "INSERT INTO s VALUES (1, ST_GeomFromText('POINT(72.8 18.9)', 4326))"
        );
        assert_eq!(
            rw("SELECT ST_GeomFromText('POINT(1 2)', 4326, 'axis-order=long-lat')"),
            "SELECT ST_GeomFromText('POINT(1 2)', 4326)"
        );
        assert_eq!(
            rw("SELECT ST_GeomFromText('POINT(1 2)')"),
            "SELECT ST_GeomFromText('POINT(1 2)')"
        );
        assert!(rw("SELECT ST_PointFromText(?, 4326)").contains("REGEXP_REPLACE(?,"));
    }

    #[test]
    fn readers() {
        assert_eq!(
            rw("SELECT ST_X(ST_GeomFromText('POINT(10 20)', 4326))"),
            "SELECT ST_Y(ST_GeomFromText('POINT(20 10)', 4326))"
        );
        assert_eq!(
            rw("SELECT ST_Latitude(loc) FROM s"),
            "SELECT ST_Y(loc) FROM s"
        );
        assert_eq!(
            rw("SELECT ST_X(loc) FROM s"),
            "SELECT IF(ST_SRID(loc) = 4326, ST_Y(loc), ST_X(loc)) FROM s"
        );
        assert!(
            rw("SELECT ST_AsText(loc) FROM s")
                .starts_with("SELECT IF(ST_SRID(loc) = 4326, REGEXP_REPLACE(ST_AsText(loc),")
        );
        assert_eq!(rw("SELECT ST_X(?)"), "SELECT ST_X(?)");
    }

    #[test]
    fn distances() {
        let s = rw(
            "SELECT id FROM s WHERE ST_Distance(loc, ST_GeomFromText('POINT(1 1)', 4326)) < 5000",
        );
        // R6 round 3: a candidate box in front of the exact test.
        assert!(
            s.starts_with("SELECT id FROM s WHERE (MBRIntersects(loc, ST_GeomFromText('POLYGON(("),
            "{s}"
        );
        assert!(
            s.contains("AND IF(ST_SRID(loc) = 4326 AND ST_GeometryType(loc) = 'POINT'"),
            "{s}"
        );
        assert!(s.contains("6378137e0"));
        assert!(s.ends_with(" < 5000)"));
        assert_eq!(
            rw("SELECT ST_Distance(loc, ST_GeomFromText(?, 4326)) FROM s"),
            "ERR 39767"
        );
        assert_eq!(
            rw("SELECT ST_Distance(a, ?) FROM s"),
            "SELECT ST_Distance(a, ?) FROM s"
        );
        assert!(rw("SELECT ST_Distance_Sphere(a, b) FROM s").contains("ASIN(SQRT("));
    }
}

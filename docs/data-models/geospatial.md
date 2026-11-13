---
title: Geospatial
audience: [user, dba]
sidebar:
  order: 7
---

# Geospatial

SprigDB stores locations and shapes in MariaDB and answers geospatial queries from SQL, MongoDB, Redis and the key-value API. Each protocol keeps its own semantics: a MySQL application gets MySQL 8.4's geographic answers, a MongoDB application gets MongoDB's, and a Redis client gets Redis's. Use this page to find which functions and operators work, how they are indexed, and what happens on a sharded table.

## Where geospatial works

| Protocol | Stored as | Index | Earth model for distances |
|---|---|---|---|
| MySQL persona (port 3306) | MariaDB spatial columns (`POINT`, `GEOMETRY`, ...) | `SPATIAL INDEX` (R-tree) | WGS 84 ellipsoid for SRID 4326, as MySQL 8.4 |
| MariaDB persona (port 3307) | MariaDB spatial columns | `SPATIAL INDEX` | MariaDB's own, unchanged |
| MongoDB | GeoJSON or legacy pairs inside documents | `2dsphere` | sphere of radius 6,378,100 m, as MongoDB |
| Redis | sorted sets scored by a 52-bit geohash | the sorted set itself | sphere of radius 6,372,797.560856 m, as Redis |
| Key-value API | a location on each key | S2 cells | WGS 84 ellipsoid |

Each protocol computes the same numbers as the database it imitates, so an application moved to SprigDB gets the same results, distances and order it got before.

## SQL: MySQL persona

MariaDB treats every geometry as planar and reads coordinates longitude first. MySQL 8.4 reads SRID 4326 (WGS 84) coordinates latitude first and measures them on the ellipsoid. On the MySQL persona, SprigDB rewrites the spatial functions so that MySQL applications get MySQL's answers. Both databases store longitude in X, so the stored bytes, and dumps, are the same either way: only the functions differ.

### What is rewritten

| You write (MySQL 8.4) | SprigDB sends to MariaDB |
|---|---|
| `ST_GeomFromText('POINT(lat lon)', 4326)` and the other text constructors | the literal's axes swapped; a non-literal text is swapped with `REGEXP_REPLACE`; `'axis-order=long-lat'` keeps the order |
| `ST_GeomFromWKB(w, 4326)` and the other WKB constructors | `ST_GeomFromText` of the swapped text |
| `ST_X(g)`, `ST_Y(g)` on SRID 4326 | swapped, so `ST_X` gives the latitude, as in MySQL |
| `ST_Latitude(g)`, `ST_Longitude(g)` (missing in MariaDB) | `ST_Y(g)`, `ST_X(g)` |
| `ST_AsText`, `ST_AsWKT`, `ST_AsBinary`, `ST_AsWKB` on SRID 4326 | latitude first |
| `ST_Distance(a, b)` of two SRID 4326 points | the ellipsoidal (Andoyer) distance in metres, computed inside MariaDB with MySQL's operations |
| `ST_Distance_Sphere(a, b)` of SRID 4326 points | haversine on the WGS 84 mean radius, as MySQL |
| The column attribute `SRID 4326`, including `/*!80003 SRID 4326 */` | MariaDB's `REF_SYSTEM_ID=4326` |

When an argument is a constructor with the literal SRID 4326, SprigDB knows it is geographic. Otherwise the rewritten SQL tests `ST_SRID(g) = 4326` when it runs, so columns and variables work too. Because the result is ordinary SQL, the rewritten functions work in every clause and in stored programs.

The distances agree with MySQL 8.4 to the last digit or within one unit in the last place.

```sql test
CREATE TABLE stores (
  id INT PRIMARY KEY,
  city VARCHAR(20),
  loc POINT NOT NULL SRID 4326,
  SPATIAL INDEX (loc)
);
INSERT INTO stores VALUES
  (1, 'mumbai', ST_GeomFromText('POINT(18.9 72.8)', 4326)),
  (2, 'delhi',  ST_GeomFromText('POINT(28.6 77.2)', 4326)),
  (3, 'pune',   ST_PointFromText('POINT(18.52 73.85)', 4326)),
  (4, 'kolkata', ST_GeomFromText('POINT(88.36 22.57)', 4326, 'axis-order=long-lat'));
```

Kolkata was written longitude first, with `'axis-order=long-lat'`, and reads back latitude first:

```sql test
SELECT city, ST_Latitude(loc) AS latitude, ST_Longitude(loc) AS longitude, ST_AsText(loc) AS wkt
FROM stores WHERE id = 4;
```

```text output
city	latitude	longitude	wkt
kolkata	22.57	88.36	POINT(22.57 88.36)
```

The distance between two stores, in metres on the ellipsoid:

```sql test
SELECT a.city AS from_city, b.city AS to_city, ST_Distance(a.loc, b.loc) AS metres
FROM stores a JOIN stores b ON a.id < b.id
WHERE a.id = 1 AND b.id = 3;
```

```text output
from_city	to_city	metres
mumbai	pune	118464.62200381035
```

### Distance filters use the spatial index

A filter of the form `ST_Distance(<column>, <constant SRID 4326 point>) < r` (or `<=`, with the arguments either way round) gets a bounding-box test in front of it:

```sql test
SELECT city FROM stores
WHERE ST_Distance(loc, ST_GeomFromText('POINT(18.9 72.8)', 4326)) < 150000
ORDER BY id;
```

```text output
city
mumbai
pune
```

MariaDB reads the column's `SPATIAL INDEX` for the points inside the box, and the exact ellipsoidal test then decides. `EXPLAIN` shows a range read on the index instead of a full scan. The box isn't added for `>`, for a radius given as a parameter, or when the circle crosses a pole or the ±180° meridian; those filters still give the right answer, by scanning.

### Nearest neighbours

A query of this shape finds the `k` nearest rows:

```sql test
SELECT city FROM stores
ORDER BY ST_Distance(loc, ST_GeomFromText('POINT(19 73)', 4326))
LIMIT 2;
```

```text output
city
mumbai
pune
```

SprigDB runs it with a growing radius. It counts the rows within 1 km of the point, using the spatial index; if there are fewer than `k`, it multiplies the radius by four and counts again. At the first radius that holds `k` rows, it runs your statement with that distance filter added. Every row within the radius is a candidate, so the `k` nearest are among them, and the result equals the unrewritten statement's.

This works the same on a sharded table: each count and the final statement go through the router, which reads every shard and merges the ordered results. The histogram `sprig_geo_knn_rounds` records how many radii a search needed.

The shape must be exactly: `SELECT ... FROM <one table> [WHERE ...] ORDER BY ST_Distance(<column>, <constant SRID 4326 point>) [ASC] LIMIT k`. Joins, `DESC`, `OFFSET` and points without the SRID run as written, which is correct but scans. If a count fails, or the radius passes 20,100 km, the statement also runs as written.

### Automatic spatial indexes

[Automatic indexing](../concepts/automatic-indexing.md) proposes a `SPATIAL INDEX` for a `NOT NULL` geometry column that queries filter with `ST_Distance`, `ST_Distance_Sphere`, `ST_Contains`, `ST_Within`, `ST_Intersects`, `ST_Overlaps`, `ST_Touches`, `ST_Crosses` or `ST_Equals`. A nullable column gets no candidate, because MariaDB can't put a spatial index on it.

### Limits of the MySQL rewrite

- A distance whose argument holds a parameter (`?`) fails with 39767 `ER_SPRIG_GEO_UNSUPPORTED_EXPRESSION` when the SRID is 4326, because the rewrite repeats the argument. Bind the point with a constructor and a literal SRID, or use a session variable.
- Distances involving lines and polygons, and `ST_Contains` and the other relations, keep MariaDB's planar answer.
- MySQL's latitude range check (error 3617) and its SRID enforcement on columns (error 3643) aren't applied.
- `ST_AsGeoJSON` prints `20` where MySQL prints `20.0`.

## SQL: MariaDB persona

The MariaDB persona (port 3307) passes spatial SQL to MariaDB unchanged, because MariaDB clients expect MariaDB's planar semantics. Spatial types, `SPATIAL INDEX` and every `ST_` function behave as in MariaDB 11.8 and 12.3. See [MySQL and MariaDB](../connect/mysql-and-mariadb.md).

## MongoDB

Store locations as GeoJSON (`Point`, `MultiPoint`, `LineString`, `MultiLineString`, `Polygon`, `MultiPolygon`, `GeometryCollection`) or as legacy coordinate pairs (`[lng, lat]`, or an embedded document of two numbers), alone or in an array. See [Documents](documents.md) for how collections are stored.

### 2dsphere indexes

```js
db.events.createIndex({ loc: "2dsphere" })
```

A `2dsphere` index on a path adds two virtual columns to the collection's table, the latitude and longitude of a point stored at the path, and a B-tree index on them. MariaDB keeps them on every write. A query pushes the region's bounding box onto the index, and the gateway checks every candidate exactly on the sphere.

Documents whose value at the path is a line or polygon go to a side table that holds each one's bounding box. The side table is brought up to date before a geo query on the path, at most once a second per gateway, and documents written since then are always kept as candidates, so results are never missing recent writes.

`listIndexes` shows the index as MongoDB does, with `2dsphereIndexVersion: 3`. An index on one `2dsphere` path is supported; a compound index with a `2dsphere` part fails with 39662.

### Query operators

| Operator | Supported forms |
|---|---|
| `$geoWithin` | `$geometry` (a `Polygon` or `MultiPolygon`, holes included), `$centerSphere`, and the flat shapes `$box`, `$center` and `$polygon` |
| `$geoIntersects` | `$geometry` of any GeoJSON type |
| `$near` | a GeoJSON point, with `$minDistance` and `$maxDistance` in metres |
| `$nearSphere` | a GeoJSON point (metres) or a legacy pair (radians), with `$minDistance` and `$maxDistance` |
| `$geoNear` stage | `near`, `distanceField`, `spherical`, `minDistance`, `maxDistance`, `query`, `key`, `includeLocs`, `distanceMultiplier` |

```js
db.events.find({ loc: { $geoWithin: { $centerSphere: [[72.88, 19.07], 0.005] } } })

db.events.find({ loc: { $near: {
  $geometry: { type: "Point", coordinates: [72.88, 19.07] },
  $minDistance: 2000, $maxDistance: 40000 } } })

db.events.aggregate([
  { $geoNear: { near: { type: "Point", coordinates: [72.88, 19.07] },
                distanceField: "dist.m", maxDistance: 50000,
                query: { kind: "b" }, includeLocs: "where",
                distanceMultiplier: 0.001 } },
  { $limit: 25 }
])
```

As in MongoDB:

- `$geoWithin` and `$geoIntersects` work with or without an index. Without one, every document is checked.
- `$near`, `$nearSphere` and `$geoNear` need a `2dsphere` index. Without one they fail with 291 (`unable to find index for $geoNear query`) or `$geoNear requires a 2d or 2dsphere index, but none were found`. With more than one `2dsphere` index, `$geoNear` needs `key`.
- Results of `$near` are sorted by distance; distances to lines and polygons are to their nearest point, and 0 inside a polygon.
- `$box`, `$center` and `$polygon` test points on the plane.
- MongoDB's error codes are returned for malformed shapes (2 `BadValue`), unclosed loops, `$near` under `$or`, and `$geoNear` not in the first stage.

These operators and stages were checked against MongoDB 8.0 with no differences.

### Not supported in MongoDB

| Feature | Error |
|---|---|
| `2d` indexes, so `$near` with a legacy pair and `$geoNear` with a legacy point without `spherical: true` | 291, as MongoDB answers without a `2d` index |
| Other index types for geo, such as `geoHaystack` | 39662 |
| Compound `2dsphere` indexes | 39662 |
| `$geometry` with a custom `crs` (big polygons) | 39662 |
| Automatic creation of `2dsphere` indexes | create them yourself |

## Redis

The Redis geo commands work as in Redis 7.2: a geo key is a sorted set whose scores are 52-bit geohashes, and distances use Redis's haversine formula and earth radius, so results match Redis exactly.

| Command | Options |
|---|---|
| `GEOADD` | `NX`, `XX`, `CH` |
| `GEOPOS`, `GEODIST`, `GEOHASH` | Redis's units: `m`, `km`, `mi`, `ft` |
| `GEOSEARCH` | `FROMMEMBER`, `FROMLONLAT`, `BYRADIUS`, `BYBOX`, `ASC`, `DESC`, `COUNT` (with `ANY`), `WITHCOORD`, `WITHDIST`, `WITHHASH` |
| `GEOSEARCHSTORE` | as `GEOSEARCH`, plus `STOREDIST` |
| `GEORADIUS`, `GEORADIUSBYMEMBER` and their `_RO` forms | Redis's, including `STORE` and `STOREDIST` |

```text
GEOADD stores 72.8 18.9 mumbai 73.85 18.52 pune 77.2 28.6 delhi
GEOSEARCH stores FROMLONLAT 73 19 BYRADIUS 200 km ASC WITHDIST
```

Redis's coordinate limits apply: longitude −180 to 180, latitude −85.05112878 to 85.05112878. Because a geo key is a sorted set, every other sorted-set command works on it, and SQL sees its members in the `<ns>__zset` view with the geohash as the score. See [Key-value](key-value.md).

A search reads every member of the key and keeps those inside the shape, so its cost grows with the size of the key. Split very large geo sets by region into several keys.

## Key-value API

Any key in an API namespace can carry a location. Give it on `Put`:

```sh
curl -u acme.app:'<password>' -X POST \
  https://sprig.example.com:8443/v1/databases/shop/namespaces/places:put \
  -d '{"key": "store:1", "text": "Mumbai central", "location": {"latitude": 18.9, "longitude": 72.8}}'
```

The first location in a namespace adds two columns to its key table: `loc POINT NULL` (longitude in X, latitude in Y) and a hidden column holding the location's level-30 S2 cell, with an index. Out-of-range coordinates fail with 39765 `ER_SPRIG_GEO_INVALID` (HTTP 400).

Search with `POST /v1/databases/{database}/namespaces/{namespace}:geoSearch`:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `region` | object | none | A circle `{"center": {...}, "radius_m": ...}`, a box `{"south_west": {...}, "north_east": {...}}`, or a polygon `{"polygon": [{...}, ...]}` |
| `near` | object | none | `{"latitude", "longitude"}`: order hits by distance from here; alone, find the `limit` nearest keys |
| `max_distance_m` | number | none | With `near` and no region: the farthest hit, in metres |
| `limit` | int | `100` | Hits to return, at most 10,000 |

```json
{"region": {"center": {"latitude": 19.07, "longitude": 72.88}, "radius_m": 5000}, "limit": 50}
```

Each hit carries the entry (as `Get` returns it), its location, and its distance in metres from the centre or the `near` point (0 when there is neither):

```text
{"hits": [
  {"entry": {"key": "store:7", "type": "string", "value": "...", "text": "...", "version": "...", "expires_at_ms": "0"},
   "location": {"latitude": ..., "longitude": ...},
   "distance_m": <metres>}
]}
```

How it searches:

- The region is covered with ranges of S2 cells, read through the index; each candidate is then tested exactly. Circle distances are geodesics on the WGS 84 ellipsoid.
- A circle orders hits by distance from its centre, `near` orders by distance from that point, and a box or polygon without `near` orders by key.
- A box crosses the ±180° meridian when its west edge is east of its east edge. Polygon edges are great-circle arcs, and points on the boundary are inside.
- `near` without a region searches a circle of 1 km, then four times wider, until it holds `limit` keys or reaches `max_distance_m`.
- A polygon has at most 100,000 vertices; more fails with 39768 `ER_SPRIG_GEO_TOO_COMPLEX`. A search with neither `region` nor `near` fails with 39765.

On a sharded namespace, each shard is searched and the results are merged, with the same answer as an unsharded one.

From SQL, the location is the `loc` column of the key table:

```sql
SELECT k, ST_Y(loc) AS latitude, ST_X(loc) AS longitude FROM shop.places WHERE loc IS NOT NULL;
```

## Vector search with locations

Vector indexes can hold a location per item and filter a similarity search by the same circle, box or polygon regions as the key-value API. See [Vectors](vectors.md).

## Errors

| Code | Name | When |
|---|---|---|
| 39765 | `ER_SPRIG_GEO_INVALID` | Bad coordinates or geometry on the key-value and vector APIs |
| 39767 | `ER_SPRIG_GEO_UNSUPPORTED_EXPRESSION` | A geodesic expression the gateway can't evaluate exactly, such as a distance over a `?` parameter |
| 39768 | `ER_SPRIG_GEO_TOO_COMPLEX` | A polygon over 100,000 vertices |
| 39662 | (MongoDB) | A MongoDB geo feature that isn't offered |

The MySQL persona otherwise returns MySQL's own errors, and the MongoDB front end MongoDB's. See [Error codes](../reference/errors.md).

## What's not supported

- Geodesic relations (`ST_Contains`, `ST_Within`, `ST_Intersects`, ...) and distances for lines and polygons on the MySQL persona: MariaDB's planar answer is returned.
- MongoDB `2d` indexes, compound `2dsphere` indexes, custom-CRS polygons, and automatic `2dsphere` indexes.
- Redis geo searches that read only the covering geohash ranges: every member of the key is read.
- Geo in full-text search: the `geo` field type and the `geoShape`, `geoWithin` and `near` operators of `$search` fail with 39823 `ER_SPRIG_SEARCH_UNSUPPORTED`. See [Search](search.md).

## Related

- [Key-value](key-value.md)
- [Documents](documents.md)
- [Vectors](vectors.md)
- [Automatic indexing](../concepts/automatic-indexing.md)
- [MySQL compatibility](../sql/overview.md)

#!/usr/bin/env python3
"""Generates sprig-shard's collation weight tables.

The tables are read out of MariaDB itself: for every code point, the
weight string of that one character at every level of every supported
collation, plus the weights of every contraction listed in the DUCET of
UCA 14.0.0 (MariaDB's uca1400 collations apply them; its UCA 4.0.0 and
5.2.0 collations don't). The Rust side recomputes WEIGHT_STRING() from
these and the conformance test compares the two on generated strings.

Usage:
  gen_collations.py --mysql 'docker exec -i sprigdb-dev-single-1 mariadb -uroot -pPW -N -B mysql' \
      --allkeys allkeys-14.0.0.txt --out ../data/collations.bin.z

The output is zlib-compressed. Format (little-endian), after inflation:
  "SPRW" u8 version=1, u16 table_count, then per table:
    u16 name_len, name, u8 width, u8 levels,
    per level: u32 run_count, runs of
      u32 start, u32 len, u8 delta (0 or 1), u8 n, n x u16 weights
      (code point start+i weighs the run's weights with the last one + delta*i)
    u32 contraction_count, per contraction:
      u8 cp_count, cp_count x u32, per level: u8 n, n x u16 weights
  Width-1 tables (latin1) store their weights in u16 too.
  Then u16 256 x u32: latin1 byte -> code point (MariaDB's latin1 is cp1252).
"""
import argparse, shlex, struct, subprocess, sys, zlib

# (table name, collation to read, physical levels, max code point, use contractions)
TABLES = [
    ("uca1400", "utf8mb4_uca1400_as_cs", 3, 0x10FFFF, True),
    ("unicode_ci", "utf8mb4_unicode_ci", 1, 0x10FFFF, False),
    ("unicode_520_ci", "utf8mb4_unicode_520_ci", 1, 0x10FFFF, False),
    ("general_ci", "utf8mb4_general_ci", 1, 0x10FFFF, False),
    ("latin1_swedish_ci", "latin1_swedish_ci", 1, 0xFF, False),
]

# The other uca1400 strengths use a subset of the as_cs levels (WEIGHT_STRING
# ... LEVEL k names the physical level; ai_cs has levels 1 and 3). Checked
# to be identical before the table is written.
UCA1400_SUBSETS = [
    ("utf8mb4_uca1400_ai_ci", [1]),
    ("utf8mb4_uca1400_ai_cs", [1, 3]),
    ("utf8mb4_uca1400_as_ci", [1, 2]),
]

def run(mysql, sql):
    p = subprocess.run(shlex.split(mysql), input=sql, capture_output=True, text=True)
    if p.returncode != 0:
        sys.exit(p.stderr)
    return p.stdout

def char_expr(cp_sql, coll):
    if coll.startswith("latin1"):
        return f"CHAR({cp_sql} USING latin1) COLLATE {coll}"
    return f"CONVERT(CHAR({cp_sql} USING utf32) USING utf8mb4) COLLATE {coll}"

def parse_hex(h, width):
    b = bytes.fromhex(h)
    return [int.from_bytes(b[i:i + width], "big") for i in range(0, len(b), width)]

def level_weights(mysql, coll, level, maxcp, width):
    sql = (f"SELECT seq, HEX(WEIGHT_STRING({char_expr('seq', coll)} LEVEL {level})) "
           f"FROM seq_0_to_{maxcp} WHERE seq NOT BETWEEN 55296 AND 57343")
    out = {}
    for line in run(mysql, sql).splitlines():
        cp, h = line.split("\t")
        out[int(cp)] = parse_hex(h if h != "NULL" else "", width)
    return out

def runs_of(weights):
    runs = []  # [start, len, delta, first]
    for cp in sorted(weights):
        w = weights[cp]
        if runs:
            r = runs[-1]
            start, n, delta, first = r
            if start + n == cp and len(first) == len(w) and (not w or first[:-1] == w[:-1]):
                if not w:
                    r[1] += 1
                    continue
                for d in ([delta] if delta is not None else [0, 1]):
                    if first[-1] + d * n == w[-1]:
                        r[1] += 1
                        r[2] = d
                        break
                else:
                    runs.append([cp, 1, None, w])
                continue
        runs.append([cp, 1, None, w])
    return runs

def contractions(path):
    seqs = []
    for line in open(path, encoding="utf-8"):
        if line.startswith(("#", "@")) or not line.strip():
            continue
        cps = line.split(";")[0].split()
        if len(cps) > 1:
            seqs.append([int(c, 16) for c in cps])
    return seqs

def contraction_weights(mysql, coll, levels, seqs):
    stmts = []
    for i, s in enumerate(seqs):
        cps = ",".join(str(c) for c in s)
        cols = ", ".join(f"HEX(WEIGHT_STRING({char_expr(cps, coll)} LEVEL {l}))"
                         for l in range(1, levels + 1))
        stmts.append(f"SELECT {i}, {cols};")
    out = {}
    for line in run(mysql, "\n".join(stmts)).splitlines():
        f = line.split("\t")
        out[int(f[0])] = [parse_hex(h, 2) for h in f[1:]]
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mysql", required=True)
    ap.add_argument("--allkeys", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    version = run(a.mysql, "SELECT VERSION()").strip()
    print("MariaDB", version, file=sys.stderr)
    seqs = contractions(a.allkeys)
    body = bytearray(b"SPRW") + struct.pack("<BH", 1, len(TABLES))
    for name, coll, levels, maxcp, contr in TABLES:
        width = 1 if coll.startswith("latin1") else 2
        nb = name.encode()
        body += struct.pack("<H", len(nb)) + nb + struct.pack("<BB", width, levels)
        total = 0
        for level in range(1, levels + 1):
            lw = level_weights(a.mysql, coll, level, maxcp, width)
            if name == "uca1400":
                for other, lv in UCA1400_SUBSETS:
                    if level in lv and level_weights(a.mysql, other, level, maxcp, width) != lw:
                        sys.exit(f"{other} level {level} differs from {coll}")
            rs = runs_of(lw)
            total += len(rs)
            body += struct.pack("<I", len(rs))
            for start, n, delta, first in rs:
                body += struct.pack("<IIBB", start, n, delta or 0, len(first))
                body += struct.pack(f"<{len(first)}H", *first)
        cw = contraction_weights(a.mysql, coll, levels, seqs) if contr else {}
        body += struct.pack("<I", len(cw))
        for i in sorted(cw):
            s = seqs[i]
            body += struct.pack("<B", len(s)) + struct.pack(f"<{len(s)}I", *s)
            for lw in cw[i]:
                body += struct.pack("<B", len(lw)) + struct.pack(f"<{len(lw)}H", *lw)
        print(f"{name}: {total} runs, {len(cw)} contractions", file=sys.stderr)
    l1 = run(a.mysql, "SELECT seq, HEX(CONVERT(CHAR(seq USING latin1) USING utf32)) "
                      "FROM seq_0_to_255")
    cps = [0] * 256
    for line in l1.splitlines():
        b, h = line.split("\t")
        cps[int(b)] = int(h, 16)
    body += struct.pack("<H", 256) + struct.pack("<256I", *cps)
    data = zlib.compress(bytes(body), 9)
    open(a.out, "wb").write(data)
    print(f"{len(body)} bytes, {len(data)} compressed", file=sys.stderr)

if __name__ == "__main__":
    main()

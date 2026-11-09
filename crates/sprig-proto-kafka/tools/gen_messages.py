#!/usr/bin/env python3
"""Generates src/messages/*.rs from Apache Kafka's message definitions.

SprigDB's Kafka codec (chapter 45 build step 8) is written in-house; the
message bodies are generated from the JSON definitions Kafka publishes under
clients/src/main/resources/common/message/ (Apache-2.0, the same licence as
SprigDB), at tag 3.9.0, which still carries every old version. The JSON files
are not kept in the repository: fetch them into a directory and run

    python3 tools/gen_messages.py <dir with the JSON files>

from crates/sprig-proto-kafka. The output is checked in and formatted with
cargo fmt; tests/golden.rs checks it against hand-written bytes.

Supported: int8, int16, uint16, int32, int64, float64, bool, uuid, string,
bytes, arrays of those and of nested structs, nullable versions, defaults,
flexible versions (compact encodings, tagged-field sections in every struct)
and tagged fields. Not needed and refused: records, commonStructs, single
(non-array) struct fields.
"""

import json
import os
import re
import sys

MESSAGES = [
    "ApiVersionsRequest", "ApiVersionsResponse",
    "MetadataRequest", "MetadataResponse",
    "FindCoordinatorRequest", "FindCoordinatorResponse",
    "JoinGroupRequest", "JoinGroupResponse",
    "SyncGroupRequest", "SyncGroupResponse",
    "HeartbeatRequest", "HeartbeatResponse",
    "LeaveGroupRequest", "LeaveGroupResponse",
    "OffsetCommitRequest", "OffsetCommitResponse",
    "OffsetFetchRequest", "OffsetFetchResponse",
    "DescribeGroupsRequest", "DescribeGroupsResponse",
    "ListGroupsRequest", "ListGroupsResponse",
    "DeleteGroupsRequest", "DeleteGroupsResponse",
    "SaslHandshakeRequest", "SaslHandshakeResponse",
    "SaslAuthenticateRequest", "SaslAuthenticateResponse",
    "InitProducerIdRequest", "InitProducerIdResponse",
    "CreateTopicsRequest", "CreateTopicsResponse",
    "DeleteTopicsRequest", "DeleteTopicsResponse",
    "CreatePartitionsRequest", "CreatePartitionsResponse",
    "DescribeConfigsRequest", "DescribeConfigsResponse",
    "IncrementalAlterConfigsRequest", "IncrementalAlterConfigsResponse",
    "ConsumerProtocolSubscription", "ConsumerProtocolAssignment",
]

MAX = 32767
KEYWORDS = {"type", "match", "ref", "self", "use", "mod", "fn", "loop", "move", "in", "as"}
PRIMS = {
    "int8": ("i8", "0"), "int16": ("i16", "0"), "uint16": ("u16", "0"),
    "int32": ("i32", "0"), "int64": ("i64", "0"), "float64": ("f64", "0.0"),
    "bool": ("bool", "false"), "uuid": ("Uuid", "Uuid::nil()"),
    "string": ("String", "String::new()"), "bytes": ("Vec<u8>", "Vec::new()"),
}


def load(path):
    text = open(path).read()
    text = re.sub(r"^\s*//.*$", "", text, flags=re.M)
    return json.loads(text)


def versions(spec):
    if spec is None or spec == "none":
        return None
    if spec.endswith("+"):
        return (int(spec[:-1]), MAX)
    if "-" in spec:
        a, b = spec.split("-")
        return (int(a), int(b))
    return (int(spec), int(spec))


def cond(rng, valid):
    """A Rust condition on `v` for a version range, simplified against the
    message's valid range."""
    if rng is None:
        return "false"
    lo, hi = rng
    parts = []
    if lo > valid[0]:
        parts.append(f"v >= {lo}")
    if hi < valid[1]:
        parts.append(f"v <= {hi}")
    return " && ".join(parts) if parts else "true"


def covers(inner, outer):
    """Whether range `inner` contains all of `outer`."""
    if inner is None:
        return False
    return inner[0] <= outer[0] and inner[1] >= outer[1]


def intersect(a, b):
    if a is None or b is None:
        return None
    lo, hi = max(a[0], b[0]), min(a[1], b[1])
    return (lo, hi) if lo <= hi else None


def snake(name):
    s = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name)
    s = re.sub(r"(?<=[A-Z])([A-Z][a-z])", r"_\1", s)
    s = s.lower()
    return s + "_" if s in KEYWORDS else s


def doc(text, indent):
    text = (text or "").strip() or "(undocumented)"
    text = text.replace("[", "\\[").replace("]", "\\]").replace("<", "&lt;").replace(">", "&gt;")
    return f"{indent}/// {text}\n"


class Field:
    def __init__(self, j, valid):
        self.j = j
        self.name = j["name"]
        self.rust = snake(self.name)
        self.typ = j["type"]
        self.versions = versions(j["versions"])
        self.nullable = intersect(versions(j.get("nullableVersions")), self.versions)
        self.tag = j.get("tag")
        self.tagged = versions(j.get("taggedVersions"))
        self.default = j.get("default")
        self.about = j.get("about", "")
        self.array = self.typ.startswith("[]")
        self.elem = self.typ[2:] if self.array else self.typ
        self.struct = self.elem not in PRIMS
        if self.struct and not self.array:
            raise SystemExit(f"single struct field {self.name} not supported")
        if self.typ == "records":
            raise SystemExit("records fields not supported")
        if self.tag is not None and self.tagged != self.versions:
            raise SystemExit(f"{self.name}: tagged in only some versions")
        if j.get("flexibleVersions"):
            raise SystemExit(f"{self.name}: field-level flexibleVersions")

    def elem_rust(self):
        return self.elem if self.struct else PRIMS[self.elem][0]

    def base_rust(self):
        return f"Vec<{self.elem_rust()}>" if self.array else self.elem_rust()

    def rust_type(self):
        b = self.base_rust()
        return f"Option<{b}>" if self.nullable else b

    def default_expr(self):
        d = self.default
        if self.array:
            base = "Vec::new()"
            if self.nullable:
                return "None" if d == "null" else f"Some({base})"
            return base
        t = self.elem
        if t == "bool":
            base = "true" if d == "true" else "false"
        elif t in ("string",):
            if self.nullable and d == "null":
                return "None"
            base = f"String::from({json.dumps(d)})" if d not in (None, "null") else "String::new()"
        elif t == "bytes":
            if self.nullable and d == "null":
                return "None"
            base = "Vec::new()"
        elif t == "uuid":
            base = "Uuid::nil()"
        elif t == "float64":
            base = str(float(d)) if d is not None else "0.0"
        else:
            base = str(int(d, 0) if isinstance(d, str) else int(d)) if d is not None else "0"
        return f"Some({base})" if self.nullable else base

    # ---- encoding ----
    def enc_value(self, val, flex):
        """Statements encoding `val` (an expression of the non-Option base type,
        by reference) with compact=`flex`."""
        t = self.elem
        if self.array:
            return f"w.array({val}, {flex}, {self.enc_elem(flex)})?;"
        return self.enc_scalar(t, val, flex) + ";"

    def enc_scalar(self, t, val, flex):
        if t == "string":
            return f"w.string({val}, {flex})?"
        if t == "bytes":
            return f"w.bytes({val}, {flex})?"
        if t == "uuid":
            return f"w.uuid({val})"
        return f"w.{PRIMS[t][0]}(*{val})"

    def enc_elem(self, flex):
        t = self.elem
        if self.struct:
            return "|w, e| e.encode(w, v)"
        if t in ("string", "bytes"):
            return f"|w, e| {{ {self.enc_scalar(t, 'e', flex)}; Ok(()) }}"
        return f"|w, e| {{ {self.enc_scalar(t, 'e', flex)}; Ok(()) }}"

    def enc_nullable(self, val, flex):
        t = self.elem
        if self.array:
            return f"w.nullable_array({val}.as_deref(), {flex}, {self.enc_elem(flex)})?;"
        if t == "string":
            return f"w.nullable_string({val}.as_deref(), {flex})?;"
        if t == "bytes":
            return f"w.nullable_bytes({val}.as_deref(), {flex})?;"
        raise SystemExit(f"nullable {t} not supported")

    def encode(self, valid, flex_expr):
        """Encoding statements for a non-tagged field."""
        val = f"self.{self.rust}"
        if not self.nullable:
            body = self.enc_value(f"&{val}", flex_expr)
        else:
            nn = f"{val}.as_ref().ok_or(CodecError::NullNotAllowed({json.dumps(self.name)}))?"
            nonnull = self.enc_value(nn, flex_expr)
            if covers(self.nullable, intersect(self.versions, valid)):
                body = self.enc_nullable(f"{val}", flex_expr)
            else:
                body = (f"if {cond(self.nullable, valid)} {{ {self.enc_nullable(val, flex_expr)} }} "
                        f"else {{ {nonnull} }}")
        c = cond(intersect(self.versions, valid), valid)
        return body if c == "true" else f"if {c} {{ {body} }}"

    # ---- decoding ----
    def dec_scalar(self, t, flex):
        if t == "string":
            return f"r.string({flex})?"
        if t == "bytes":
            return f"r.bytes({flex})?"
        return f"r.{'uuid' if t == 'uuid' else PRIMS[t][0]}()?"

    def dec_elem(self, flex):
        t = self.elem
        if self.struct:
            return f"|r| {t}::decode(r, v)"
        return f"|r| Ok({self.dec_scalar(t, flex)})"

    def dec_value(self, flex):
        if self.array:
            return f"r.array({flex}, {self.dec_elem(flex)})?"
        return self.dec_scalar(self.elem, flex)

    def dec_nullable(self, flex):
        t = self.elem
        if self.array:
            return f"r.nullable_array({flex}, {self.dec_elem(flex)})?"
        if t == "string":
            return f"r.nullable_string({flex})?"
        if t == "bytes":
            return f"r.nullable_bytes({flex})?"
        raise SystemExit(f"nullable {t} not supported")

    def decode(self, valid, flex_expr, target="s"):
        if not self.nullable:
            expr = self.dec_value(flex_expr)
        elif covers(self.nullable, intersect(self.versions, valid)):
            expr = self.dec_nullable(flex_expr)
        else:
            expr = (f"if {cond(self.nullable, valid)} {{ {self.dec_nullable(flex_expr)} }} "
                    f"else {{ Some({self.dec_value(flex_expr)}) }}")
        stmt = f"{target}.{self.rust} = {expr};"
        c = cond(intersect(self.versions, valid), valid)
        return stmt if c == "true" else f"if {c} {{ {stmt} }}"


class Struct:
    def __init__(self, name, fields_json, valid, about, top):
        self.name = name
        self.valid = valid
        self.about = about
        self.top = top
        self.fields = [Field(f, valid) for f in fields_json]
        self.children = []
        for fj, f in zip(fields_json, self.fields):
            if f.struct:
                self.children.append(Struct(f.elem, fj.get("fields", []), valid,
                                            f.about, False))

    def all(self):
        out = [self]
        for c in self.children:
            out.extend(c.all())
        return out

    def render(self):
        o = []
        o.append(doc(self.about, ""))
        o.append("#[derive(Debug, Clone, PartialEq)]\n")
        o.append(f"pub struct {self.name} {{\n")
        for f in self.fields:
            vs = f.j["versions"]
            o.append(doc(f"{f.about} (versions {vs}"
                         + (f", nullable {f.j['nullableVersions']}" if f.nullable else "")
                         + (f", tag {f.tag}" if f.tag is not None else "") + ")", "    "))
            o.append(f"    pub {f.rust}: {f.rust_type()},\n")
        o.append("    /// Tagged fields this codec doesn't know (flexible versions).\n")
        o.append("    pub unknown_tagged_fields: Vec<RawTaggedField>,\n")
        o.append("}\n\n")
        o.append(f"impl Default for {self.name} {{\n    fn default() -> Self {{\n        Self {{\n")
        for f in self.fields:
            o.append(f"            {f.rust}: {f.default_expr()},\n")
        o.append("            unknown_tagged_fields: Vec::new(),\n        }\n    }\n}\n\n")

        regular = [f for f in self.fields if f.tag is None]
        tagged = sorted([f for f in self.fields if f.tag is not None], key=lambda f: f.tag)
        enc = []
        for f in regular:
            enc.append("        " + f.encode(self.valid, "flex") + "\n")
        enc.append("        if flex {\n")
        if tagged:
            enc.append("            let mut known = Vec::new();\n")
            for f in tagged:
                c = cond(f.tagged, self.valid)
                val = f"self.{f.rust}"
                inner = f.enc_value(f"&{val}", "true") if not f.nullable else f.enc_nullable(val, "true")
                enc.append(f"            if {c} && {val} != {f.default_expr()} {{\n")
                enc.append(f"                known.push(({f.tag}, encode_with(|w| {{ {inner} Ok(()) }})?));\n")
                enc.append("            }\n")
            enc.append("            w.tagged_fields(known, &self.unknown_tagged_fields)?;\n")
        else:
            enc.append("            w.tagged_fields(Vec::new(), &self.unknown_tagged_fields)?;\n")
        enc.append("        }\n")

        dec = ["        let mut s = Self::default();\n"]
        for f in regular:
            dec.append("        " + f.decode(self.valid, "flex") + "\n")
        dec.append("        if flex {\n")
        if tagged:
            dec.append("            let mut t = Self::default();\n")
            dec.append("            s.unknown_tagged_fields = r.tagged_fields(|tag, r| match tag {\n")
            for f in tagged:
                c = cond(f.tagged, self.valid)
                expr = f.dec_value("true") if not f.nullable else f.dec_nullable("true")
                guard = "" if c == "true" else f" if {c}"
                dec.append(f"                {f.tag}{guard} => {{ t.{f.rust} = {expr}; Ok(true) }}\n")
            dec.append("                _ => Ok(false),\n            })?;\n")
            for f in tagged:
                dec.append(f"            s.{f.rust} = t.{f.rust};\n")
        else:
            dec.append("            s.unknown_tagged_fields = r.tagged_fields(|_, _| Ok(false))?;\n")
        dec.append("        }\n        Ok(s)\n")

        uses_v_enc = any("v" in line.replace("flex", "") for line in enc)
        if self.top:
            o.append(f"impl Message for {self.name} {{\n")
            o.append(f"    const API_KEY: i16 = API_KEY;\n")
            o.append(f"    const MIN_VERSION: i16 = {self.valid[0]};\n")
            o.append(f"    const MAX_VERSION: i16 = {self.valid[1]};\n")
            o.append(f"    const FLEXIBLE_MIN: i16 = FLEXIBLE_MIN;\n\n")
            o.append("    fn encode(&self, w: &mut Writer, v: i16) -> Result<()> {\n")
            o.append("        let flex = v >= FLEXIBLE_MIN;\n")
            o.extend(enc)
            o.append("        Ok(())\n    }\n\n")
            o.append("    fn decode(r: &mut Reader<'_>, v: i16) -> Result<Self> {\n")
            o.append("        let flex = v >= FLEXIBLE_MIN;\n")
            o.extend(dec)
            o.append("    }\n}\n\n")
        else:
            o.append(f"impl {self.name} {{\n")
            o.append("    /// Encodes this struct at version `v`.\n    ///\n    /// # Errors\n"
                     "    /// As [`Message::encode`].\n")
            o.append("    pub fn encode(&self, w: &mut Writer, v: i16) -> Result<()> {\n")
            o.append("        let flex = v >= FLEXIBLE_MIN;\n")
            o.extend(enc)
            o.append("        Ok(())\n    }\n\n")
            o.append("    /// Decodes this struct at version `v`.\n    ///\n    /// # Errors\n"
                     "    /// As [`Message::decode`].\n")
            o.append("    pub fn decode(r: &mut Reader<'_>, v: i16) -> Result<Self> {\n")
            o.append("        let flex = v >= FLEXIBLE_MIN;\n")
            o.extend(dec)
            o.append("    }\n}\n\n")
        return "".join(o)


def generate(src_dir, out_dir):
    mods = []
    for name in MESSAGES:
        j = load(os.path.join(src_dir, name + ".json"))
        if j.get("commonStructs"):
            raise SystemExit(f"{name}: commonStructs not supported")
        valid = versions(j["validVersions"])
        flex = versions(j.get("flexibleVersions", "none"))
        flex_min = flex[0] if flex else MAX
        api = j.get("apiKey", -1)
        top = Struct(name, j["fields"], valid, f"`{name}`, versions {j['validVersions']}"
                     f" (flexible {j.get('flexibleVersions', 'none')}).", True)
        names = [s.name for s in top.all()]
        if len(names) != len(set(names)):
            raise SystemExit(f"{name}: duplicate struct names")
        mod = snake(name)
        mods.append((mod, name))
        body = [
            "// @generated by tools/gen_messages.py from Apache Kafka 3.9.0's\n",
            f"// {name}.json. Do not edit by hand.\n\n",
            f"//! `{name}` (API key {api}): the message and its nested structs.\n\n",
            "#![allow(clippy::all, clippy::pedantic)]\n",
            "#![allow(unused_variables, unused_mut)]\n\n",
            "#[allow(unused_imports)]\n",
            "use uuid::Uuid;\n\n",
            "#[allow(unused_imports)]\n",
            "use crate::wire::{CodecError, Message, RawTaggedField, Reader, Result, Writer, encode_with};\n\n",
            f"/// The API key (`-1`: not an API message).\n",
            f"pub const API_KEY: i16 = {api};\n",
            f"/// The first flexible version (`i16::MAX`: none).\n",
            f"pub const FLEXIBLE_MIN: i16 = {flex_min};\n\n",
        ]
        for s in top.all():
            body.append(s.render())
        with open(os.path.join(out_dir, mod + ".rs"), "w") as fh:
            fh.write("".join(body))
    with open(os.path.join(out_dir, "mod.rs"), "w") as fh:
        fh.write("// @generated by tools/gen_messages.py. Do not edit by hand.\n\n")
        fh.write("//! Message bodies, generated from Apache Kafka 3.9.0's message\n")
        fh.write("//! definitions (Apache-2.0) by `tools/gen_messages.py`. Each module\n")
        fh.write("//! holds one message and its nested structs; the message implements\n")
        fh.write("//! [`crate::wire::Message`].\n\n")
        for mod, name in mods:
            fh.write(f"pub mod {mod};\n")
        fh.write("\n")
        for mod, name in mods:
            fh.write(f"pub use {mod}::{name};\n")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: gen_messages.py <dir with Kafka's message JSON files>")
    here = os.path.dirname(os.path.abspath(__file__))
    out = os.path.join(here, "..", "src", "messages")
    os.makedirs(out, exist_ok=True)
    generate(sys.argv[1], out)

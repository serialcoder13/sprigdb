#!/usr/bin/env python3
"""Assembles docs/ into one Markdown book for the PDF (docs/pdf/build-pdf.sh).

Sections become parts (level-1 headings) in the website's order, and pages
become chapters (level 2) in each page's `sidebar.order`. Front matter is
removed and links between pages point at the website, so the PDF needs no
anchors that pandoc can't resolve.
"""

import re
import sys
from pathlib import Path

DOCS = Path(__file__).resolve().parent.parent
SITE = "https://serialcoder13.github.io/sprigdb/docs/"
SECTIONS = [
    ("concepts", "Concepts"),
    ("get-started", "Get started"),
    ("connect", "Connect"),
    ("sql", "SQL"),
    ("data-models", "Data models"),
    ("performance", "Performance"),
    ("security", "Security"),
    ("operate", "Operate"),
    ("tools", "Tools"),
    ("troubleshooting", "Troubleshooting"),
    ("reference", "Reference"),
]


def front_matter(text):
    m = re.match(r"^---\n(.*?)\n---\n", text, re.S)
    if not m:
        return {}, text
    meta = {}
    order = re.search(r"^\s+order:\s*(\d+)", m.group(1), re.M)
    title = re.search(r"^title:\s*(.+)$", m.group(1), re.M)
    meta["order"] = int(order.group(1)) if order else 999
    meta["title"] = title.group(1).strip().strip("\"'") if title else ""
    return meta, text[m.end():]


def site_link(page_dir, target):
    """A relative .md link from docs/<page_dir>/ as a website URL."""
    path, _, anchor = target.partition("#")
    full = (DOCS / page_dir / path).resolve()
    try:
        rel = full.relative_to(DOCS)
    except ValueError:
        return target
    slug = str(rel.with_suffix("")).replace("README", "")
    url = SITE + (slug + "/" if slug else "")
    return url + ("#" + anchor if anchor else "")


def page(path, section):
    meta, body = front_matter(path.read_text())
    body = body.strip("\n")
    # Drop the page's own H1 (it becomes the chapter heading) and shift the rest.
    body = re.sub(r"^# [^\n]*\n", "", body, count=1)
    body = re.sub(r"^(#{1,5}) ", lambda m: "#" + m.group(1) + " ", body, flags=re.M)
    # Images: paths from typst's root, docs/ (build-pdf.sh passes --root).
    body = re.sub(
        r"(!\[[^\]]*\]\()(?!https?:)([^)\s]+)\)",
        lambda m: m.group(1) + "/" + str((DOCS / section / m.group(2)).resolve().relative_to(DOCS)) + ")",
        body,
    )
    # Generated pages start with an HTML comment.
    body = re.sub(r"<!--.*?-->\n?", "", body, flags=re.S)
    body = re.sub(
        r"\]\((?!https?:|#)([^)]+?\.md(?:#[^)]*)?)\)",
        lambda m: "](" + site_link(section, m.group(1)) + ")",
        body,
    )
    # Anchors within the page: heading ids aren't unique across the book,
    # so these point at the page on the website too.
    body = re.sub(
        r"\]\(#([^)]+)\)",
        lambda m: "](" + site_link(section, path.name + "#" + m.group(1)) + ")",
        body,
    )
    # HTML line breaks in tables (errors.md) become pandoc hard breaks.
    body = body.replace("<br>", " ")
    title = meta.get("title") or path.stem.replace("-", " ").capitalize()
    return meta.get("order", 999), title, f"## {title}\n\n{body}\n"


def main():
    out = []
    readme = DOCS / "README.md"
    if readme.exists():
        _, _, text = page(readme, ".")
        out.append("# About these docs\n\n" + text.split("\n", 1)[1])
    for folder, label in SECTIONS:
        d = DOCS / folder
        if not d.is_dir():
            continue
        pages = sorted(
            (page(p, folder) for p in d.glob("*.md")),
            key=lambda t: (t[0], t[1]),
        )
        if not pages:
            continue
        out.append(f"# {label}\n")
        out.extend(text for _, _, text in pages)
    sys.stdout.write("\n".join(out))


if __name__ == "__main__":
    main()

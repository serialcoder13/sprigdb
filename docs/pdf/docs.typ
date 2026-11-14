// Pandoc template (typst writer) for the SprigDB documentation PDF.
// Level-1 headings are sections (Concepts, Connect, ...), level-2 headings are pages
// (assemble.py shifts them).

#let horizontalrule = line(start: (25%, 0%), end: (75%, 0%), stroke: 0.5pt + luma(180))

#show terms.item: it => block(breakable: false)[
  #text(weight: "bold")[#it.term]
  #block(inset: (left: 1.5em, top: -0.4em))[#it.description]
]

$if(highlighting-definitions)$
$highlighting-definitions$
$endif$

#let accent = rgb("#a5502c")
#let rule-color = luma(205)
#let code-bg = luma(246)
#let quiet = luma(105)
#let mono = ("DejaVu Sans Mono", "Menlo")

#set document(title: [$title$], author: "SprigDB project")
#set text(font: ("Libertinus Serif", "New Computer Modern"), size: 10.5pt, lang: "en")
#set par(justify: true, leading: 0.62em, spacing: 0.95em)
#set list(indent: 0.6em, spacing: 0.6em)
#set enum(indent: 0.6em, spacing: 0.6em)
#show link: set text(fill: accent)

// Running header: the current chapter, except on pages that open a part or a chapter.
#let running-header = context {
  let page-now = here().page()
  let opens = query(heading.where(level: 1).or(heading.where(level: 2)))
    .filter(h => h.location().page() == page-now)
  if opens.len() > 0 { return none }
  let before = query(selector(heading.where(level: 2)).before(here()))
  if before.len() > 0 {
    set text(size: 8.5pt, fill: quiet)
    align(right, before.last().body)
    v(-0.5em)
    line(length: 100%, stroke: 0.4pt + rule-color)
  }
}

#let running-footer = context {
  if page.numbering != none {
    align(center, text(size: 8.5pt, fill: quiet, counter(page).display(page.numbering)))
  }
}

#set page(
  paper: "a4",
  margin: (x: 2.2cm, top: 2.5cm, bottom: 2.2cm),
  header: running-header,
  footer: running-footer,
  numbering: none,
)

// Parts: a page of their own.
#show heading.where(level: 1, outlined: true): it => {
  pagebreak(weak: true)
  v(28%)
  align(center, text(size: 26pt, weight: "bold", fill: accent, it.body))
  pagebreak(weak: true)
}

// Chapters: start on a new page.
#show heading.where(level: 2): it => {
  pagebreak(weak: true)
  v(0.8cm)
  block(width: 100%, below: 1.3em, {
    text(size: 21pt, weight: "bold", fill: accent, it.body)
    v(-0.35em)
    line(length: 100%, stroke: 0.9pt + accent)
  })
}

#show heading.where(level: 3): it => block(above: 1.6em, below: 0.8em,
  text(size: 14pt, weight: "bold", fill: accent, it.body))
#show heading.where(level: 4): it => block(above: 1.3em, below: 0.65em,
  text(size: 11.5pt, weight: "bold", it.body))
#show heading.where(level: 5): it => block(above: 1.1em, below: 0.55em,
  text(size: 10.5pt, weight: "bold", style: "italic", it.body))

// Code and diagrams.
#show raw.where(block: true): it => block(
  width: 100%, fill: code-bg, inset: (x: 8pt, y: 7pt), radius: 3pt,
  stroke: 0.4pt + rule-color, breakable: true,
  text(font: mono, size: 7.6pt, it),
)
// Inline code uses highlight, not box, so long spans can wrap at spaces. No extent or
// radius: with them each run after an inserted break clips the glyph before it.
#show raw.where(block: false): it => highlight(
  fill: code-bg, top-edge: "ascender", bottom-edge: "descender",
  text(font: mono, size: 0.86em, it),
)
// Very long spans without spaces (sql_mode lists and the like) may also wrap after , _ and .
#show raw.where(block: false): it => {
  if it.text.len() > 40 { show regex("[,_.]"): m => m.text + sym.zws; it } else { it }
}

// Tables.
#set table(
  inset: (x: 5pt, y: 4.5pt),
  stroke: (x, y) => if y == 0 { (bottom: 0.8pt + accent) } else { (bottom: 0.3pt + rule-color) },
  fill: (x, y) => if y == 0 { luma(238) } else { none },
)
#show table: set text(size: 9pt)
#show table: set align(left)
#show table: set par(justify: false)
#show table.cell.where(y: 0): set text(weight: "bold")
// Let long identifiers in narrow cells (metric and setting names) wrap after _ and .
#show table.cell: it => {
  show raw.where(block: false): r => { show regex("[_.]"): m => m.text + sym.zws; r }
  it
}
#show figure.where(kind: table): set block(breakable: true)

// Contents.
#show outline.entry.where(level: 1): it => {
  v(0.9em, weak: true)
  strong(it)
}

// Title page.
#page(header: none, footer: none, numbering: none)[
  #v(14%)
  #align(center, image("mascot.png", width: 38%))
  #v(1.2em)
  #align(center, text(size: 34pt, weight: "bold", fill: accent)[SprigDB])
  #align(center, text(size: 18pt)[Documentation])
  #v(0.6em)
  #align(center, text(size: 12pt, fill: luma(70))[$subtitle$])
  #v(1fr)
  #line(length: 100%, stroke: 0.6pt + accent)
  #text(size: 10pt)[$date$]
  #linebreak()
  #text(size: 9pt, fill: quiet)[Apache License 2.0 · https://github.com/modlix-india/sprigdb]
]

#set page(numbering: "i")
#counter(page).update(1)
#block(below: 1.4em, text(size: 21pt, weight: "bold", fill: accent)[Contents])
#outline(title: none, depth: 2, indent: 1.4em)

#set page(numbering: "1")
#pagebreak(weak: true)
#counter(page).update(1)

$body$

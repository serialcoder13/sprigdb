"""Generates the SprigDB mascot as terminal art for the `sprig` TUI and for docs.

    python3 make_ascii.py

Writes, next to this script:
    mascot.rs        a self-contained ratatui module (art, colour masks, frames, palette)
    txt/<name>.txt   plain ASCII, for README, --help banners and terminals with no colour
    ans/<name>.ans   the same with 24-bit ANSI colour (`cat ans/hero.ans`)
    preview.html     every piece rendered with its colours, for review

Every piece is pure ASCII so it works in any terminal font. Each art line has a colour mask
line; each mask letter picks a palette colour. Unpainted characters are teal (DEFAULT_KEY).
"""
import html
import os

HERE = os.path.dirname(os.path.abspath(__file__))

# key -> (name, sRGB hex, xterm-256 index for terminals without truecolor)
PALETTE = {
    't': ('teal', '#35C2B5', 37),
    'd': ('teal_dark', '#1E968B', 30),
    'l': ('lime', '#A3D13A', 149),
    's': ('leaf_dark', '#6E9E22', 106),
    'b': ('stem', '#8A5A35', 95),
    'k': ('ink', '#0B1A1E', 16),
    'w': ('white', '#FFFFFF', 231),
    'g': ('gold', '#F2AE1C', 214),
    'e': ('emblem', '#2E7D32', 28),
    'p': ('cheek', '#F08A6A', 209),
    'r': ('mouth', '#C0392B', 160),
    'y': ('star', '#FFD43B', 220),
    'c': ('bandage', '#F3E7C9', 230),
    'm': ('muted', '#7F9AA0', 109),
}

DEFAULT_KEY = 't'

# --------------------------------------------------------------------------- the art
# Each entry: name -> (art lines, mask lines). Masks must match the art cell for cell.

def block(text):
    """Art as a raw block; the first and last newline are dropped, lines keep their spacing."""
    return text.strip('\n').split('\n')


def paint(art, rules):
    """Builds a mask from rules: (rows, (col_from, col_to), chars or None, key), later rules win.
    rows/cols are inclusive ranges; chars=None paints every non-space character in the region."""
    mask = [[' '] * len(line) for line in art]
    for rows, cols, chars, key in rules:
        r0, r1 = rows if isinstance(rows, tuple) else (rows, rows)
        c0, c1 = cols
        for r in range(r0, min(r1, len(art) - 1) + 1):
            for c in range(c0, min(c1, len(art[r]) - 1) + 1):
                ch = art[r][c]
                if ch != ' ' and (chars is None or ch in chars):
                    mask[r][c] = key
    return [''.join(m) for m in mask]


def variant(art, edits):
    """A frame: art with (row, col, text) overwrites."""
    out = list(art)
    for r, c, text in edits:
        line = out[r].ljust(c + len(text))
        out[r] = (line[:c] + text + line[c + len(text):]).rstrip()
    return out


ALL = (0, 99)

# ---- hero: the full figure, 16 rows x 34 cols (splash, `sprig --version`, docs)
HERO = block(r"""
                ,\ /,
                 \|/
          .-~~-'''V'''-~~-.
     \\\ /                 \ ///
    \\\\\|   .-.     .-.   |/////
   ======|   (O)     (@)   |======
    /////|   '-' ._. '-'   |\\\\\
     ///  \  ,  \_uu_/  ,  / \\\
           '-.._______..-'
           .-'         '-.
        .-./   .-----.   \.-.
        (  |   /  *  \   |cc)
        '-'|   \     /   |'-'
           |    '---'    |~~~.
            \  .-.  .-.  /~~~~'>
             (___)  (___)
""")
HERO_RULES = [
    ((0, 1), (14, 22), ",\\/", 'l'), (1, (17, 17), "|", 'b'),
    (2, (14, 22), "'", 'l'), (2, (18, 18), "V", 'd'),
    ((3, 7), (0, 8), None, 'l'), ((3, 7), (28, 40), None, 'l'),
    (5, (0, 8), "=", 'd'), (5, (28, 40), "=", 'd'),
    ((4, 6), (13, 15), None, 'w'),
    ((4, 6), (21, 23), None, 'g'), (5, (22, 22), "@", 'e'),
    (7, (11, 25), ",", 'p'), (7, (16, 21), None, 'r'), (7, (18, 19), "u", 'w'),
    ((10, 13), (14, 22), None, 'l'), (11, (18, 18), "*", 'y'),
    (11, (26, 27), "c", 'c'),
    (13, (26, 40), None, 's'), (14, (26, 40), None, 'd'),
]

# ---- buddy: the compact figure, 6 rows x 21 cols (TUI header, empty states, toasts)
BUDDY_X = 3  # left margin for the waving arm
BUDDY = block(r"""
       ,\|/,
  \\ .-''V''-. //
  ==| (O) (@) |==
  // \ ,\uu/, / \\
    .'  .*.  '.~~.
    '-(_)-(_)-'~~'
""")
BUDDY = [' ' * BUDDY_X + line for line in BUDDY]
X = BUDDY_X
BUDDY_RULES = [
    (0, ALL, None, 'l'), (0, (X + 9, X + 9), "|", 'b'),
    (1, (X + 6, X + 12), "'", 'l'), (1, (X + 9, X + 9), "V", 'd'),
    ((1, 3), (X + 0, X + 3), None, 'l'), ((1, 3), (X + 15, X + 20), None, 'l'),
    (2, (X + 0, X + 3), "=", 'd'), (2, (X + 15, X + 20), "=", 'd'),
    (2, (X + 6, X + 8), None, 'w'), (2, (X + 10, X + 12), None, 'g'), (2, (X + 11, X + 11), "@", 'e'),
    (3, (X + 7, X + 7), ",", 'p'), (3, (X + 12, X + 12), ",", 'p'),
    (3, (X + 8, X + 11), None, 'r'), (3, (X + 9, X + 10), "uU", 'w'),
    (4, (X + 8, X + 10), None, 'l'), (4, (X + 9, X + 9), "*", 'y'),
    (4, (X + 15, X + 20), None, 's'), (5, (X + 15, X + 20), None, 'd'),
]


def buddy(edits, extra=()):
    art = variant(BUDDY, edits)
    return art, paint(art, BUDDY_RULES + list(extra))


def hero(edits, extra=()):
    art = variant(HERO, edits)
    return art, paint(art, HERO_RULES + list(extra))


PIECES = {
    'hero': hero([]),
    'hero-blink': hero([(5, 14, '-')]),
    'buddy': buddy([]),
    'buddy-blink': buddy([(2, X + 7, '-')]),
    'buddy-wave-1': buddy([(2, 0, 'o'), (3, 1, '\\_____')], [((2, 3), (0, 6), None, 't')]),
    'buddy-wave-2': buddy([(1, 1, 'o'), (2, 1, '|'), (3, 1, '\\_____')],
                          [((1, 3), (0, 6), None, 't')]),
    'buddy-sleep': buddy([(2, X + 6, '(-)'), (3, X + 8, '\\__/'), (0, X + 15, 'z Z')],
                         [(0, (X + 15, X + 17), None, 'm')]),
    'buddy-sad': buddy([(2, X + 6, '(;)'), (3, X + 8, '/--\\'), (0, X + 15, '.--.')],
                       [(0, (X + 15, X + 18), None, 'm'), (2, (X + 7, X + 7), ";", 'w')]),
    'buddy-celebrate': buddy([(2, X + 6, '(^)'), (3, X + 8, '\\UU/'), (0, 0, '*  .'), (0, X + 15, '.  *'),
                              (4, 0, '*'), (4, X + 20, '  +'), (5, 1, '.')],
                             [(0, (0, 3), None, 'y'), (0, (X + 15, X + 18), None, 'y'), (4, (0, 0), None, 'g'),
                              (4, (X + 20, X + 22), None, 'g'), (5, (1, 1), None, 'y')]),
    'buddy-think': buddy([(2, X + 6, '(o)'), (0, X + 15, '?')], [(0, (X + 15, X + 15), None, 'w')]),
}

# frames for the TUI's animations: (piece, ticks to hold); a tick is the caller's choice (100 ms suits)
ANIMATIONS = {
    'hero': [('hero', 40), ('hero-blink', 2)],
    'idle': [('buddy', 30), ('buddy-blink', 2)],
    'wave': [('buddy-wave-1', 4), ('buddy-wave-2', 4)],
    'celebrate': [('buddy-celebrate', 4), ('buddy', 2)],
    'sleep': [('buddy-sleep', 30)],
    'sad': [('buddy-sad', 30)],
    'think': [('buddy-think', 20), ('buddy-blink', 2)],
}


# --------------------------------------------------------------------------- checks
def check(name, art, mask):
    assert len(art) == len(mask), f'{name}: {len(art)} art lines, {len(mask)} mask lines'
    for i, (a, m) in enumerate(zip(art, mask)):
        a, m = a.rstrip(), m.rstrip()
        assert all(ch.isascii() for ch in a), f'{name}:{i}: non-ASCII art'
        for j, ch in enumerate(a):
            key = m[j] if j < len(m) else ' '
            if key != ' ' and key not in PALETTE:
                raise AssertionError(f'{name}:{i}:{j}: unknown mask key {key!r}')


def runs(art_line, mask_line):
    """Split a line into (text, key) runs; spaces get key ' '."""
    out = []
    for j, ch in enumerate(art_line):
        key = (mask_line[j] if j < len(mask_line) else ' ') if ch != ' ' else ' '
        if ch != ' ' and key == ' ':
            key = DEFAULT_KEY
        if out and out[-1][1] == key:
            out[-1][0] += ch
        else:
            out.append([ch, key])
    return out


# --------------------------------------------------------------------------- outputs
def write_txt_ans():
    os.makedirs(os.path.join(HERE, 'txt'), exist_ok=True)
    os.makedirs(os.path.join(HERE, 'ans'), exist_ok=True)
    for name, (art, mask) in PIECES.items():
        with open(os.path.join(HERE, 'txt', f'{name}.txt'), 'w') as f:
            f.write('\n'.join(l.rstrip() for l in art) + '\n')
        lines = []
        for a, m in zip(art, mask):
            s = ''
            for text, key in runs(a.rstrip(), m):
                if key == ' ':
                    s += text
                else:
                    h = PALETTE[key][1].lstrip('#')
                    r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
                    s += f'\x1b[38;2;{r};{g};{b}m{text}\x1b[0m'
            lines.append(s)
        with open(os.path.join(HERE, 'ans', f'{name}.ans'), 'w') as f:
            f.write('\n'.join(lines) + '\n')


def write_preview():
    blocks = []
    for name, (art, mask) in PIECES.items():
        rows = []
        for a, m in zip(art, mask):
            row = ''
            for text, key in runs(a.rstrip(), m):
                t = html.escape(text)
                row += t if key == ' ' else f'<span style="color:{PALETTE[key][1]}">{t}</span>'
            rows.append(row)
        blocks.append(f'<figure><figcaption>{name}</figcaption><pre>' + '\n'.join(rows) + '</pre></figure>')
    page = ('<!doctype html><meta charset="utf-8"><title>Sprig ASCII Mascot</title><style>'
            'body{margin:0;padding:16px;background:#0f1d22;color:#cfe;font:13px system-ui;display:flex;'
            'flex-wrap:wrap;gap:24px;align-items:flex-start}figure{margin:0}figcaption{opacity:.6;margin-bottom:4px}'
            'pre{margin:0;font:16px/1.15 Menlo,monospace;background:#12313A;padding:12px 16px;border-radius:8px}'
            '</style>' + ''.join(blocks))
    with open(os.path.join(HERE, 'preview.html'), 'w') as f:
        f.write(page)


def resolved_mask(art, mask):
    """The mask with every unpainted visible character set to DEFAULT_KEY (what the TUI uses)."""
    out = []
    for a, m in zip(art, mask):
        row = ''
        for j, ch in enumerate(a.rstrip()):
            key = m[j] if j < len(m) else ' '
            row += (key if key != ' ' else DEFAULT_KEY) if ch != ' ' else ' '
        out.append(row)
    return out


def rs_str(text):
    return 'r#"' + text + '"#'


def write_rust():
    ident = lambda n: n.upper().replace('-', '_')
    out = ['//! The SprigDB mascot (the teal axolotl with the leaf-button eye) as terminal art for the',
           '//! `sprig` TUI. Pure ASCII, coloured per character from the brand palette.',
           '//!',
           '//! Generated by `brand/mascot/ascii/make_ascii.py`; edit the art there and regenerate.',
           '//! Self-contained: depends only on ratatui. Drop it into the TUI and call [`render`].',
           '',
           'use ratatui::style::{Color, Style};',
           'use ratatui::text::{Line, Span, Text};',
           '',
           '/// One piece of art: its lines and, cell for cell, a colour key per character.',
           '#[derive(Debug, Clone, Copy)]',
           'pub struct Art {',
           '    pub name: &\'static str,',
           '    pub lines: &\'static [&\'static str],',
           '    pub mask: &\'static [&\'static str],',
           '}',
           '',
           'impl Art {',
           '    /// Width in terminal cells (the art is ASCII, one cell per byte).',
           '    pub fn width(&self) -> u16 {',
           '        self.lines.iter().map(|l| l.len()).max().unwrap_or(0) as u16',
           '    }',
           '',
           '    pub fn height(&self) -> u16 {',
           '        self.lines.len() as u16',
           '    }',
           '}',
           '',
           '/// How much colour the terminal supports.',
           '#[derive(Debug, Clone, Copy, PartialEq, Eq)]',
           'pub enum ColourMode {',
           '    /// No colour (`NO_COLOR`, dumb terminals, piped output).',
           '    None,',
           '    /// The xterm 256-colour palette (macOS Terminal before 26, tmux without `Tc`).',
           '    Ansi256,',
           '    /// 24-bit colour.',
           '    TrueColor,',
           '}',
           '',
           '/// The colour for a mask key, or `None` for the terminal\'s default.',
           'pub fn colour(key: u8, mode: ColourMode) -> Option<Color> {',
           '    let (rgb, idx): ((u8, u8, u8), u8) = match key {']
    for k, (name, hexc, idx) in PALETTE.items():
        h = hexc.lstrip('#')
        r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
        out.append(f"        b'{k}' => (({r}, {g}, {b}), {idx}), // {name} {hexc}")
    out += ['        _ => return None,',
            '    };',
            '    match mode {',
            '        ColourMode::None => None,',
            '        ColourMode::Ansi256 => Some(Color::Indexed(idx)),',
            '        ColourMode::TrueColor => Some(Color::Rgb(rgb.0, rgb.1, rgb.2)),',
            '    }',
            '}',
            '',
            '/// Renders a piece of art as ratatui text, one span per run of same-coloured characters.',
            'pub fn render(art: &Art, mode: ColourMode) -> Text<\'static> {',
            '    let mut lines = Vec::with_capacity(art.lines.len());',
            '    for (line, mask) in art.lines.iter().zip(art.mask.iter()) {',
            '        let (lb, mb) = (line.as_bytes(), mask.as_bytes());',
            '        let mut spans: Vec<Span<\'static>> = Vec::new();',
            '        let mut start = 0;',
            '        while start < lb.len() {',
            '            let key = |i: usize| if lb[i] == b\' \' { b\' \' } else { mb.get(i).copied().unwrap_or(b\' \') };',
            '            let k = key(start);',
            '            let mut end = start + 1;',
            '            while end < lb.len() && key(end) == k {',
            '                end += 1;',
            '            }',
            '            let text = &line[start..end];',
            '            spans.push(match colour(k, mode) {',
            '                Some(c) => Span::styled(text, Style::new().fg(c)),',
            '                None => Span::raw(text),',
            '            });',
            '            start = end;',
            '        }',
            '        lines.push(Line::from(spans));',
            '    }',
            '    Text::from(lines)',
            '}',
            '',
            '/// One frame of an animation: the art and how many ticks to hold it.',
            '#[derive(Debug, Clone, Copy)]',
            'pub struct Frame {',
            '    pub art: &\'static Art,',
            '    pub ticks: u16,',
            '}',
            '',
            '/// The frame to show at `tick` (a tick is the caller\'s choice; 100 ms suits), looping.',
            'pub fn frame_at(anim: &[Frame], tick: u64) -> &\'static Art {',
            '    let total: u64 = anim.iter().map(|f| u64::from(f.ticks.max(1))).sum();',
            '    let mut t = tick % total.max(1);',
            '    for f in anim {',
            '        let n = u64::from(f.ticks.max(1));',
            '        if t < n {',
            '            return f.art;',
            '        }',
            '        t -= n;',
            '    }',
            '    anim[0].art',
            '}',
            '']
    for name, (art, mask) in PIECES.items():
        rm = resolved_mask(art, mask)
        out.append(f'pub const {ident(name)}: Art = Art {{')
        out.append(f'    name: "{name}",')
        out.append('    lines: &[')
        out += [f'        {rs_str(l.rstrip())},' for l in art]
        out.append('    ],')
        out.append('    mask: &[')
        out += [f'        {rs_str(m)},' for m in rm]
        out.append('    ],')
        out.append('};')
        out.append('')
    for anim, frames in ANIMATIONS.items():
        out.append(f'pub const ANIM_{anim.upper()}: &[Frame] = &[')
        out += [f'    Frame {{ art: &{ident(p)}, ticks: {t} }},' for p, t in frames]
        out.append('];')
        out.append('')
    out.append('/// Every piece, for galleries and tests.')
    out.append('pub const ALL: &[&Art] = &[' + ', '.join('&' + ident(n) for n in PIECES) + '];')
    out += ['',
            '#[cfg(test)]',
            'mod tests {',
            '    use super::*;',
            '',
            '    #[test]',
            '    fn masks_match_the_art() {',
            '        for art in ALL {',
            '            assert_eq!(art.lines.len(), art.mask.len(), "{}", art.name);',
            '            for (l, m) in art.lines.iter().zip(art.mask.iter()) {',
            '                assert!(l.is_ascii(), "{}: non-ASCII", art.name);',
            '                assert_eq!(l.len(), m.len(), "{}: {l:?}", art.name);',
            '                for (c, k) in l.bytes().zip(m.bytes()) {',
            '                    assert_eq!(c == b\' \', k == b\' \', "{}: {l:?}", art.name);',
            '                    assert!(k == b\' \' || colour(k, ColourMode::TrueColor).is_some(), "{}", art.name);',
            '                }',
            '            }',
            '        }',
            '    }',
            '',
            '    #[test]',
            '    fn render_keeps_the_text() {',
            '        for art in ALL {',
            '            let text = render(art, ColourMode::TrueColor);',
            '            for (line, src) in text.lines.iter().zip(art.lines.iter()) {',
            '                let s: String = line.spans.iter().map(|s| s.content.as_ref()).collect();',
            '                assert_eq!(&s, src);',
            '            }',
            '        }',
            '    }',
            '',
            '    #[test]',
            '    fn animations_loop() {',
            '        assert_eq!(frame_at(ANIM_IDLE, 0).name, "buddy");',
            '        assert_eq!(frame_at(ANIM_IDLE, 30).name, "buddy-blink");',
            '        assert_eq!(frame_at(ANIM_IDLE, 32).name, "buddy");',
            '    }',
            '}',
            '']
    with open(os.path.join(HERE, 'mascot.rs'), 'w') as f:
        f.write('\n'.join(out))


if __name__ == '__main__':
    for n, (a, m) in PIECES.items():
        check(n, a, m)
    write_txt_ans()
    write_preview()
    write_rust()
    print('ok:', ', '.join(PIECES))

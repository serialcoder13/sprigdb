"""The coral-gold colourway of the SprigDB mascot (chosen by Kiran, 12 Nov 2026).

Inspired by a pink axolotl's palette: ivory body, pink-to-coral gills, dark eyes. Works on the
VTracer SVGs in sprigdb/brand/mascot/svg (unchanged): every <path> is a translate(x,y) plus
coordinates, so each shape's bounding box is known and parts that share a colour can be
told apart by where they sit. The sprout on top of the head stays green ("sprig").

Usage: python3 coralgold.py <out_dir> [pose ...]
"""
import colorsys, pathlib, re, sys

SRC = pathlib.Path(__file__).resolve().parents[3] / "sprigdb/brand/mascot/svg"
PATH = re.compile(r'<path d="([^"]*)" fill="(#[0-9A-Fa-f]{6})" transform="translate\(([-\d.]+),([-\d.]+)\)"/>')

def hls(hex_):
    r, g, b = (int(hex_[i:i + 2], 16) / 255 for i in (1, 3, 5))
    return colorsys.rgb_to_hls(r, g, b)

def to_hex(h, l, s):
    r, g, b = colorsys.hls_to_rgb(h % 1.0, max(0, min(1, l)), max(0, min(1, s)))
    return "#%02X%02X%02X" % (round(r * 255), round(g * 255), round(b * 255))

def ramp(x, stops):
    """Interpolate (hue degrees, saturation, lightness) along stops keyed by input lightness."""
    if x <= stops[0][0]:
        h, s, l = stops[0][1]
        return to_hex(h / 360, l, s)
    for (x0, a), (x1, b) in zip(stops, stops[1:]):
        if x <= x1:
            t = (x - x0) / (x1 - x0)
            dh = ((b[0] - a[0] + 180) % 360) - 180          # shortest way round the hue circle
            h = (a[0] + dh * t) % 360
            return to_hex(h / 360, a[2] + (b[2] - a[2]) * t, a[1] + (b[1] - a[1]) * t)
    h, s, l = stops[-1][1]
    return to_hex(h / 360, l, s)

def bbox(d, tx, ty):
    nums = [float(n) for n in re.findall(r'-?\d+(?:\.\d+)?', d)]
    xs, ys = nums[0::2], nums[1::2]
    return tx + min(xs), ty + min(ys), tx + max(xs), ty + max(ys)

# Palettes. Ramps are (input lightness, (hue°, saturation, lightness)) stops; the input is the
# source pixel's lightness, so the drawing's light/dark modelling is kept.
PALETTES = {
    "coralgold": dict(
        edge=(22, 0.55, 0.78),
        body=[(0.12, (8, 0.42, 0.70)), (0.28, (16, 0.42, 0.84)), (0.40, (28, 0.46, 0.91)), (0.70, (40, 0.55, 0.95))],
        gill=[(0.15, (350, 0.55, 0.45)), (0.40, (4, 0.66, 0.58)), (0.65, (20, 0.76, 0.64)), (0.88, (42, 0.86, 0.62))],
        belly=(34, 0.55, 0.82), sprout=(95, 0.50), slate=(215, 0.25), warm=0.90),
}
P = PALETTES["coralgold"]

def recolour(hex_, part):
    """part: sprout, emblem, belly, gill (green shapes) or frill (teal shapes)."""
    h, l, s = hls(hex_)
    deg = h * 360
    if s < 0.12 or l < 0.10 or l > 0.95:
        return hex_                                   # outlines, eyes, whites
    if 150 <= deg < 200:                              # teal family
        if s > 0.85 and l > 0.40:                     # the bright sticker glow
            eh, es, el = P["edge"]
            return to_hex(eh / 360, el, es)
        if part == "frill":                           # teal frills around the head are gills too
            return ramp(l, P["gill"])
        return ramp(l, P["body"])
    if 200 <= deg < 260:                              # blue accents: laptop, database
        sh, sf = P["slate"]
        return to_hex(sh / 360, l, s * sf)
    if 60 <= deg < 150:                               # leaves and belly
        if part in ("sprout", "emblem"):
            gh, gf = P["sprout"]
            return to_hex(gh / 360, min(0.62, l + 0.04), s * gf)
        if part == "belly":
            bh, bs, bl = P["belly"]
            return to_hex(bh / 360, bl + 0.13 * l, bs)
        return ramp(l, P["gill"])
    return to_hex(h, l + 0.03, s * P["warm"])         # gold monocle, cheeks, mouth, star

def main():
    global P
    args = sys.argv[1:]
    if args and args[0].startswith("--palette="):
        P = PALETTES[args.pop(0).split("=", 1)[1]]
    out = pathlib.Path(args[0]); out.mkdir(parents=True, exist_ok=True)
    poses = args[1:] or [p.stem for p in sorted(SRC.glob("*.svg"))]
    for pose in poses:
        svg = (SRC / f"{pose}.svg").read_text()
        shapes = [(m, bbox(m.group(1), float(m.group(3)), float(m.group(4)))) for m in PATH.finditer(svg)]
        greens = [(m, b) for m, b in shapes if 60 <= hls(m.group(2))[0] * 360 < 150 and hls(m.group(2))[2] >= 0.12]
        allb = [b for _, b in shapes if (b[2] - b[0]) < 1200]
        X0 = min(b[0] for b in allb); X1 = max(b[2] for b in allb)
        Y0 = min(b[1] for b in allb); Y1 = max(b[3] for b in allb)
        W, H = X1 - X0, Y1 - Y0
        cx = X0 + W / 2
        def part(b):
            bw, bh = b[2] - b[0], b[3] - b[1]
            mx, my = (b[0] + b[2]) / 2, (b[1] + b[3]) / 2
            if b[3] < Y0 + 0.24 * H and abs(mx - cx) < 0.16 * W:
                return "sprout"                        # above the head, near the middle
            if my > Y0 + 0.50 * H and abs(mx - cx) < 0.22 * W and bw < 0.45 * W:
                return "belly"                         # lower half, central
            return "gill"
        # Leaf emblems (on the monocle, the laptop) sit inside a larger gold or grey shape.
        holders = [b for m, b in shapes
                   if (35 <= hls(m.group(2))[0] * 360 < 60 or 200 <= hls(m.group(2))[0] * 360 < 260 or hls(m.group(2))[2] < 0.12)
                   and (b[2] - b[0]) < 0.5 * W]
        def inside(b):
            return any(h[0] <= b[0] and h[1] <= b[1] and b[2] <= h[2] and b[3] <= h[3]
                       and (h[2] - h[0]) * (h[3] - h[1]) > 2.5 * max(1.0, (b[2] - b[0]) * (b[3] - b[1]))
                       for h in holders)
        parts = {id(m): ("emblem" if inside(b) else part(b)) for m, b in greens}
        for m, b in shapes:
            h_, l_, s_ = hls(m.group(2))
            if 150 <= h_ * 360 < 200 and s_ <= 0.85:
                mx, my = (b[0] + b[2]) / 2, (b[1] + b[3]) / 2
                bw, bh = b[2] - b[0], b[3] - b[1]
                if my < Y0 + 0.50 * H and abs(mx - cx) > 0.17 * W and bw < 0.30 * W and bh < 0.32 * H:
                    parts[id(m)] = "frill"
        def sub(m):
            return m.group(0).replace(m.group(2), recolour(m.group(2), parts.get(id(m), "gill")))
        out_svg = svg
        for m, _ in reversed(shapes):
            out_svg = out_svg[:m.start()] + sub(m) + out_svg[m.end():]
        # colours outside <path> (the avatar's circles): background disc and ring
        out_svg = re.sub(r'(<circle[^>]*fill=")(#[0-9A-Fa-f]{6})', lambda mm: mm.group(1) + recolour(mm.group(2), "gill"), out_svg)
        (out / f"{pose}.svg").write_text(out_svg)

if __name__ == "__main__":
    main()

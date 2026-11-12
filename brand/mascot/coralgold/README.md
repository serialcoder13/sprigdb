# Coral-gold colourway (12 November 2026)

The colourway of the mascot that Studio and the TUI use, chosen by Kiran. The teal originals one level up are unchanged.

| Path | Contents |
|---|---|
| `svg/` | The 9 poses, smoothed and re-traced (VTracer spline mode). Each SVG is 2048 units with a 512 px default size |
| `png/` | The 9 poses at 1024 px, with smoothed edges |
| `make_coralgold.py` | Recolours the teal SVGs into the colourway, separating the parts by where each shape sits |

Studio's small web copies (WebP at 48, 96, 220 and 440 px) live in `studio/public/mascot/`. The 2048 px masters and the design notes are in `sprigdb-plan/design/mascot-calm/`.

How these were made:
1. Recolour the teal SVGs with `make_coralgold.py`.
2. Render each pose at 4096 px. The avatar is rendered in Chrome, because its viewBox and clip need a full SVG renderer.
3. Apply a 17 px median filter and halve the size, to 2048 px.
4. Trace with VTracer: `vtracer -m spline -f 16 -p 7 -g 14 -c 70 -l 6 -s 50 --path_precision 2`.

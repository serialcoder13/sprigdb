// QR codes for TOTP enrolment: the `otpauth://` link
// as a symbol an authenticator app can scan. The `qrcode` package only builds
// the modules; Studio draws them as one SVG path, so nothing is rendered from
// a markup string and the CSP needs nothing new.

import QRCode from 'qrcode';

export interface QrPath {
  /** Modules per side, without the quiet zone. */
  size: number;
  /** One SVG path drawing every dark module, offset by the quiet zone. */
  d: string;
  /** Side of the view box: size plus a 4-module quiet zone each side. */
  box: number;
}

export const QUIET_ZONE = 4;

/** The dark modules of the symbol for `text`, as an SVG path. */
export function qrPath(text: string): QrPath {
  // M (15%) is what authenticator apps and most generators use.
  const { modules } = QRCode.create(text, { errorCorrectionLevel: 'M' });
  const size = modules.size;
  const parts: string[] = [];
  for (let y = 0; y < size; y++) {
    let x = 0;
    while (x < size) {
      if (!modules.get(y, x)) {
        x++;
        continue;
      }
      // A run of dark modules on one row is one rectangle.
      let run = 1;
      while (x + run < size && modules.get(y, x + run)) run++;
      parts.push(`M${x + QUIET_ZONE} ${y + QUIET_ZONE}h${run}v1h-${run}z`);
      x += run;
    }
  }
  return { size, d: parts.join(''), box: size + 2 * QUIET_ZONE };
}

/** Whether a module is dark, read back from a path (for tests). */
export function darkModules(p: QrPath): Set<string> {
  const dark = new Set<string>();
  for (const m of p.d.matchAll(/M(\d+) (\d+)h(\d+)/g)) {
    const x = Number(m[1]) - QUIET_ZONE;
    const y = Number(m[2]) - QUIET_ZONE;
    for (let i = 0; i < Number(m[3]); i++) dark.add(`${x + i},${y}`);
  }
  return dark;
}

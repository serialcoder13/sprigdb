import { describe, expect, it } from 'vitest';
import QRCode from 'qrcode';
import { darkModules, qrPath, QUIET_ZONE } from './qr';

const uri =
  'otpauth://totp/SprigDB:acme.app?secret=JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP&issuer=SprigDB&algorithm=SHA1&digits=6&period=30';

describe('TOTP QR code (ST33)', () => {
  it('draws exactly the modules the encoder produced', () => {
    const p = qrPath(uri);
    const { modules } = QRCode.create(uri, { errorCorrectionLevel: 'M' });
    expect(p.size).toBe(modules.size);
    expect(p.box).toBe(modules.size + 2 * QUIET_ZONE);
    const dark = darkModules(p);
    for (let y = 0; y < p.size; y++)
      for (let x = 0; x < p.size; x++) expect(dark.has(`${x},${y}`)).toBe(modules.get(y, x) === 1);
  });

  it('has the three finder patterns and a size of 17 + 4 × version', () => {
    const p = qrPath(uri);
    expect((p.size - 17) % 4).toBe(0);
    const dark = darkModules(p);
    const finder = (ox: number, oy: number) => {
      for (let y = 0; y < 7; y++)
        for (let x = 0; x < 7; x++) {
          const ring = x === 0 || y === 0 || x === 6 || y === 6;
          const core = x >= 2 && x <= 4 && y >= 2 && y <= 4;
          expect(dark.has(`${ox + x},${oy + y}`)).toBe(ring || core);
        }
    };
    finder(0, 0);
    finder(p.size - 7, 0);
    finder(0, p.size - 7);
  });

  it('a longer link needs a larger symbol, and the path is only rectangles', () => {
    const small = qrPath('otpauth://totp/a?secret=AAAA');
    const big = qrPath(uri);
    expect(big.size).toBeGreaterThan(small.size);
    expect(big.d).toMatch(/^(M\d+ \d+h\d+v1h-\d+z)+$/);
  });
});

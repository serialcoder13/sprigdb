import { useMemo } from 'react';
import { qrPath } from './qr';

/** A scannable QR code for an `otpauth://` link, drawn as SVG. Dark modules
 * on a white field in every theme: scanners need the contrast. */
export function QrCode({ text, label, px = 200 }: Readonly<{ text: string; label: string; px?: number }>) {
  const p = useMemo(() => qrPath(text), [text]);
  return (
    <svg
      role="img"
      aria-label={label}
      data-testid="totp-qr"
      width={px}
      height={px}
      viewBox={`0 0 ${p.box} ${p.box}`}
      shapeRendering="crispEdges"
      style={{ background: '#fff', borderRadius: 4 }}
    >
      <rect width={p.box} height={p.box} fill="#fff" />
      <path d={p.d} fill="#000" />
    </svg>
  );
}

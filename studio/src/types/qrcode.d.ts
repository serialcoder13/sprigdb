// The part of the `qrcode` package (MIT) Studio uses: building the symbol's
// modules, which Studio draws itself as SVG (no canvas, no markup strings).
declare module 'qrcode' {
  export interface QRCodeCreateOptions {
    errorCorrectionLevel?: 'L' | 'M' | 'Q' | 'H';
    version?: number;
  }
  export interface BitMatrix {
    size: number;
    data: Uint8Array;
    get(row: number, col: number): number;
  }
  export interface QRCodeSymbol {
    modules: BitMatrix;
    version: number;
  }
  export function create(text: string, options?: QRCodeCreateOptions): QRCodeSymbol;
  const QRCode: { create: typeof create };
  export default QRCode;
}

// Message and record bodies (chapter 44, "Sinks": bodies show as text, JSON
// or hex). The API sends binary cells as base64.

/** A base64 cell as UTF-8 text, else as `0x` hex; null stays null. */
export function binaryText(b64: string | null): string | null {
  if (b64 === null) return null;
  let bytes: Uint8Array;
  try {
    bytes = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
  } catch {
    return b64;
  }
  try {
    return new TextDecoder('utf-8', { fatal: true }).decode(bytes);
  } catch {
    return `0x${Array.from(bytes, (x) => x.toString(16).padStart(2, '0')).join('')}`;
  }
}

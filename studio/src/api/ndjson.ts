// NDJSON decoding (Sql.Query streams one JSON message per line).
// Bytes arrive in arbitrary chunks; a line may span chunks, and UTF-8
// sequences may be split.

export class NdjsonDecoder {
  private readonly text = new TextDecoder('utf-8');
  private rest = '';

  /** The complete lines in `chunk`, parsed. */
  push(chunk: Uint8Array): unknown[] {
    this.rest += this.text.decode(chunk, { stream: true });
    return this.drain(false);
  }

  /** Whatever is left at the end of the stream. */
  end(): unknown[] {
    this.rest += this.text.decode();
    return this.drain(true);
  }

  private drain(final: boolean): unknown[] {
    const out: unknown[] = [];
    let nl = this.rest.indexOf('\n');
    while (nl >= 0) {
      const line = this.rest.slice(0, nl).trim();
      this.rest = this.rest.slice(nl + 1);
      if (line) out.push(JSON.parse(line));
      nl = this.rest.indexOf('\n');
    }
    if (final && this.rest.trim()) {
      out.push(JSON.parse(this.rest.trim()));
      this.rest = '';
    }
    return out;
  }
}

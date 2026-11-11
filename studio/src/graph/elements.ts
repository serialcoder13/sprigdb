// Graph elements in a Cypher or GQL result (chapter 44 step 27). The gateway
// sends nodes, relationships and paths as JSON cells tagged `$node`, `$rel`
// and `$path` (chapter 37, "Languages"); scalars are text. This collects the
// distinct elements of every cell, nested lists and maps included, up to
// studio.graph_max_elements.

export type Props = Record<string, unknown>;

export interface GNode {
  id: string;
  labels: string[];
  props: Props;
}

export interface GEdge {
  id: string;
  type: string;
  start: string;
  end: string;
  props: Props;
}

export interface GraphData {
  nodes: Map<string, GNode>;
  edges: Map<string, GEdge>;
  /** Elements not drawn because of the cap. */
  dropped: number;
  /** Relationships whose ends weren't in the result (drawn with stub ends). */
  dangling: number;
}

export function emptyGraph(): GraphData {
  return { nodes: new Map(), edges: new Map(), dropped: 0, dangling: 0 };
}

const idText = (v: unknown): string | null =>
  typeof v === 'number' || typeof v === 'string' || typeof v === 'bigint' ? String(v) : null;

function isObj(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

/** Adds the elements of one decoded value. */
export function collect(v: unknown, g: GraphData, max: number): void {
  const room = () => g.nodes.size + g.edges.size < max;
  if (Array.isArray(v)) {
    for (const x of v) collect(x, g, max);
    return;
  }
  if (!isObj(v)) return;
  const n = v.$node;
  if (isObj(n)) {
    const id = idText(n.id);
    if (id === null || g.nodes.has(id)) return;
    if (!room()) {
      g.dropped++;
      return;
    }
    g.nodes.set(id, {
      id,
      labels: Array.isArray(n.labels) ? n.labels.map(String) : [],
      props: isObj(n.props) ? n.props : {},
    });
    return;
  }
  const r = v.$rel;
  if (isObj(r)) {
    const id = idText(r.id);
    const start = idText(r.start);
    const end = idText(r.end);
    if (id === null || g.edges.has(id)) return;
    // An unbound relationship (inside a path whose ends weren't filled)
    // can't be drawn.
    if (start === null || end === null) return;
    if (!room()) {
      g.dropped++;
      return;
    }
    g.edges.set(id, {
      id,
      type: typeof r.type === 'string' ? r.type : '',
      start,
      end,
      props: isObj(r.props) ? r.props : {},
    });
    return;
  }
  if (Array.isArray(v.$path)) {
    collect(v.$path, g, max);
    return;
  }
  // A map value (RETURN {a: n}) or a projection: look inside.
  for (const x of Object.values(v)) collect(x, g, max);
}

/** Parses one cell: JSON when it looks like JSON, else nothing to draw. */
export function cellValue(cell: string | null): unknown {
  if (cell === null) return null;
  const c = cell.trimStart()[0];
  if (c !== '{' && c !== '[') return null;
  try {
    return JSON.parse(cell);
  } catch {
    return null;
  }
}

/** The graph in a result's cells. */
export function graphOf(rowCount: number, colCount: number, cell: (r: number, c: number) => string | null, max: number): GraphData {
  const g = emptyGraph();
  for (let r = 0; r < rowCount; r++) for (let c = 0; c < colCount; c++) collect(cellValue(cell(r, c)), g, max);
  for (const e of g.edges.values()) if (!g.nodes.has(e.start) || !g.nodes.has(e.end)) g.dangling++;
  return g;
}

/** Whether a result holds anything to draw (decides whether "Graph" is offered). */
export function hasGraph(rowCount: number, colCount: number, cell: (r: number, c: number) => string | null): boolean {
  const rows = Math.min(rowCount, 50);
  for (let r = 0; r < rows; r++)
    for (let c = 0; c < colCount; c++) {
      const v = cell(r, c);
      if (v && (v.includes('"$node"') || v.includes('"$rel"') || v.includes('"$path"'))) return true;
    }
  return false;
}

/** Merges `more` into `g` (expanding a node), keeping the cap. */
export function merge(g: GraphData, more: GraphData, max: number): GraphData {
  const out: GraphData = { nodes: new Map(g.nodes), edges: new Map(g.edges), dropped: g.dropped, dangling: 0 };
  for (const n of more.nodes.values()) collect({ $node: n }, out, max);
  for (const e of more.edges.values()) collect({ $rel: e }, out, max);
  for (const e of out.edges.values()) if (!out.nodes.has(e.start) || !out.nodes.has(e.end)) out.dangling++;
  return out;
}

/** The caption of a node: its `name`, `title` or first string property, else its first label and id. */
export function caption(n: GNode): string {
  for (const k of ['name', 'title', 'label', 'id']) {
    const v = n.props[k];
    if (typeof v === 'string' || typeof v === 'number') return String(v);
  }
  const first = Object.values(n.props).find((v) => typeof v === 'string');
  if (typeof first === 'string') return first.length > 24 ? `${first.slice(0, 23)}…` : first;
  return `${n.labels[0] ?? 'node'} ${n.id}`;
}

/** A Cypher literal for a node id (ids are integers; anything else quoted). */
export function idLiteral(id: string): string {
  return /^-?\d+$/.test(id) ? id : `'${id.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}'`;
}

/** The bounded neighbourhood query "expand" sends. */
export function expandQuery(id: string, limit: number): string {
  return `MATCH (n)-[r]-(m) WHERE id(n) = ${idLiteral(id)} RETURN n, r, m LIMIT ${Math.max(1, Math.floor(limit))}`;
}

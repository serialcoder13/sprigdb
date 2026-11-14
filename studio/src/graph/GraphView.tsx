// The graph view: Cytoscape.js (MIT), loaded on
// first use, drawing at most studio.graph_max_elements nodes plus
// relationships. Selecting a node or relationship shows its labels and
// properties as text; "Expand" fetches the node's neighbours with a bounded
// query as the same user. The grid stays the accessible alternative.

import { useEffect, useMemo, useRef, useState } from 'react';
import cytoscape, { type Core, type ElementDefinition, type EventObject } from 'cytoscape';
import { caption, type GEdge, type GNode, type GraphData } from './elements';

const PALETTE_LIGHT = ['#1f6f43', '#0b57d0', '#8a3ffc', '#b3261e', '#9a6700', '#007d79', '#a2191f', '#5a3e00'];
const PALETTE_DARK = ['#5cc48a', '#8ab4f8', '#be95ff', '#ff8a80', '#f1c21b', '#3ddbd9', '#ff8389', '#d2a106'];

function token(name: string, fallback: string): string {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}

/** A stable colour per label (the first label decides). */
export function labelColour(label: string, dark: boolean): string {
  let h = 0;
  for (let i = 0; i < label.length; i++) h = (h * 31 + label.charCodeAt(i)) >>> 0;
  const p = dark ? PALETTE_DARK : PALETTE_LIGHT;
  return p[h % p.length]!;
}

function elements(g: GraphData, dark: boolean): ElementDefinition[] {
  const out: ElementDefinition[] = [];
  for (const n of g.nodes.values()) {
    out.push({
      group: 'nodes',
      data: { id: `n${n.id}`, label: caption(n), colour: labelColour(n.labels[0] ?? '', dark) },
    });
  }
  // Ends missing from the result get a small stub node.
  for (const e of g.edges.values()) {
    for (const end of [e.start, e.end]) {
      if (!g.nodes.has(end) && !out.some((x) => x.data.id === `n${end}`)) {
        out.push({ group: 'nodes', data: { id: `n${end}`, label: `#${end}`, colour: '#888888', stub: 1 } });
      }
    }
    out.push({ group: 'edges', data: { id: `e${e.id}`, source: `n${e.start}`, target: `n${e.end}`, label: e.type } });
  }
  return out;
}

export interface Selected {
  kind: 'node' | 'rel';
  node?: GNode;
  rel?: GEdge;
}

export default function GraphView({
  graph,
  onExpand,
  label,
}: Readonly<{ graph: GraphData; onExpand?: (id: string) => Promise<void>; label: string }>) {
  const host = useRef<HTMLDivElement>(null);
  const cy = useRef<Core | null>(null);
  const [sel, setSel] = useState<Selected | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [dark, setDark] = useState(() => document.documentElement.dataset.theme === 'dark');
  const els = useMemo(() => elements(graph, dark), [graph, dark]);

  useEffect(() => {
    const retheme = () => {
      const bg = token('--bg', '#ffffff');
      // A dark page has a dark background token.
      setDark(parseInt(bg.replace('#', '').slice(0, 2) || 'ff', 16) < 0x80);
    };
    retheme();
    window.addEventListener('sprig-theme', retheme);
    const mq = window.matchMedia?.('(prefers-color-scheme: dark)');
    mq?.addEventListener('change', retheme);
    return () => {
      window.removeEventListener('sprig-theme', retheme);
      mq?.removeEventListener('change', retheme);
    };
  }, []);

  useEffect(() => {
    if (!host.current) return;
    const fg = token('--fg', '#1b1f24');
    const muted = token('--fg-muted', '#4d5560');
    const bg = token('--bg', '#ffffff');
    const focus = token('--focus', '#0b57d0');
    const c = cytoscape({
      container: host.current,
      elements: els,
      wheelSensitivity: 0.3,
      style: [
        {
          selector: 'node',
          style: {
            'background-color': 'data(colour)',
            label: 'data(label)',
            color: fg,
            'font-size': 11,
            'text-valign': 'bottom',
            'text-margin-y': 4,
            'text-outline-color': bg,
            'text-outline-width': 2,
            width: 26,
            height: 26,
          },
        },
        { selector: 'node[stub]', style: { width: 12, height: 12, 'font-size': 9 } },
        {
          selector: 'edge',
          style: {
            width: 1.5,
            'line-color': muted,
            'target-arrow-color': muted,
            'target-arrow-shape': 'triangle',
            'curve-style': 'bezier',
            label: 'data(label)',
            'font-size': 9,
            color: fg,
            'text-rotation': 'autorotate',
            'text-outline-color': bg,
            'text-outline-width': 2,
          },
        },
        { selector: ':selected', style: { 'border-width': 3, 'border-color': focus, 'line-color': focus, 'target-arrow-color': focus } },
      ],
      layout: { name: els.length > 600 ? 'grid' : 'cose', animate: false },
    });
    const idOf = (ev: EventObject) => String((ev.target as cytoscape.SingularElementArgument).id()).slice(1);
    c.on('tap', 'node', (ev) => {
      const id = idOf(ev);
      const n = graph.nodes.get(id);
      setSel(n ? { kind: 'node', node: n } : { kind: 'node', node: { id, labels: [], props: {} } });
    });
    c.on('tap', 'edge', (ev) => {
      const rel = graph.edges.get(idOf(ev));
      if (rel) setSel({ kind: 'rel', rel });
    });
    c.on('tap', (ev) => {
      if (ev.target === c) setSel(null);
    });
    cy.current = c;
    return () => {
      c.destroy();
      cy.current = null;
    };
  }, [els, graph]);

  const expand = async () => {
    if (!onExpand || !sel?.node) return;
    setBusy(true);
    setErr(null);
    try {
      await onExpand(sel.node.id);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="graph-wrap">
      <div
        ref={host}
        className="graph-host"
        role="img"
        aria-label={`${label}: ${graph.nodes.size} nodes, ${graph.edges.size} relationships. The grid view lists the same result as text.`}
        data-testid="graph-view"
        data-nodes={graph.nodes.size}
        data-edges={graph.edges.size}
      />
      <aside className="graph-side stack" aria-label="Selection" data-testid="graph-selection">
        <div className="row">
          <button onClick={() => cy.current?.fit(undefined, 20)}>Fit</button>
          <button
            onClick={() => cy.current?.layout({ name: 'cose', animate: false }).run()}
          >
            Layout
          </button>
        </div>
        <label className="stack">
          <span>Node</span>
          <select
            value={sel?.node?.id ?? ''}
            onChange={(e) => {
              const n = graph.nodes.get(e.target.value);
              setSel(n ? { kind: 'node', node: n } : null);
              const el = cy.current?.getElementById(`n${e.target.value}`);
              cy.current?.elements().unselect();
              el?.select();
            }}
            data-testid="graph-node-select"
          >
            <option value="">(choose, or click one)</option>
            {[...graph.nodes.values()].map((n) => (
              <option key={n.id} value={n.id}>
                {caption(n)} ({n.labels.join(':') || 'node'} {n.id})
              </option>
            ))}
          </select>
        </label>
        {!sel && <p className="muted">Select a node or relationship to see its properties.</p>}
        {sel?.node && (
          <>
            <h3 className="mono">
              {sel.node.labels.map((l) => `:${l}`).join('') || 'node'} {sel.node.id}
            </h3>
            <Props props={sel.node.props} />
            {onExpand && (
              <button onClick={() => void expand()} disabled={busy} data-testid="graph-expand">
                Expand neighbours
              </button>
            )}
          </>
        )}
        {sel?.rel && (
          <>
            <h3 className="mono">
              ({sel.rel.start})-[:{sel.rel.type} {sel.rel.id}]-&gt;({sel.rel.end})
            </h3>
            <Props props={sel.rel.props} />
          </>
        )}
        {err && (
          <p className="error" role="alert">
            {err}
          </p>
        )}
      </aside>
    </div>
  );
}

function fmt(v: unknown): string {
  return typeof v === 'string' ? v : JSON.stringify(v);
}

function Props({ props }: Readonly<{ props: Record<string, unknown> }>) {
  const keys = Object.keys(props);
  if (!keys.length) return <p className="muted">No properties.</p>;
  return (
    <dl className="kv">
      {keys.map((k) => (
        <div key={k} style={{ display: 'contents' }}>
          <dt>{k}</dt>
          <dd className="mono">{fmt(props[k])}</dd>
        </div>
      ))}
    </dl>
  );
}

import { useMemo, useState } from 'react';
import { useSession } from '../auth/session';
import { execute } from '../api/sql';
import type { ResultBuffer } from '../results/buffer';
import { expandQuery, graphOf, merge, type GraphData } from './elements';
import GraphView from './GraphView';

/** A Cypher or GQL result drawn as a graph, with "expand". */
export default function GraphResult({
  buffer,
  rows,
  db,
  label,
  language,
}: Readonly<{ buffer: ResultBuffer; rows: number; db: string | null; label: string; language: 'cypher' | 'gql' }>) {
  const max = useSession((s) => s.me?.studio.graph_max_elements ?? 2000);
  const base = useMemo(
    () => graphOf(rows, buffer.columns.length, (r, c) => buffer.cell(r, c), max),
    [buffer, rows, max],
  );
  const [extra, setExtra] = useState<GraphData | null>(null);
  const g = extra ?? base;
  const onExpand = async (id: string) => {
    // Expansion is always Cypher: GQL's MATCH is the same text here.
    const r = await execute(db, expandQuery(id, Math.min(100, max)), { language: language === 'gql' ? 'cypher' : language, maxRows: 100 });
    const res = r.results.find((x) => x.columns?.length);
    if (!res) return;
    const rows = res.rows ?? [];
    const more = graphOf(rows.length, res.columns?.length ?? 0, (ri, c) => rows[ri]?.[c] ?? null, max);
    setExtra(merge(g, more, max));
  };
  return (
    <div className="stack">
      {(g.dropped > 0 || g.dangling > 0) && (
        <p className="banner" role="status" data-testid="graph-capped">
          {g.dropped > 0 && `${g.dropped} elements not drawn: studio.graph_max_elements is ${max}. `}
          {g.dangling > 0 && `${g.dangling} relationships end at nodes the result doesn't include (drawn small).`}
        </p>
      )}
      <GraphView graph={g} label={label} onExpand={onExpand} />
    </div>
  );
}

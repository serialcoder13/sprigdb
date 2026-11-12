// The structure view's SprigDB tabs (the Studio and TUI redesign):
// relations, triggers, row-level policies, inheritance and shards of one table,
// read through the same public API and statements as the dedicated pages.

import { Link, useParams } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { objects, rows as fetchRows } from '../api/sql';
import { json } from '../api/client';
import { sqlValue } from '../lib/sql';
import { errorText } from '../api/errors';
import type { ShardGroup } from '../shards/shards';
import { keyText } from '../shards/shards';

export type StructureTab = 'relations' | 'triggers' | 'policies' | 'inheritance' | 'shards';

function Table({ head, body, testId, empty }: { head: string[]; body: (string | null | undefined)[][]; testId: string; empty: string }) {
  if (!body.length) return <p className="muted">{empty}</p>;
  return (
    <div className="table-scroll">
      <table className="table" data-testid={testId}>
        <thead>
          <tr>
            {head.map((h) => (
              <th key={h} scope="col">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {body.map((r, i) => (
            <tr key={i}>
              {r.map((c, j) => (
                <td key={j} className={j === 0 ? undefined : 'mono'}>
                  {c ?? ''}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Loading({ q }: { q: { isLoading: boolean; error: unknown } }) {
  if (q.error) return <p className="error">{errorText(q.error)}</p>;
  if (q.isLoading) return <p className="muted">Loading…</p>;
  return null;
}

/** Foreign keys this table holds, and the ones that point at it. */
function Relations({ db, table }: { db: string; table: string }) {
  const q = useQuery({
    queryKey: ['obj', db, table, 'relations'],
    queryFn: async () => {
      const cols = 'CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME';
      const from = 'information_schema.KEY_COLUMN_USAGE';
      const [out, inc] = await Promise.all([
        fetchRows(db, `SELECT ${cols} FROM ${from} WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ${sqlValue(table)} AND REFERENCED_TABLE_NAME IS NOT NULL ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION`),
        fetchRows(db, `SELECT ${cols} FROM ${from} WHERE REFERENCED_TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME = ${sqlValue(table)} ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION`),
      ]);
      return { out: out.rows, inc: inc.rows };
    },
  });
  return (
    <div className="stack">
      <Loading q={q} />
      {q.data && (
        <>
          <h3>References from {table}</h3>
          <Table
            testId="relations-out"
            head={['Constraint', 'Column', 'References']}
            body={q.data.out.map((r) => [r[0], r[2], `${r[3] ?? ''}(${r[4] ?? ''})`])}
            empty="This table references no other table."
          />
          <h3>References to {table}</h3>
          <Table
            testId="relations-in"
            head={['Constraint', 'From', 'Column']}
            body={q.data.inc.map((r) => [r[0], `${r[1] ?? ''}(${r[2] ?? ''})`, r[4]])}
            empty="No table references this one."
          />
        </>
      )}
    </div>
  );
}

/** SQL triggers on the table; logic-layer triggers are on the Logic page. */
function Triggers({ db, table }: { db: string; table: string }) {
  const { tenant = '' } = useParams();
  const q = useQuery({
    queryKey: ['obj', db, table, 'triggers'],
    queryFn: () =>
      fetchRows(
        db,
        `SELECT TRIGGER_NAME, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE = ${sqlValue(table)} ORDER BY ACTION_TIMING, EVENT_MANIPULATION, ACTION_ORDER`,
      ),
  });
  return (
    <div className="stack">
      <Loading q={q} />
      {q.data && (
        <Table
          testId="triggers"
          head={['Trigger', 'When', 'On', 'Statement']}
          body={q.data.rows.map((r) => [r[0], r[1], r[2], r[3]])}
          empty="No SQL triggers on this table."
        />
      )}
      <p className="muted">
        Triggers written in the logic layer (JavaScript, Lua, KIRun) are on the{' '}
        <Link to={`/t/${encodeURIComponent(tenant)}/logic`}>Logic</Link> page.
      </p>
    </div>
  );
}

/** Row-level security policies on the table (chapter 29). */
function Policies({ db, table }: { db: string; table: string }) {
  const { tenant = '' } = useParams();
  const q = useQuery({
    queryKey: ['obj', db, table, 'policies'],
    queryFn: () => fetchRows('-', `SHOW POLICIES FROM \`${db.replace(/`/g, '``')}\``),
  });
  const mine = (q.data?.rows ?? []).filter((r) => r[1] === table);
  return (
    <div className="stack">
      <Loading q={q} />
      {q.data && (
        <Table
          testId="policies"
          head={['Policy', 'Mode', 'Commands', 'Roles', 'Using', 'With check', 'Enabled', 'Forced']}
          body={mine.map((r) => r.slice(2))}
          empty="No row-level policies on this table: every user with SELECT sees every row."
        />
      )}
      <p className="muted">
        Create and test policies on <Link to={`/t/${encodeURIComponent(tenant)}/policies`}>Roles and policies</Link>.
      </p>
    </div>
  );
}

/** Whether and how the table is inherited (chapter 42). */
function Inheritance({ db, table }: { db: string; table: string }) {
  const { tenant = '' } = useParams();
  const q = useQuery({ queryKey: ['inherit', 'tables'], queryFn: () => objects('-', 'SHOW INHERITABLE TABLES') });
  const row = (q.data ?? []).find((t) => t.Table === `${db}.${table}` || t.Table === table);
  return (
    <div className="stack">
      <Loading q={q} />
      {q.data && !row && <p className="muted">This table isn't inheritable. Its rows belong to this tenant only.</p>}
      {row && (
        <dl className="kv" data-testid="inheritance">
          <dt>Key</dt>
          <dd className="mono">{row.Key}</dd>
          <dt>Origin</dt>
          <dd>{row.Origin ?? row.Inherited_from ?? 'declared here'}</dd>
          <dt>Columns added here</dt>
          <dd className="mono">{row.Own_columns || '—'}</dd>
          <dt>Group</dt>
          <dd>{row.Group}</dd>
          <dt>Shares</dt>
          <dd>{row.Shares || '—'}</dd>
        </dl>
      )}
      <p className="muted">
        Shares and received data are on the <Link to={`/t/${encodeURIComponent(tenant)}/inheritance`}>Inheritance</Link> page.
      </p>
    </div>
  );
}

/** Where the table's shards live (chapter 15). */
function Shards({ db, table }: { db: string; table: string }) {
  const { tenant = '' } = useParams();
  const q = useQuery({ queryKey: ['shards'], queryFn: () => json<{ groups: ShardGroup[] }>('/v1/shards') });
  const group = (q.data?.groups ?? []).find((g) => g.members.some((m) => m.db === db && m.table === table));
  const member = group?.members.find((m) => m.db === db && m.table === table);
  return (
    <div className="stack">
      <Loading q={q} />
      {q.data && !group && <p className="muted">Not sharded: the table lives in one shard with the rest of its database.</p>}
      {group && member && (
        <>
          <dl className="kv" data-testid="shards">
            <dt>Table group</dt>
            <dd>{group.name}</dd>
            <dt>Key</dt>
            <dd className="mono">{keyText(group)}</dd>
            <dt>Routing</dt>
            <dd>{member.routing}</dd>
            <dt>State</dt>
            <dd>{group.state}</dd>
            <dt>Shards</dt>
            <dd>{group.map?.shards.length ?? '—'}</dd>
          </dl>
          <p className="muted">
            Moves, splits and the advisor are on the <Link to={`/t/${encodeURIComponent(tenant)}/shards`}>Shards</Link> page.
          </p>
        </>
      )}
    </div>
  );
}

export function StructureView({ tab, db, table }: { tab: StructureTab; db: string; table: string }) {
  switch (tab) {
    case 'relations':
      return <Relations db={db} table={table} />;
    case 'triggers':
      return <Triggers db={db} table={table} />;
    case 'policies':
      return <Policies db={db} table={table} />;
    case 'inheritance':
      return <Inheritance db={db} table={table} />;
    case 'shards':
      return <Shards db={db} table={table} />;
  }
}

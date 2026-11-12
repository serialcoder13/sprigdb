// Connection forms and the script-sink editor (chapter 44 "Sinks", chapter
// 51; Studio round 5, decisions ST48 and ST49). Everything is a statement
// shown before it runs, as a SQL client would write it:
//
// - connections: `SHOW CONNECTIONS FROM`, `SHOW CREATE CONNECTION`,
//   `CREATE CONNECTION ... WITH (...) CREDENTIALS (...)`,
//   `ALTER CONNECTION ... SET CREDENTIALS (...)`, `DROP CONNECTION`;
// - script sinks: `CREATE SINK ... TO SCRIPT LANGUAGE l AS $$...$$` and
//   `ALTER SINK ... SET SCRIPT AS $$...$$`, edited in Monaco (JavaScript,
//   Lua) or `@fincity/kirun-ui` (KIRun);
// - sinks to a connection (webhook, Kafka, S3, database; round 7, ST70):
//   `CREATE SINK ... TO <target> CONNECTION c ...`;
// - dry runs before saving (round 7): `TEST CREATE CONNECTION ...` (ST68)
//   and `TEST CREATE SINK ... WITH SAMPLE n` (ST69) create nothing.
//
// Credentials are write-only: the fields are password inputs, the statement
// on screen shows '********' for each value, the values live only in this
// form's state until the statement ran, and nothing keeps them (no draft,
// no history). The gateway redacts them in its logs and `SHOW CREATE`.
import { lazy, Suspense, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { errorText } from '../api/errors';
import { Generic, col, table, useStatement } from './MessagingPage';
import { ConnectionTest, SinkDryRun, StepTable, steps } from './DryRuns';
import {
  CONN_CREDENTIALS,
  CONN_KINDS,
  CONN_REQUIRED,
  SCRIPT_TEMPLATES,
  TARGET_KINDS,
  createConnection,
  createScriptSink,
  createSink,
  dropConnection,
  scriptOf,
  setCredentials,
  setScript,
  testNewConnection,
  type ConnKind,
  type Pair,
  type ScriptLanguage,
  type SinkSource,
  type SinkSpec,
  type StepRow,
} from './statements';

const Monaco = lazy(() => import('../editor/Monaco'));
const KirunEditor = lazy(() => import('../logic/KirunEditor'));

const NO_COMPLETION = { keywords: [], objects: () => [], columns: () => [] };
/** Disclosure toggles big enough to hit (WCAG 2.2 target size, 24 px). */
const TARGET = { paddingBlock: 6, cursor: 'pointer' } as const;

/** `key = value` lines as pairs (blank lines and `#` comments skipped). */
export function parsePairs(text: string): Pair[] {
  return text
    .split('\n')
    .map((l) => l.trim())
    .filter((l) => l && !l.startsWith('#'))
    .map((l) => {
      const i = l.indexOf('=');
      if (i < 0) throw new Error(`"${l}": write key = value`);
      return { key: l.slice(0, i).trim(), value: l.slice(i + 1).trim().replace(/^'(.*)'$/, '$1') };
    });
}

/** Write-only credential fields: a name from the type's list and a value. */
function CredentialFields({ kind, value, onChange, idPrefix }: Readonly<{ kind: ConnKind; value: Pair[]; onChange: (v: Pair[]) => void; idPrefix: string }>) {
  const names = CONN_CREDENTIALS[kind];
  return (
    <fieldset className="stack" data-testid={`${idPrefix}-creds`}>
      <legend>Credentials (write-only)</legend>
      {value.map((p, i) => (
        <div className="row" key={i}>
          <label className="row">
            Name
            <select value={p.key} onChange={(e) => onChange(value.map((x, j) => (j === i ? { ...x, key: e.target.value } : x)))}>
              {names.map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </label>
          <label className="row">
            Value
            <input
              type="password"
              autoComplete="new-password"
              spellCheck={false}
              value={p.value}
              onChange={(e) => onChange(value.map((x, j) => (j === i ? { ...x, value: e.target.value } : x)))}
              data-testid={`${idPrefix}-cred-${i}`}
            />
          </label>
          <button type="button" onClick={() => onChange(value.filter((_, j) => j !== i))} aria-label={`Remove credential ${i + 1}`}>
            ✕
          </button>
        </div>
      ))}
      <div>
        <button type="button" onClick={() => onChange([...value, { key: names[0] ?? '', value: '' }])}>
          Add a credential
        </button>
      </div>
    </fieldset>
  );
}

function NewConnection({ db, onDone }: Readonly<{ db: string; onDone: () => void }>) {
  const st = useStatement(db);
  const [name, setName] = useState('');
  const [kind, setKind] = useState<ConnKind>('webhook');
  const [req, setReq] = useState<Record<string, string>>({});
  const [more, setMore] = useState('');
  const [creds, setCreds] = useState<Pair[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [tested, setTested] = useState<StepRow[] | null>(null);
  const [testing, setTesting] = useState(false);
  const options = () => [...CONN_REQUIRED[kind].map((k) => ({ key: k, value: req[k] ?? '' })), ...parsePairs(more)];
  const submit = async () => {
    setErr(null);
    try {
      const opts = options();
      const sql = createConnection(name, kind, opts, creds);
      if (await st.run(sql, createConnection(name, kind, opts, creds, true))) {
        setCreds([]);
        setTested(null);
        onDone();
      }
    } catch (e) {
      setErr(errorText(e));
    }
  };
  // Dry run before saving (ST68): TEST CREATE CONNECTION checks the
  // definition as given and creates nothing.
  const testFirst = async () => {
    setErr(null);
    setTested(null);
    try {
      const opts = options();
      const n = name.trim() || kind;
      const shown = testNewConnection(n, kind, opts, creds, true);
      if (!(await st.ask(shown, 'The test opens the destination with this definition, as a sink would, and creates nothing: no connection is saved and nothing is written.'))) return;
      setTesting(true);
      setTested(await steps(db, testNewConnection(n, kind, opts, creds)));
    } catch (e) {
      setErr(errorText(e));
    } finally {
      setTesting(false);
    }
  };
  return (
    <form
      className="stack"
      aria-label="New connection"
      data-testid="conn-new"
      onSubmit={(e) => {
        e.preventDefault();
        void submit();
      }}
    >
      <div className="row">
        <label className="row">
          Name
          <input value={name} required onChange={(e) => setName(e.target.value)} data-testid="conn-name" />
        </label>
        <label className="row">
          Type
          <select
            value={kind}
            onChange={(e) => {
              setKind(e.target.value as ConnKind);
              setCreds([]);
            }}
            data-testid="conn-kind"
          >
            {CONN_KINDS.map((k) => (
              <option key={k} value={k}>
                {k}
              </option>
            ))}
          </select>
        </label>
      </div>
      <div className="row">
        {CONN_REQUIRED[kind].map((k) => (
          <label className="row" key={k}>
            {k}
            <input value={req[k] ?? ''} onChange={(e) => setReq({ ...req, [k]: e.target.value })} data-testid={`conn-opt-${k}`} />
          </label>
        ))}
      </div>
      <label className="stack">
        <span>More options, one `key = value` a line</span>
        <textarea rows={3} value={more} onChange={(e) => setMore(e.target.value)} spellCheck={false} data-testid="conn-more" />
      </label>
      <CredentialFields kind={kind} value={creds} onChange={setCreds} idPrefix="conn-new" />
      <div className="row">
        <button type="button" onClick={() => void testFirst()} disabled={testing} data-testid="conn-test-first">
          Test before creating…
        </button>
        <button className="primary" type="submit" data-testid="conn-create">
          Create connection…
        </button>
        {testing && <span className="muted">Testing…</span>}
      </div>
      {tested && (
        <div className="stack" data-testid="conn-test-first-result">
          <StepTable steps={tested} />
        </div>
      )}
      {err && (
        <p className="error" role="alert">
          {err}
        </p>
      )}
      {st.node}
    </form>
  );
}

function ConnectionDetail({ db, name, kind, onChanged }: Readonly<{ db: string; name: string; kind: ConnKind | null; onChanged: () => void }>) {
  const st = useStatement(db);
  const qn = `\`${name.replace(/`/g, '``')}\``;
  const def = useQuery({ queryKey: ['conn-def', db, name], queryFn: () => table(db, `SHOW CREATE CONNECTION ${qn}`) });
  const [creds, setCreds] = useState<Pair[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [retest, setRetest] = useState(0);
  const k: ConnKind = kind ?? 'webhook';
  const save = async () => {
    setErr(null);
    try {
      if (await st.run(setCredentials(name, creds), setCredentials(name, creds, true))) {
        setCreds([]);
        await def.refetch();
        onChanged();
        setRetest((n) => n + 1);
      }
    } catch (e) {
      setErr(errorText(e));
    }
  };
  return (
    <section className="card stack" aria-labelledby="conn-h" data-testid="conn-detail">
      <h3 id="conn-h" style={{ margin: 0 }}>
        Connection {name}
      </h3>
      {def.error && <p className="error">{errorText(def.error)}</p>}
      {def.data && (
        <pre className="logic-stmt" data-testid="conn-def">
          {def.data.rows.map((r) => r[r.length - 1]).join('\n')}
        </pre>
      )}
      <ConnectionTest db={db} name={name} autoRun={retest} />
      <form
        className="stack"
        aria-label={`Set credentials of ${name}`}
        onSubmit={(e) => {
          e.preventDefault();
          void save();
        }}
      >
        <CredentialFields kind={k} value={creds} onChange={setCreds} idPrefix="conn-set" />
        <div className="row">
          <button type="submit" disabled={!creds.some((c) => c.value)} data-testid="conn-set-credentials">
            Set credentials…
          </button>
          <button
            type="button"
            onClick={() =>
              void st.run(dropConnection(name)).then((ok) => {
                if (ok) onChanged();
              })
            }
          >
            Drop…
          </button>
        </div>
      </form>
      {err && (
        <p className="error" role="alert">
          {err}
        </p>
      )}
      {st.node}
    </section>
  );
}

function Connections({ db }: Readonly<{ db: string }>) {
  const list = useQuery({ queryKey: ['connections', db], queryFn: () => table(db, `SHOW CONNECTIONS FROM \`${db.replace(/`/g, '``')}\``) });
  const [sel, setSel] = useState<{ name: string; kind: ConnKind | null } | null>(null);
  return (
    <section className="stack" aria-labelledby="conns-h">
      <h2 id="conns-h" style={{ margin: 0 }}>
        Connections
      </h2>
      {list.error && <p className="error">{errorText(list.error)}</p>}
      {list.data && (
        <Generic
          t={list.data}
          testid="msg-connections"
          onPick={(r) => {
            const name = col(list.data, r, 'Name', 'Connection', 'name') ?? r[0] ?? null;
            const kind = (col(list.data, r, 'Type', 'Kind', 'type') ?? '').toLowerCase();
            if (name) setSel({ name, kind: (CONN_KINDS as readonly string[]).includes(kind) ? (kind as ConnKind) : null });
          }}
        />
      )}
      {sel && (
        <ConnectionDetail
          key={sel.name}
          db={db}
          name={sel.name}
          kind={sel.kind}
          onChanged={() => {
            void list.refetch();
          }}
        />
      )}
      <details>
        <summary data-testid="conn-new-open" style={TARGET}>New connection</summary>
        <NewConnection db={db} onDone={() => void list.refetch()} />
      </details>
    </section>
  );
}

/** A script's code: Monaco for JavaScript and Lua; kirun-ui (and its JSON) for KIRun. */
function ScriptCode({ language, value, onChange, label }: Readonly<{ language: ScriptLanguage; value: string; onChange: (v: string) => void; label: string }>) {
  return (
    <Suspense fallback={<div className="editor-host muted">Loading editor…</div>}>
      {language === 'kirun' ? (
        <>
          <KirunEditor value={value} onChange={onChange} />
          <details>
            <summary style={TARGET}>Definition as JSON</summary>
            <Monaco value={value} onChange={onChange} onRun={() => undefined} completion={NO_COMPLETION} language="javascript" label={`${label} (KIRun JSON)`} handle={() => undefined} />
          </details>
        </>
      ) : (
        <Monaco value={value} onChange={onChange} onRun={() => undefined} completion={NO_COMPLETION} language={language} label={label} handle={() => undefined} />
      )}
    </Suspense>
  );
}

function NewScriptSink({ db, onDone }: Readonly<{ db: string; onDone: () => void }>) {
  const st = useStatement(db);
  const [name, setName] = useState('');
  const [from, setFrom] = useState<SinkSource>({ kind: 'stream', name: '' });
  const [language, setLanguage] = useState<ScriptLanguage>('javascript');
  const [using, setUsing] = useState('');
  const [body, setBody] = useState(SCRIPT_TEMPLATES.javascript);
  const [err, setErr] = useState<string | null>(null);
  const submit = async () => {
    setErr(null);
    try {
      if (await st.run(createScriptSink(name, from, language, using.split(','), body))) onDone();
    } catch (e) {
      setErr(errorText(e));
    }
  };
  return (
    <form
      className="stack"
      aria-label="New script sink"
      data-testid="script-sink-new"
      onSubmit={(e) => {
        e.preventDefault();
        void submit();
      }}
    >
      <div className="row">
        <label className="row">
          Name
          <input value={name} required onChange={(e) => setName(e.target.value)} data-testid="script-sink-name" />
        </label>
        <label className="row">
          From
          <select value={from.kind} onChange={(e) => setFrom({ ...from, kind: e.target.value as SinkSource['kind'] })}>
            <option value="stream">stream</option>
            <option value="queue">queue</option>
            <option value="changes">changes of tables</option>
          </select>
        </label>
        <label className="row">
          Source
          <input value={from.name} required placeholder={from.kind === 'changes' ? 't1, t2' : ''} onChange={(e) => setFrom({ ...from, name: e.target.value })} data-testid="script-sink-source" />
        </label>
        <label className="row">
          Language
          <select
            value={language}
            onChange={(e) => {
              const l = e.target.value as ScriptLanguage;
              if (body === SCRIPT_TEMPLATES[language]) setBody(SCRIPT_TEMPLATES[l]);
              setLanguage(l);
            }}
            data-testid="script-sink-language"
          >
            <option value="javascript">JavaScript</option>
            <option value="lua">Lua</option>
            <option value="kirun">KIRun</option>
          </select>
        </label>
        <label className="row">
          Connections
          <input value={using} placeholder="crm_api, ..." onChange={(e) => setUsing(e.target.value)} data-testid="script-sink-using" />
        </label>
      </div>
      <ScriptCode language={language} value={body} onChange={setBody} label="Script sink code" />
      <div>
        <button className="primary" type="submit" data-testid="script-sink-create">
          Create sink…
        </button>
      </div>
      <SinkDryRun db={db} name={name} create={() => createScriptSink(name || 'new_sink', from, language, using.split(','), body)} />
      <p className="muted">A script sink&apos;s dry run shows what its deliver function would receive, not what the function does.</p>
      {err && (
        <p className="error" role="alert">
          {err}
        </p>
      )}
      {st.node}
    </form>
  );
}

/** Edits an existing script sink's code (`ALTER SINK ... SET SCRIPT`). */
export function ScriptEditor({ db, name, create, onSaved }: Readonly<{ db: string; name: string; create: string; onSaved: () => void }>) {
  const script = scriptOf(create);
  const [body, setBody] = useState(script?.body ?? '');
  const st = useStatement(db);
  const [err, setErr] = useState<string | null>(null);
  if (!script) return null;
  return (
    <section className="stack" aria-label={`Script of ${name}`} data-testid="script-sink-edit">
      <h3 style={{ margin: 0 }}>Script ({script.language})</h3>
      <ScriptCode language={script.language} value={body} onChange={setBody} label={`Script of sink ${name}`} />
      <div className="row">
        <button
          onClick={() => {
            setErr(null);
            try {
              void st.run(setScript(name, body)).then((ok) => ok && onSaved());
            } catch (e) {
              setErr(errorText(e));
            }
          }}
          disabled={body === script.body}
          data-testid="script-sink-save"
        >
          Save script…
        </button>
        <button onClick={() => setBody(script.body)} disabled={body === script.body}>
          Revert
        </button>
      </div>
      {err && (
        <p className="error" role="alert">
          {err}
        </p>
      )}
      {st.node}
    </section>
  );
}

const EMPTY_SINK: SinkSpec = {
  name: '',
  from: { kind: 'stream', name: '' },
  where: '',
  columns: '',
  transform: '',
  target: 'webhook',
  connection: '',
  where_to: '',
  key: '',
  guarantee: 'at least once',
  start: 'latest',
  options: [],
};

const WHERE_TO: Record<SinkSpec['target'], string> = { webhook: 'Path', kafka: 'Topic', s3: 'Key prefix', database: 'Table' };

/** A sink to a connection (ST70): the `CREATE SINK` form, with a dry run before creating it. */
function NewSink({ db, onDone }: Readonly<{ db: string; onDone: () => void }>) {
  const st = useStatement(db);
  const conns = useQuery({ queryKey: ['connections', db], queryFn: () => table(db, `SHOW CONNECTIONS FROM \`${db.replace(/`/g, '``')}\``) });
  const [spec, setSpec] = useState<SinkSpec>(EMPTY_SINK);
  const [opts, setOpts] = useState('');
  const [err, setErr] = useState<string | null>(null);
  const set = (p: Partial<SinkSpec>) => setSpec((s) => ({ ...s, ...p }));
  const sql = () => createSink({ ...spec, options: parsePairs(opts) });
  const names = conns.data ? conns.data.rows.map((r) => col(conns.data, r, 'Name', 'name') ?? '').filter(Boolean) : [];
  const submit = async () => {
    setErr(null);
    try {
      if (await st.run(sql())) {
        setSpec(EMPTY_SINK);
        setOpts('');
        onDone();
      }
    } catch (e) {
      setErr(errorText(e));
    }
  };
  return (
    <form
      className="stack"
      aria-label="New sink"
      data-testid="sink-new"
      onSubmit={(e) => {
        e.preventDefault();
        void submit();
      }}
    >
      <div className="row">
        <label className="row">
          Name
          <input value={spec.name} required onChange={(e) => set({ name: e.target.value })} data-testid="sink-new-name" />
        </label>
        <label className="row">
          From
          <select value={spec.from.kind} onChange={(e) => set({ from: { ...spec.from, kind: e.target.value as SinkSource['kind'] } })} data-testid="sink-new-from">
            <option value="stream">stream</option>
            <option value="queue">queue</option>
            <option value="changes">changes of tables</option>
          </select>
        </label>
        <label className="row">
          Source
          <input
            value={spec.from.name}
            required
            placeholder={spec.from.kind === 'changes' ? 't1, t2' : ''}
            onChange={(e) => set({ from: { ...spec.from, name: e.target.value } })}
            data-testid="sink-new-source"
          />
        </label>
      </div>
      <div className="row">
        <label className="row">
          Where
          <input value={spec.where} placeholder="amount > 100" size={28} onChange={(e) => set({ where: e.target.value })} data-testid="sink-new-where" />
        </label>
        <label className="row">
          Columns
          <input value={spec.columns} placeholder="all" onChange={(e) => set({ columns: e.target.value })} data-testid="sink-new-columns" />
        </label>
        <label className="row">
          Transform
          <input value={spec.transform} placeholder="a function" onChange={(e) => set({ transform: e.target.value })} data-testid="sink-new-transform" />
        </label>
      </div>
      <div className="row">
        <label className="row">
          To
          <select value={spec.target} onChange={(e) => set({ target: e.target.value as SinkSpec['target'] })} data-testid="sink-new-target">
            {TARGET_KINDS.map((k) => (
              <option key={k} value={k}>
                {k}
              </option>
            ))}
          </select>
        </label>
        <label className="row">
          Connection
          <input value={spec.connection} required list="sink-new-conns" onChange={(e) => set({ connection: e.target.value })} data-testid="sink-new-connection" />
          <datalist id="sink-new-conns">
            {names.map((n) => (
              <option key={n} value={n} />
            ))}
          </datalist>
        </label>
        <label className="row">
          {WHERE_TO[spec.target]}
          <input value={spec.where_to} required={spec.target !== 'webhook'} onChange={(e) => set({ where_to: e.target.value })} data-testid="sink-new-to" />
        </label>
        {spec.target === 'kafka' && (
          <label className="row">
            Key
            <input value={spec.key} placeholder="an expression" onChange={(e) => set({ key: e.target.value })} data-testid="sink-new-key" />
          </label>
        )}
      </div>
      <div className="row">
        <label className="row">
          Guarantee
          <select value={spec.guarantee} onChange={(e) => set({ guarantee: e.target.value as SinkSpec['guarantee'] })}>
            <option value="at least once">at least once</option>
            <option value="exactly once">exactly once</option>
          </select>
        </label>
        <label className="row">
          Start at
          <select value={spec.start} onChange={(e) => set({ start: e.target.value as SinkSpec['start'] })}>
            <option value="latest">latest</option>
            <option value="earliest">earliest</option>
            <option value="snapshot">snapshot</option>
          </select>
        </label>
      </div>
      <label className="stack">
        <span>Options, one `key = value` a line</span>
        <textarea rows={2} value={opts} onChange={(e) => setOpts(e.target.value)} spellCheck={false} data-testid="sink-new-options" />
      </label>
      <div>
        <button className="primary" type="submit" data-testid="sink-new-create">
          Create sink…
        </button>
      </div>
      {err && (
        <p className="error" role="alert">
          {err}
        </p>
      )}
      <SinkDryRun db={db} name={spec.name} create={sql} />
      {st.node}
    </form>
  );
}

/** The sink page's forms: connections, a new sink and a new script sink. */
export default function SinkForms({ db, onCreated }: Readonly<{ db: string; onCreated: () => void }>) {
  return (
    <div className="stack">
      <Connections db={db} />
      <details>
        <summary data-testid="sink-new-open" style={TARGET}>New sink</summary>
        <NewSink db={db} onDone={onCreated} />
      </details>
      <details>
        <summary data-testid="script-sink-open" style={TARGET}>New script sink</summary>
        <NewScriptSink db={db} onDone={onCreated} />
      </details>
    </div>
  );
}

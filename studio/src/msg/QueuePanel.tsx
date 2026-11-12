// A queue's counts and its page (chapter 44 step 29; Studio round 6): ready,
// unacknowledged and consumers from `sprig.mq_info` (every queue of the page
// in one call, ST66), browsing without taking
// (`sprig.mq_browse`), "get and requeue", purge and republish. Every call is
// a statement run as the signed-in user, so the queue engine checks their
// queue permissions (R9 item 89); purge and republish are shown first, with
// what they do, before they run.

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { errorText } from '../api/errors';
import { begin, execute, rollback } from '../api/sql';
import { Generic, table, useStatement, type Table } from './MessagingPage';
import { getMessages, mqBrowse, mqInfo, mqInfoAll, mqPurge, mqRepublish, queueInfo, queueInfos, settle, type QueueInfo } from './statements';

const visible = () => (document.visibilityState === 'visible' ? 5000 : false);

/** `mq_info` for one queue, refreshed every 5 s while the page shows. */
export function useQueueInfo(db: string, queue: string, vhost: string | null) {
  return useQuery<QueueInfo | null>({
    queryKey: ['mq-info', db, vhost, queue],
    queryFn: async () => {
      const t = await table(db, mqInfo(queue, vhost));
      return queueInfo(t.columns, t.rows[0]);
    },
    refetchInterval: visible,
    retry: false,
  });
}

/**
 * `mq_info` for every queue of the vhost in one call (ST66), refreshed every
 * 5 s while the page shows: a map by queue name, which leaves out queues the
 * user may not read. `null` from a gateway without the batch form; the rows
 * then ask one queue each.
 */
export function useAllQueueInfo(db: string, vhost: string | null) {
  return useQuery<Map<string, QueueInfo> | null>({
    queryKey: ['mq-info', db, vhost, '*'],
    enabled: vhost !== null,
    queryFn: async () => {
      try {
        const t = await table(db, mqInfoAll(vhost));
        return t.columns.includes('queue') ? queueInfos(t.columns, t.rows) : null;
      } catch {
        return null;
      }
    },
    refetchInterval: visible,
    retry: false,
  });
}

const NOT_READABLE = 'Not shown: you may not read this queue (39253)';

function Cells({ queue, info, why, loading }: Readonly<{ queue: string; info: QueueInfo | null | undefined; why?: string; loading: boolean }>) {
  const cell = (k: keyof QueueInfo) => (
    <td className="num" title={why} data-testid={`mq-${k}-${queue}`}>
      {info ? info[k] : loading ? '…' : '?'}
    </td>
  );
  return (
    <>
      {cell('ready')}
      {cell('unacked')}
      {cell('consumers')}
    </>
  );
}

/** Three cells: ready, unacknowledged, consumers, one `mq_info` call for this queue. */
export function QueueCounts({ db, queue, vhost }: Readonly<{ db: string; queue: string; vhost: string | null }>) {
  const q = useQueueInfo(db, queue, vhost);
  return <Cells queue={queue} info={q.data} why={q.error ? errorText(q.error) : undefined} loading={!q.error} />;
}

/** The same cells from the batch answer: a queue missing from it may not be read. */
export function QueueCountsFrom({ queue, all }: Readonly<{ queue: string; all: ReadonlyMap<string, QueueInfo> }>) {
  const info = all.get(queue);
  return <Cells queue={queue} info={info} why={info ? undefined : NOT_READABLE} loading={false} />;
}

export function QueueDetail({
  db,
  queue,
  vhost,
  exchanges,
}: Readonly<{ db: string; queue: string; vhost: string | null; exchanges: readonly string[] }>) {
  const qc = useQueryClient();
  const info = useQueueInfo(db, queue, vhost);
  const st = useStatement(db);
  const [max, setMax] = useState('50');
  const [shown, setShown] = useState<{ how: 'browse' | 'peek'; rows: Table | null; err: string | null } | null>(null);
  const [rep, setRep] = useState({ exchange: exchanges.find((e) => e !== '') ?? '', key: '', max: '100' });
  const [err, setErr] = useState<string | null>(null);
  const refresh = () => qc.invalidateQueries({ queryKey: ['mq-info', db, vhost] });

  const browse = async () => {
    setShown({ how: 'browse', rows: null, err: null });
    try {
      setShown({ how: 'browse', rows: await table(db, mqBrowse(queue, Number(max), vhost)), err: null });
    } catch (e) {
      setShown({ how: 'browse', rows: null, err: errorText(e) });
    }
  };
  // Get, then give every message back with a nack (requeue): they count as
  // redelivered, as with RabbitMQ's management UI (decision ST45).
  const peek = async () => {
    setShown({ how: 'peek', rows: null, err: null });
    const tx = await begin(db).catch(() => null);
    const opt = tx ? { transaction: tx.name } : {};
    try {
      const r = await execute(db, getMessages(queue, 10, vhost), opt);
      const first = r.results.find((x) => x.columns?.length);
      const t: Table = { columns: (first?.columns ?? []).map((c) => c.name), rows: first?.rows ?? [] };
      const ti = t.columns.indexOf('delivery_token');
      for (const row of t.rows) {
        const tok = ti >= 0 ? row[ti] : null;
        if (tok) await execute(db, settle(tok, true), opt);
      }
      if (tx) await rollback(tx).catch(() => undefined);
      setShown({ how: 'peek', rows: { columns: t.columns.filter((c) => c !== 'delivery_token'), rows: t.rows.map((r) => r.filter((_, i) => i !== ti)) }, err: null });
    } catch (e) {
      if (tx) await rollback(tx).catch(() => undefined);
      setShown({ how: 'peek', rows: null, err: errorText(e) });
    }
    await refresh();
  };
  const purge = async () => {
    const ready = info.data?.ready;
    const t = await st.runTable(mqPurge(queue, vhost), {
      note: `This removes ${ready ?? 'all'} ready message${ready === 1 ? '' : 's'} of ${queue} for good. Messages delivered and not yet acknowledged stay.`,
      done: (r) => `Purged ${r.rows[0]?.[0] ?? 0} messages.`,
    });
    if (t) {
      setShown(null);
      await refresh();
    }
  };
  const republish = async () => {
    setErr(null);
    let sql: string;
    try {
      sql = mqRepublish(queue, rep.exchange, rep.key, Number(rep.max), vhost);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
      return;
    }
    const t = await st.runTable(sql, {
      note: `This moves up to ${rep.max} messages from ${queue} to exchange ${rep.exchange || '(default)'}${rep.key.trim() ? ` with routing key ${rep.key.trim()}` : ', each with its own routing key'}. At least once: a crash midway can leave a message in both places.`,
      done: (r) => `Moved ${r.rows[0]?.[0] ?? 0} messages.`,
    });
    if (t) {
      setShown(null);
      await refresh();
    }
  };

  return (
    <section className="card stack" aria-labelledby="queue-h" data-testid="mq-queue-detail">
      <h3 id="queue-h" style={{ margin: 0 }}>
        Queue <span className="mono">{queue}</span>
      </h3>
      {info.error && (
        <p className="error" role="alert">
          {errorText(info.error)}
        </p>
      )}
      {info.data && (
        <dl className="kv" data-testid="mq-info">
          <dt>Ready</dt>
          <dd>{info.data.ready}</dd>
          <dt>Unacknowledged</dt>
          <dd>{info.data.unacked}</dd>
          <dt>Consumers</dt>
          <dd>{info.data.consumers}</dd>
        </dl>
      )}
      <div className="row">
        <label className="row">
          Messages <input value={max} onChange={(e) => setMax(e.target.value)} size={5} inputMode="numeric" data-testid="mq-browse-max" />
        </label>
        <button onClick={() => void browse()} data-testid="mq-browse">
          Browse
        </button>
        <button onClick={() => void peek()} data-testid={`msg-peek-${queue}`} title="Takes 10 messages and gives them back: they count as redelivered">
          Get 10 and requeue
        </button>
        <button className="danger" onClick={() => void purge()} data-testid="mq-purge">
          Purge…
        </button>
      </div>
      <p className="muted">Browse shows stored messages without taking or locking them; consumers are unaffected.</p>
      {shown && (
        <div className="stack" aria-label={shown.how === 'browse' ? `Messages of ${queue}, browsed` : `Messages of ${queue}, requeued`}>
          {shown.err && (
            <p className="error" role="alert">
              {shown.err}
            </p>
          )}
          {shown.rows && <Generic t={shown.rows} testid={shown.how === 'browse' ? 'mq-browsed' : 'msg-peeked'} />}
        </div>
      )}
      <details>
        <summary style={{ paddingBlock: 6, cursor: 'pointer' }} data-testid="mq-republish-open">
          Republish (move messages to an exchange)
        </summary>
        <div className="row">
          <label className="row">
            Exchange
            <select value={rep.exchange} onChange={(e) => setRep({ ...rep, exchange: e.target.value })} data-testid="mq-republish-exchange">
              {exchanges.map((e) => (
                <option key={e} value={e}>
                  {e || '(default)'}
                </option>
              ))}
            </select>
          </label>
          <label className="row">
            Routing key <input value={rep.key} placeholder="keep each message's" onChange={(e) => setRep({ ...rep, key: e.target.value })} data-testid="mq-republish-key" />
          </label>
          <label className="row">
            At most <input value={rep.max} onChange={(e) => setRep({ ...rep, max: e.target.value })} size={6} inputMode="numeric" data-testid="mq-republish-max" />
          </label>
          <button onClick={() => void republish()} data-testid="mq-republish">
            Republish…
          </button>
        </div>
        {err && (
          <p className="error" role="alert">
            {err}
          </p>
        )}
      </details>
      {st.node}
    </section>
  );
}

import { useEffect, useMemo, useRef, useState, type ComponentType } from 'react';
import { useNavigate, useParams } from 'react-router';
import { useIntl } from 'react-intl';
import { useQueryClient } from '@tanstack/react-query';
import { logout, useSession } from '../auth/session';
import { addTab } from '../editor/tabs';
import { currentTheme, setTheme, type Theme } from '../app/theme';
import { useDatabases, type DbSnapshot } from '../catalog/snapshot';
import { operatorSession } from '../ops/ops';
import { Database, Eye, LogOut, Moon, Search, Share2, SquareFunction, SquareTerminal, Table2 } from '../icons';
import { SECTIONS, resolve } from './sections';
import { useWorkspace } from './store';
import { DOC_ICON } from './WorkspaceTabs';
import type { MessageId } from '../i18n/en';

interface Item {
  id: string;
  label: string;
  hint: string;
  icon: ComponentType<{ size?: number }>;
  run: () => void;
}

/** Subsequence match with a score: earlier and contiguous matches rank higher. */
export function score(query: string, text: string): number | null {
  const q = query.toLowerCase();
  const t = text.toLowerCase();
  if (!q) return 0;
  const at = t.indexOf(q);
  if (at >= 0) return 1000 - at * 2 - (t.length - q.length) / 10;
  let i = 0;
  let gaps = 0;
  let last = -1;
  for (let j = 0; j < t.length && i < q.length; j++) {
    if (t[j] === q[i]) {
      if (last >= 0 && j !== last + 1) gaps++;
      last = j;
      i++;
    }
  }
  return i === q.length ? 500 - gaps * 10 - t.length / 10 : null;
}

const THEMES: Theme[] = ['system', 'light', 'dark', 'contrast'];

/** The command palette: Ctrl+K or Ctrl+P, one search over pages,
 * open tabs, databases, the objects of databases already loaded, and commands. */
export function CommandPalette({ open, onClose }: { open: boolean; onClose: () => void }) {
  // Mounted only while open, so each opening starts empty.
  return open ? <PaletteBody onClose={onClose} /> : null;
}

function PaletteBody({ onClose }: { onClose: () => void }) {
  const intl = useIntl();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const { tenant = '' } = useParams();
  const base = `/t/${encodeURIComponent(tenant)}`;
  const me = useSession((s) => s.me);
  const dbs = useDatabases();
  const docs = useWorkspace((s) => s.docs);
  const [q, setQ] = useState('');
  const [sel, setSel] = useState(0);
  const input = useRef<HTMLInputElement>(null);
  const t = (id: MessageId) => intl.formatMessage({ id });

  useEffect(() => {
    input.current?.focus();
  }, []);

  const items = useMemo<Item[]>(() => {
    const go = (to: string) => () => void navigate(to);
    const out: Item[] = [];
    const isOperator = !!(me?.platform_roles.includes('operator') || operatorSession());
    for (const s of SECTIONS) {
      if ((s.operatorOnly && !isOperator) || s.panel) continue;
      out.push({ id: `s:${s.id}`, label: t(s.label), hint: t('palette.kind.page'), icon: s.icon, run: go(resolve(base, s.to)) });
      for (const l of s.links) {
        if (l.to === s.to && l.label === s.label) continue;
        out.push({ id: `l:${l.to}`, label: t(l.label), hint: t(s.label), icon: s.icon, run: go(resolve(base, l.to)) });
      }
    }
    for (const d of docs) {
      out.push({ id: `d:${d.rel}`, label: d.title, hint: t('palette.kind.tab'), icon: DOC_ICON[d.kind], run: go(d.rel ? `${base}/${d.rel}` : base) });
    }
    for (const db of dbs.data ?? []) {
      const dbPath = `${base}/db/${encodeURIComponent(db)}`;
      out.push({ id: `db:${db}`, label: db, hint: t('palette.kind.database'), icon: Database, run: go(dbPath) });
      const snap = qc.getQueryData<DbSnapshot>(['catalog', 'db', db]);
      for (const o of snap?.objects ?? []) {
        const icon = o.kind === 'view' ? Eye : o.kind === 'graph' ? Share2 : o.kind === 'table' ? Table2 : SquareFunction;
        out.push({
          id: `o:${db}:${o.kind}:${o.name}`,
          label: o.name,
          hint: `${o.kind} · ${db}`,
          icon,
          run: go(`${dbPath}/obj/${o.kind}/${encodeURIComponent(o.name)}`),
        });
      }
    }
    out.push({
      id: 'c:new',
      label: t('palette.cmd.newQuery'),
      hint: t('palette.kind.command'),
      icon: SquareTerminal,
      run: () => void navigate(`${base}/q/${addTab().id}`),
    });
    out.push({
      id: 'c:theme',
      label: t('palette.cmd.theme'),
      hint: currentTheme(),
      icon: Moon,
      run: () => setTheme(THEMES[(THEMES.indexOf(currentTheme()) + 1) % THEMES.length]!),
    });
    out.push({ id: 'c:out', label: t('palette.cmd.signOut'), hint: t('palette.kind.command'), icon: LogOut, run: () => void logout() });
    return out;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [base, dbs.data, docs, me]);

  const shown = useMemo(() => {
    const scored = items
      .map((it) => ({ it, s: Math.max(score(q, it.label) ?? -1, (score(q, it.hint) ?? -1) - 400) }))
      .filter((x) => x.s >= 0);
    scored.sort((a, b) => b.s - a.s);
    return scored.slice(0, 50).map((x) => x.it);
  }, [items, q]);

  const pick = (it: Item | undefined) => {
    if (!it) return;
    onClose();
    it.run();
  };
  return (
    <div className="palette-backdrop" onMouseDown={onClose}>
      <div
        className="palette"
        role="dialog"
        aria-modal="true"
        aria-label={t('palette.label')}
        onMouseDown={(e) => e.stopPropagation()}
      >
        <div className="palette-input">
          <Search size={16} />
          <input
            ref={input}
            role="combobox"
            aria-expanded="true"
            aria-controls="palette-list"
            aria-activedescendant={shown[sel] ? `pal-${sel}` : undefined}
            aria-label={t('palette.placeholder')}
            placeholder={t('palette.placeholder')}
            value={q}
            onChange={(e) => {
              setQ(e.target.value);
              setSel(0);
            }}
            onKeyDown={(e) => {
              if (e.key === 'Escape') onClose();
              else if (e.key === 'ArrowDown') {
                e.preventDefault();
                setSel((s) => Math.min(s + 1, shown.length - 1));
              } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                setSel((s) => Math.max(s - 1, 0));
              } else if (e.key === 'Enter') {
                e.preventDefault();
                pick(shown[sel]);
              }
            }}
          />
        </div>
        <ul id="palette-list" role="listbox" aria-label={t('palette.label')}>
          {shown.length === 0 && <li className="muted palette-empty">{t('palette.empty')}</li>}
          {shown.map((it, i) => {
            const Icon = it.icon;
            return (
              <li
                key={it.id}
                id={`pal-${i}`}
                role="option"
                aria-selected={i === sel}
                className={i === sel ? 'sel' : undefined}
                onMouseEnter={() => setSel(i)}
                onClick={() => pick(it)}
              >
                <Icon size={15} />
                <span className="palette-label">{it.label}</span>
                <span className="palette-hint">{it.hint}</span>
              </li>
            );
          })}
        </ul>
      </div>
    </div>
  );
}

// Monaco (MIT), loaded on first use. Only the editor core and the MySQL
// tokenizer are bundled; its one worker is a file under /studio/assets/
// (CSP worker-src 'self'). Completion comes from the catalog snapshot;
// check markers from the gateway (Sql.Check), 500 ms after typing stops.

import { useEffect, useRef } from 'react';
import * as monaco from 'monaco-editor/editor';
import 'monaco-editor/languages/definitions/mysql/register';
import EditorWorker from 'monaco-editor/editor/editor.worker?worker';
import { isDark } from '../app/theme';

declare global {
  interface Window {
    MonacoEnvironment?: { getWorker(id: string, label: string): Worker };
  }
}

self.MonacoEnvironment = { getWorker: () => new EditorWorker() };

export interface CompletionSource {
  keywords: string[];
  objects(): { name: string; detail: string }[];
  columns(): { name: string; detail: string }[];
}

let completionRegistered = false;
let source: CompletionSource | null = null;

function registerCompletion(): void {
  if (completionRegistered) return;
  completionRegistered = true;
  monaco.languages.registerCompletionItemProvider('mysql', {
    triggerCharacters: ['.', ' '],
    provideCompletionItems(model, position) {
      const word = model.getWordUntilPosition(position);
      const range = new monaco.Range(position.lineNumber, word.startColumn, position.lineNumber, word.endColumn);
      const s = source;
      if (!s) return { suggestions: [] };
      const K = monaco.languages.CompletionItemKind;
      return {
        suggestions: [
          ...s.objects().map((o) => ({ label: o.name, kind: K.Struct, detail: o.detail, insertText: o.name, range })),
          ...s.columns().map((c) => ({ label: c.name, kind: K.Field, detail: c.detail, insertText: c.name, range })),
          ...s.keywords.map((k) => ({ label: k, kind: K.Keyword, insertText: k, range })),
        ],
      };
    },
  });
}

export interface MonacoHandle {
  /** The selection's text, or '' without a selection. */
  selection(): string;
  /** The cursor's offset in the text. */
  offset(): number;
  /** Markers for problems: [startOffset, endOffset, message, warning?]. */
  setMarkers(markers: [number, number, string, boolean?][]): void;
  focus(): void;
}

export default function Monaco({
  value,
  onChange,
  onRun,
  completion,
  label,
  handle,
}: {
  value: string;
  onChange: (v: string) => void;
  onRun: () => void;
  completion: CompletionSource;
  label: string;
  handle: (h: MonacoHandle | null) => void;
}) {
  const host = useRef<HTMLDivElement>(null);
  const ed = useRef<monaco.editor.IStandaloneCodeEditor | null>(null);
  const run = useRef(onRun);
  const change = useRef(onChange);
  useEffect(() => {
    run.current = onRun;
    change.current = onChange;
    source = completion;
  });
  useEffect(() => {
    registerCompletion();
    if (!host.current) return;
    const e = monaco.editor.create(host.current, {
      value,
      language: 'mysql',
      theme: isDark() ? 'vs-dark' : 'vs',
      minimap: { enabled: false },
      automaticLayout: true,
      fontSize: 13,
      scrollBeyondLastLine: false,
      ariaLabel: label,
      accessibilitySupport: 'auto',
      tabSize: 2,
    });
    ed.current = e;
    e.onDidChangeModelContent(() => change.current(e.getValue()));
    e.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => run.current());
    handle({
      selection: () => {
        const sel = e.getSelection();
        const m = e.getModel();
        return sel && m && !sel.isEmpty() ? m.getValueInRange(sel) : '';
      },
      offset: () => {
        const m = e.getModel();
        const p = e.getPosition();
        return m && p ? m.getOffsetAt(p) : 0;
      },
      setMarkers: (ms) => {
        const m = e.getModel();
        if (!m) return;
        monaco.editor.setModelMarkers(
          m,
          'sprig',
          ms.map(([a, b, message, warning]) => {
            const s = m.getPositionAt(a);
            const t = m.getPositionAt(b);
            return {
              startLineNumber: s.lineNumber,
              startColumn: s.column,
              endLineNumber: t.lineNumber,
              endColumn: t.column,
              message,
              severity: warning ? monaco.MarkerSeverity.Warning : monaco.MarkerSeverity.Error,
            };
          }),
        );
      },
      focus: () => e.focus(),
    });
    // Follow the page's theme (the system's, or the user's choice).
    const retheme = () => monaco.editor.setTheme(isDark() ? 'vs-dark' : 'vs');
    const mq = window.matchMedia?.('(prefers-color-scheme: dark)');
    mq?.addEventListener('change', retheme);
    window.addEventListener('sprig-theme', retheme);
    return () => {
      mq?.removeEventListener('change', retheme);
      window.removeEventListener('sprig-theme', retheme);
      handle(null);
      e.dispose();
      ed.current = null;
    };
    // The editor is created once per mount; value changes flow out.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  useEffect(() => {
    const e = ed.current;
    if (e && e.getValue() !== value) e.setValue(value);
  }, [value]);
  return <div ref={host} className="editor-host" data-testid="sql-editor" />;
}

// Monaco (MIT), loaded on first use. Only the editor core and the MySQL
// tokenizer are bundled; its one worker is a file under /studio/assets/
// (CSP worker-src 'self'). Completion comes from the catalog snapshot;
// check markers from the gateway (Sql.Check), 500 ms after typing stops.

import { useEffect, useRef } from 'react';
import * as monaco from 'monaco-editor/editor';
import 'monaco-editor/languages/definitions/mysql/register';
// Cypher (graphs), JavaScript (MongoDB commands, logic), Redis and
// Lua, all Monarch tokenizers without workers of their own.
import 'monaco-editor/languages/definitions/cypher/register';
import 'monaco-editor/languages/definitions/javascript/register';
import 'monaco-editor/languages/definitions/redis/register';
import 'monaco-editor/languages/definitions/lua/register';
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

/** The editor languages Studio uses. */
export type EditorLanguage = 'mysql' | 'cypher' | 'javascript' | 'redis' | 'lua';

const completionRegistered = new Set<EditorLanguage>();
let source: CompletionSource | null = null;

function registerCompletion(lang: EditorLanguage): void {
  if (completionRegistered.has(lang)) return;
  completionRegistered.add(lang);
  monaco.languages.registerCompletionItemProvider(lang, {
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

let themesDefined = false;

/** Monaco's themes with SQL operators (CROSS JOIN, AND, LIKE) at 4.5:1
 * contrast or more (WCAG 1.4.3); Monaco's own grey is 3.6:1 on white. */
function defineThemes(): void {
  if (themesDefined) return;
  themesDefined = true;
  // The calmer palette of the workspace redesign: indigo
  // keywords, burnt-orange strings, coral numbers, muted comments; every
  // colour at 4.5:1 or more on its background.
  monaco.editor.defineTheme('sprig-light', {
    base: 'vs',
    inherit: true,
    rules: [
      { token: 'keyword', foreground: '4c5fd5' },
      { token: 'string', foreground: '9a4a17' },
      { token: 'number', foreground: 'b4441c' },
      { token: 'comment', foreground: '6b7280', fontStyle: 'italic' },
      { token: 'predefined', foreground: '8250df' },
      { token: 'predefined.sql', foreground: '8250df' },
      { token: 'keyword.sql', foreground: '4c5fd5' },
      { token: 'number.sql', foreground: 'b4441c' },
      { token: 'string.sql', foreground: '9a4a17' },
      { token: 'operator.sql', foreground: '57606a' },
      { token: 'operator', foreground: '57606a' },
      { token: 'delimiter', foreground: '57606a' },
      { token: 'delimiter.parenthesis', foreground: '57606a' },
      { token: 'delimiter.parenthesis.sql', foreground: '57606a' },
    ],
    colors: {
      'editor.background': '#ffffff',
      'editorLineNumber.foreground': '#9aa1ab',
      'editorLineNumber.activeForeground': '#1c2026',
      'editor.selectionBackground': '#dfe3ea',
      'editorBracketHighlight.foreground1': '#57606a',
      'editorBracketHighlight.foreground2': '#57606a',
      'editorBracketHighlight.foreground3': '#57606a',
      'editorBracketHighlight.foreground4': '#57606a',
      'editorBracketHighlight.foreground5': '#57606a',
      'editorBracketHighlight.foreground6': '#57606a',
    },
  });
  monaco.editor.defineTheme('sprig-dark', {
    base: 'vs-dark',
    inherit: true,
    rules: [
      { token: 'keyword', foreground: '8fa4ff' },
      { token: 'string', foreground: 'f0a36c' },
      { token: 'number', foreground: 'f08a6a' },
      { token: 'comment', foreground: '8b929c', fontStyle: 'italic' },
      { token: 'predefined', foreground: 'c4a7f5' },
      { token: 'predefined.sql', foreground: 'c4a7f5' },
      { token: 'keyword.sql', foreground: '8fa4ff' },
      { token: 'number.sql', foreground: 'f08a6a' },
      { token: 'string.sql', foreground: 'f0a36c' },
      { token: 'operator.sql', foreground: 'a8b3bf' },
      { token: 'operator', foreground: 'a8b3bf' },
      { token: 'delimiter', foreground: 'a8b3bf' },
      { token: 'delimiter.parenthesis', foreground: 'a8b3bf' },
      { token: 'delimiter.parenthesis.sql', foreground: 'a8b3bf' },
    ],
    colors: {
      'editor.background': '#17191d',
      'editorLineNumber.foreground': '#6f7682',
      'editorLineNumber.activeForeground': '#e6e8eb',
      'editor.selectionBackground': '#343a44',
      'editorBracketHighlight.foreground1': '#a8b3bf',
      'editorBracketHighlight.foreground2': '#a8b3bf',
      'editorBracketHighlight.foreground3': '#a8b3bf',
      'editorBracketHighlight.foreground4': '#a8b3bf',
      'editorBracketHighlight.foreground5': '#a8b3bf',
      'editorBracketHighlight.foreground6': '#a8b3bf',
    },
  });
}

function themeName(): string {
  return isDark() ? 'sprig-dark' : 'sprig-light';
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
  language = 'mysql',
}: {
  language?: EditorLanguage;
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
    registerCompletion(language);
    defineThemes();
    if (!host.current) return;
    const e = monaco.editor.create(host.current, {
      value,
      language,
      theme: themeName(),
      minimap: { enabled: false },
      automaticLayout: true,
      fontSize: 13,
      scrollBeyondLastLine: false,
      ariaLabel: label,
      accessibilitySupport: 'auto',
      tabSize: 2,
      // Plain brackets: the rainbow colouring fights the calmer palette.
      bracketPairColorization: { enabled: false },
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
    const retheme = () => monaco.editor.setTheme(themeName());
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
  useEffect(() => {
    const m = ed.current?.getModel();
    if (m && m.getLanguageId() !== language) {
      registerCompletion(language);
      monaco.editor.setModelLanguage(m, language);
    }
  }, [language]);
  return <div ref={host} className="editor-host" data-testid="sql-editor" />;
}

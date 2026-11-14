// KIRun functions in @fincity/kirun-ui (MIT; with KIRun itself, the only
// Modlix parts allowed, guide 12.18): the visual step graph and the text
// form, inside Studio's layout and theme. The repository is KIRun's own
// functions plus signatures of SprigDB's (Sprig.Db.*, Sprig.Http.Fetch,
// Sprig.Mq.*), so the editor validates and completes them; they run only in
// the gateway.

import { useEffect, useMemo, useState } from 'react';
import { KIRunEditor } from '@fincity/kirun-ui';
import '@fincity/kirun-ui/dist/module.css';
import {
  Event,
  FunctionOutput,
  FunctionSignature,
  HybridRepository,
  KIRunFunctionRepository,
  KIRunSchemaRepository,
  Parameter,
  type Function as KFunction,
  type Repository,
} from '@fincity/kirun-js';
import { SPRIG_FUNCTIONS, type Sig } from './sprigFunctions';

/** A signature-only function: the editor validates against it; running it
 * in the browser is refused (it runs in the gateway). */
function stub(s: Sig): KFunction {
  const sig = new FunctionSignature(s.name)
    .setNamespace(s.ns)
    .setParameters(new Map(s.params.map(([n, schema, variable]) => [n, Parameter.of(n, schema, variable ?? false)])))
    .setEvents(new Map([Event.outputEventMapEntry(new Map(s.out))]));
  return {
    getSignature: () => sig,
    getProbableEventSignature: () => sig.getEvents(),
    execute: () => Promise.resolve(new FunctionOutput([])),
  } as Partial<KFunction> as KFunction;
}

class SprigRepository implements Repository<KFunction> {
  private readonly fns = new Map(SPRIG_FUNCTIONS.map((s) => [`${s.ns}.${s.name}`, stub(s)]));
  find(namespace: string, name: string): Promise<KFunction | undefined> {
    return Promise.resolve(this.fns.get(`${namespace}.${name}`));
  }
  filter(name: string): Promise<string[]> {
    const n = name.toLowerCase();
    return Promise.resolve([...this.fns.keys()].filter((k) => k.toLowerCase().includes(n)));
  }
}

function isDarkPage(): boolean {
  const bg = getComputedStyle(document.documentElement).getPropertyValue('--bg').trim().replace('#', '');
  return parseInt(bg.slice(0, 2) || 'ff', 16) < 0x80;
}

export default function KirunEditor({
  value,
  onChange,
  readOnly,
}: Readonly<{ value: string; onChange: (text: string) => void; readOnly?: boolean }>) {
  const fnRepo = useMemo(() => new HybridRepository<KFunction>(new KIRunFunctionRepository(), new SprigRepository()), []);
  const schemaRepo = useMemo(() => new KIRunSchemaRepository(), []);
  const [dark, setDark] = useState(isDarkPage);
  useEffect(() => {
    const re = () => setDark(isDarkPage());
    window.addEventListener('sprig-theme', re);
    const mq = window.matchMedia?.('(prefers-color-scheme: dark)');
    mq?.addEventListener('change', re);
    return () => {
      window.removeEventListener('sprig-theme', re);
      mq?.removeEventListener('change', re);
    };
  }, []);
  const def = useMemo(() => {
    try {
      return JSON.parse(value) as unknown;
    } catch {
      return undefined;
    }
  }, [value]);
  if (def === undefined) {
    return (
      <p className="error" role="alert">
        The definition isn&apos;t valid JSON; fix it in the text below.
      </p>
    );
  }
  return (
    <div className="kirun-host" data-testid="kirun-editor">
      <KIRunEditor
        key={dark ? 'dark' : 'light'}
        functionDefinition={def}
        onChange={(d: unknown) => onChange(JSON.stringify(d, null, 2))}
        functionRepository={fnRepo}
        schemaRepository={schemaRepo}
        readOnly={readOnly}
        defaultTheme={dark ? 'dark' : 'light'}
        showThemeSelector={false}
        style={{ height: '100%' }}
      />
    </div>
  );
}

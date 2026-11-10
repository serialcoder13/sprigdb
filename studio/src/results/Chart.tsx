import { useEffect, useRef, useState } from 'react';
import type { ResultBuffer } from './buffer';
import { isDark } from '../app/theme';

/** A chart of a result (Apache ECharts, loaded on first use): the first
 * column as categories, numeric columns as series. The grid is the
 * accessible alternative. */
export function Chart({ buffer }: { buffer: ResultBuffer }) {
  const host = useRef<HTMLDivElement>(null);
  const [kind, setKind] = useState<'bar' | 'line'>('bar');
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let disposed = false;
    let chart: { dispose(): void; setOption(o: unknown): void } | null = null;
    void (async () => {
      try {
        const [core, charts, comps, renderers] = await Promise.all([
          import('echarts/core'),
          import('echarts/charts'),
          import('echarts/components'),
          import('echarts/renderers'),
        ]);
        core.use([
          charts.BarChart,
          charts.LineChart,
          comps.GridComponent,
          comps.TooltipComponent,
          comps.LegendComponent,
          renderers.SVGRenderer,
        ]);
        if (disposed || !host.current) return;
        chart = core.init(host.current, isDark() ? 'dark' : undefined, { renderer: 'svg' });
        const n = Math.min(buffer.rowCount, 2000);
        const cats = Array.from({ length: n }, (_, r) => buffer.cell(r, 0) ?? 'NULL');
        const series = buffer.columns
          .map((c, i) => ({ c, i }))
          .filter(({ i }) => i > 0 && buffer.numeric(i))
          .map(({ c, i }) => ({
            name: c.name,
            type: kind,
            data: Array.from({ length: n }, (_, r) => Number(buffer.cell(r, i) ?? NaN)),
          }));
        if (!series.length) {
          setError('No numeric column after the first: nothing to chart.');
          return;
        }
        chart.setOption({
          backgroundColor: 'transparent',
          tooltip: { trigger: 'axis' },
          legend: {},
          xAxis: { type: 'category', data: cats },
          yAxis: { type: 'value' },
          series,
        });
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => {
      disposed = true;
      chart?.dispose();
    };
  }, [buffer, kind]);
  return (
    <div className="stack">
      <div className="row" role="radiogroup" aria-label="Chart type">
        <label>
          <input type="radio" checked={kind === 'bar'} onChange={() => setKind('bar')} /> Bar
        </label>
        <label>
          <input type="radio" checked={kind === 'line'} onChange={() => setKind('line')} /> Line
        </label>
      </div>
      {error && <p className="muted">{error}</p>}
      <div ref={host} style={{ blockSize: 320 }} role="img" aria-label="Chart of the result; the grid holds the same data" />
    </div>
  );
}

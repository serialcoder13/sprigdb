import { describe, expect, it } from 'vitest';
import { caption, expandQuery, graphOf, hasGraph, merge } from './elements';
import { classifyCypher, cypherAt, splitCypher } from './cypher';

const n = (id: number, labels: string[], props: Record<string, unknown> = {}) => JSON.stringify({ $node: { id, labels, props } });
const r = (id: number, start: number, end: number, type: string) => JSON.stringify({ $rel: { id, start, end, type, props: { since: 2020 } } });

describe('graph elements of a Cypher result (step 27)', () => {
  const rows = [
    [n(1, ['Person'], { name: 'Asha' }), r(10, 1, 2, 'KNOWS'), n(2, ['Person'], { name: 'Ben' })],
    [n(1, ['Person'], { name: 'Asha' }), r(11, 1, 3, 'LIKES'), n(3, ['Movie'], { title: 'Up' })],
    ['42', 'x', null],
  ];
  const cell = (ri: number, c: number) => rows[ri]?.[c] ?? null;

  it('collects distinct nodes and relationships, ignoring scalars', () => {
    const g = graphOf(3, 3, cell, 100);
    expect([...g.nodes.keys()]).toEqual(['1', '2', '3']);
    expect([...g.edges.keys()]).toEqual(['10', '11']);
    expect(g.edges.get('11')).toMatchObject({ start: '1', end: '3', type: 'LIKES' });
    expect(g.dangling).toBe(0);
    expect(hasGraph(3, 3, cell)).toBe(true);
    expect(hasGraph(1, 1, () => '{"a":1}')).toBe(false);
  });

  it('reads paths, lists and maps, and counts relationships whose ends are missing', () => {
    const path = JSON.stringify({
      $path: [{ $node: { id: 5, labels: ['A'], props: {} } }, { $rel: { id: 50, start: 5, end: 6, type: 'T', props: {} } }, { $node: { id: 6, labels: ['B'], props: {} } }],
    });
    const list = JSON.stringify([{ $node: { id: 7, labels: [], props: {} } }, { m: { $rel: { id: 70, start: 7, end: 99, type: 'X', props: {} } } }]);
    const g = graphOf(1, 2, (_r, c) => (c === 0 ? path : list), 100);
    expect(g.nodes.size).toBe(3);
    expect(g.edges.size).toBe(2);
    expect(g.dangling).toBe(1);
  });

  it('stops at studio.graph_max_elements and says how many were dropped', () => {
    const g = graphOf(3, 3, cell, 3);
    expect(g.nodes.size + g.edges.size).toBe(3);
    expect(g.dropped).toBe(2);
  });

  it('expand merges new neighbours without duplicates and keeps the cap', () => {
    const g = graphOf(1, 3, cell, 10);
    const more = graphOf(1, 3, (_r, c) => [n(2, ['Person']), r(12, 2, 4, 'KNOWS'), n(4, ['Person'])][c] ?? null, 10);
    const m = merge(g, more, 10);
    expect([...m.nodes.keys()].sort()).toEqual(['1', '2', '4']);
    expect(m.edges.size).toBe(2);
    expect(expandQuery('17', 50)).toBe('MATCH (n)-[r]-(m) WHERE id(n) = 17 RETURN n, r, m LIMIT 50');
    expect(expandQuery("x'1", 5)).toContain("id(n) = 'x\\'1'");
  });

  it('captions nodes by name, title or label', () => {
    expect(caption({ id: '1', labels: ['P'], props: { name: 'Asha' } })).toBe('Asha');
    expect(caption({ id: '1', labels: ['P'], props: {} })).toBe('P 1');
  });
});

describe('Cypher text', () => {
  it('splits on ; outside strings, names and comments', () => {
    const t = "MATCH (n {s: 'a;b'}) RETURN n; // c;\nCREATE (`x;y`:L) /* ; */ ;\n";
    const s = splitCypher(t);
    expect(s.map((x) => x.text)).toEqual(["MATCH (n {s: 'a;b'}) RETURN n", '// c;\nCREATE (`x;y`:L) /* ; */']);
    expect(t.slice(s[1]!.start, s[1]!.end)).toBe(s[1]!.text);
    expect(cypherAt(t, t.indexOf('CREATE') + 2)?.text).toBe(s[1]!.text);
  });

  it('classifies for studio.confirm_writes', () => {
    expect(classifyCypher('MATCH (n) RETURN n')).toBe('read');
    expect(classifyCypher("MATCH (n) WHERE n.s = 'CREATE' RETURN n")).toBe('read');
    expect(classifyCypher('MATCH (n) SET n.x = 1')).toBe('write');
    expect(classifyCypher('MATCH (n) DETACH DELETE n')).toBe('write');
    expect(classifyCypher('CREATE GRAPH social')).toBe('ddl');
    expect(classifyCypher('CREATE INDEX FOR (p:P) ON (p.name)')).toBe('ddl');
  });
});

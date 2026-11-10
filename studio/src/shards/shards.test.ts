import { describe, expect, it } from 'vitest';
import { keyText, rangeText, shardSummary, type ShardGroup } from './shards';

const g: ShardGroup = {
  group: 3,
  kind: 'sharded',
  name: 'customers',
  key_spec: { parts: [{ class: 'numeric' }] },
  state: 'active',
  min_shards: null,
  origin: 'advisor',
  advisor_run: '7',
  updated_at: null,
  members: [
    { db: 'shop', table: 'orders', routing: 'parent', key_columns: null, parent: 'customers', parent_fk: 'fk_c', rekey_state: 'absent', pinned: false },
    { db: 'shop', table: 'customers', routing: 'columns', key_columns: ['customer_id'], parent: null, parent_fk: null, rekey_state: 'absent', pinned: false },
  ],
  map: {
    version: 4,
    state: 'active',
    shards: [
      { shard: 1, lo: '0', hi: '8000000000000000', cluster: 'dc-a-c1', state: 'serving', epoch: 1, job: null },
      { shard: 2, lo: '8000000000000000', hi: null, cluster: 'dc-a-c2', state: 'held', epoch: 2, job: 'j1' },
    ],
  },
};

describe('shards view', () => {
  it('names the key by the root member and its class', () => {
    expect(keyText(g)).toBe('customer_id (numeric)');
    expect(keyText({ ...g, members: [], key_spec: null })).toBe('(not chosen yet)');
    expect(keyText({ ...g, kind: 'default', members: [], key_spec: null })).toBe('—');
  });
  it('summarizes the map', () => {
    expect(shardSummary(g)).toEqual({ shards: 2, clusters: ['dc-a-c1', 'dc-a-c2'], held: 1 });
    expect(rangeText(g.map!.shards[1]!)).toBe('8000000000000000 – 2^64');
    expect(shardSummary({ ...g, map: undefined })).toEqual({ shards: 0, clusters: [], held: 0 });
  });
});

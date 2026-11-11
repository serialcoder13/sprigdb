// SprigDB's KIRun functions (crate sprig-logic-js, `kirun.rs`), as
// signatures for the editor's validation and completion.

import { Schema } from '@fincity/kirun-js';

export type Sig = { ns: string; name: string; params: [string, Schema, boolean?][]; out: [string, Schema][] };

const any = (n: string) => Schema.ofAny(n);

/** SprigDB's KIRun functions (crate sprig-logic-js, `kirun.rs`). */
export const SPRIG_FUNCTIONS: Sig[] = [
  { ns: 'Sprig.Db', name: 'Query', params: [['sql', Schema.ofString('sql')], ['args', any('args'), true]], out: [['rows', any('rows')]] },
  {
    ns: 'Sprig.Db',
    name: 'QueryOne',
    params: [['sql', Schema.ofString('sql')], ['args', any('args'), true]],
    out: [
      ['row', any('row')],
      ['found', Schema.ofBoolean('found')],
    ],
  },
  { ns: 'Sprig.Db', name: 'Exec', params: [['sql', Schema.ofString('sql')], ['args', any('args'), true]], out: [['affected', Schema.ofLong('affected')]] },
  {
    ns: 'Sprig.Http',
    name: 'Fetch',
    params: [['url', Schema.ofString('url')], ['method', any('method')], ['headers', any('headers')], ['body', any('body')]],
    out: [
      ['status', Schema.ofInteger('status')],
      ['headers', Schema.ofObject('headers')],
      ['body', Schema.ofString('body')],
    ],
  },
  { ns: 'Sprig.Db', name: 'Publish', params: [['stream', any('stream')], ['key', any('key')], ['value', any('value')]], out: [['affected', Schema.ofLong('affected')]] },
  { ns: 'Sprig.Db', name: 'CommitOffset', params: [['stream', any('stream')], ['group', any('group')], ['offset', any('offset')]], out: [] },
  { ns: 'Sprig.Mq', name: 'Publish', params: [['exchange', any('exchange')], ['routingKey', any('routingKey')], ['body', any('body')]], out: [] },
  { ns: 'Sprig.Mq', name: 'Get', params: [['queue', any('queue')]], out: [['message', any('message')]] },
  { ns: 'Sprig.Mq', name: 'Ack', params: [['deliveryTag', any('deliveryTag')]], out: [] },
  { ns: 'Sprig.Mq', name: 'Nack', params: [['deliveryTag', any('deliveryTag')]], out: [] },
];


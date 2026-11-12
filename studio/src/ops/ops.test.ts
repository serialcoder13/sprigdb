import { describe, expect, it } from 'vitest';
import { confirmTyped, legacyOperatorMode, moveLag } from './ops';

describe('operator mode', () => {
  it('asks for the target name and a reason', () => {
    const answers = (a: (string | null)[]) => () => a.shift() ?? null;
    expect(confirmTyped('acme', 'Run', answers(['acme', 'hot shard']))).toBe('hot shard');
    expect(confirmTyped('acme', 'Run', answers(['acm', 'x']))).toBeNull();
    expect(confirmTyped('acme', 'Run', answers(['acme', '  ']))).toBeNull();
    expect(confirmTyped('acme', 'Run', answers([null]))).toBeNull();
  });
  it('shows lag in ms or seconds', () => {
    const j = { id: 'x', kind: 'move', tenant: 1, state: 'copying', source: 'a', target: 'b' };
    expect(moveLag({ ...j, progress: { lag_ms: 48 } })).toBe('48 ms');
    expect(moveLag({ ...j, progress: { lag_ms: 2500 } })).toBe('2.5 s');
    expect(moveLag({ ...j, progress: null })).toBe('');
  });
});

describe('named operators', () => {
  it('opens a session only from a login that returned a token', async () => {
    const { sessionOf, operatorHeaders } = await import('./ops');
    expect(sessionOf({ operator: { name: 'alice', role: 'platform_operator', dcs: [] }, enrol_ticket: 't' })).toBeNull();
    const s = sessionOf({ token: 'sprigop_x', expires_ms: 10, operator: { name: 'alice', role: 'platform_operator', dcs: [] } });
    expect(s).toEqual({ token: 'sprigop_x', name: 'alice', expires_ms: 10 });
    expect(operatorHeaders(s)).toEqual({ 'x-sprig-operator': 'sprigop_x' });
    expect(operatorHeaders(null)).toEqual({});
  });
});

describe('operator sign-in', () => {
  it('sends six digits as a code and anything else as a recovery code', async () => {
    const { loginBody } = await import('./ops');
    expect(loginBody(' alice ', 'pw', '')).toEqual({ name: 'alice', password: 'pw' });
    expect(loginBody('alice', 'pw', '123 456')).toEqual({ name: 'alice', password: 'pw', code: '123456' });
    expect(loginBody('alice', 'pw', 'abcde-fghij')).toEqual({ name: 'alice', password: 'pw', recovery_code: 'abcde-fghij' });
    expect(loginBody('alice', 'pw', '12345')).toEqual({ name: 'alice', password: 'pw', recovery_code: '12345' });
  });
  it('reads the secret of an otpauth link', async () => {
    const { secretOf } = await import('./ops');
    expect(secretOf('otpauth://totp/SprigDB:alice?secret=JBSWY3DPEHPK3PXP&issuer=SprigDB')).toBe('JBSWY3DPEHPK3PXP');
    expect(secretOf('not a link')).toBe('');
  });
});

describe('the older operator mode', () => {
  it('is deprecated only while no named operator is signed in', () => {
    const named = { token: 't', name: 'ana', expires_ms: 1 };
    expect(legacyOperatorMode(['operator'], null)).toBe(true);
    expect(legacyOperatorMode(['operator'], named)).toBe(false);
    expect(legacyOperatorMode([], null)).toBe(false);
    expect(legacyOperatorMode(undefined, null)).toBe(false);
  });
});

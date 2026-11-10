import { describe, expect, it } from 'vitest';
import { DEV_OK, licenceOk, packageAllowed, RUNTIME_OK } from './check-licences.mjs';

describe('licence allowlist', () => {
  it('accepts permissive licences and expressions', () => {
    expect(licenceOk('MIT', RUNTIME_OK)).toBe(true);
    expect(licenceOk('(MPL-2.0 OR Apache-2.0)', RUNTIME_OK)).toBe(true);
    expect(licenceOk('MPL-2.0', RUNTIME_OK)).toBe(false);
    expect(licenceOk('MPL-2.0', DEV_OK)).toBe(true);
  });
  it('refuses copyleft and source-available licences', () => {
    for (const l of ['GPL-3.0', 'AGPL-3.0-only', 'LGPL-2.1', 'SSPL-1.0', 'BUSL-1.1', 'BSL-1.1']) {
      expect(licenceOk(l, DEV_OK)).toBe(false);
    }
    expect(licenceOk('MIT OR GPL-3.0', RUNTIME_OK)).toBe(true);
  });
  it('allows only KIRun from Modlix', () => {
    expect(packageAllowed('@fincity/kirun-ui')).toBe(true);
    expect(packageAllowed('@fincity/kirun-js')).toBe(true);
    expect(packageAllowed('@fincity/nocode-ui')).toBe(false);
    expect(packageAllowed('@fincity/anything')).toBe(false);
    expect(packageAllowed('modlix-widgets')).toBe(false);
    expect(packageAllowed('react')).toBe(true);
  });
});

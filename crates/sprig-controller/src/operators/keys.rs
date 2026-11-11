//! The keys that seal operators' TOTP seeds: derived from the controller's
//! master key file (`[controller] workload_key_file`, the file workers and
//! gateways use for tenant data keys) for the purpose
//! [`sprig_auth::operator::KEY_PURPOSE`]. Every controller of the
//! deployment needs the same file, as the seeds live in the global tier.

use sprig_auth::mfa::FactorKeys;
use sprig_common::ids::TenantId;
use sprig_workload::keyring::MasterKeyring;

/// [`FactorKeys`] over a master keyring.
pub struct OperatorKeys(pub MasterKeyring);

impl std::fmt::Debug for OperatorKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperatorKeys").finish_non_exhaustive()
    }
}

impl FactorKeys for OperatorKeys {
    fn current(&self, tenant: TenantId) -> Option<(u32, [u8; 32])> {
        self.0
            .current_for(sprig_auth::operator::KEY_PURPOSE, tenant)
            .map(|(v, k)| (v, *k.bytes()))
    }

    fn key(&self, tenant: TenantId, version: u32) -> Option<[u8; 32]> {
        self.0
            .key_for(sprig_auth::operator::KEY_PURPOSE, tenant, version)
            .map(|k| *k.bytes())
    }
}

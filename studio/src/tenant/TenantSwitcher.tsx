import { useNavigate, useParams } from 'react-router';
import { useIntl } from 'react-intl';
import { useSession } from '../auth/session';

/** The tenant tree from WhoAmI (chapter 44, "Tenant switcher"): the home
 * tenant and, for SPRIG_TENANT_ADMIN, its descendants, with paths. */
export function TenantSwitcher() {
  const intl = useIntl();
  const me = useSession((s) => s.me);
  const { tenant = '' } = useParams();
  const navigate = useNavigate();
  const tenants = me?.tenants ?? [];
  if (tenants.length <= 1) {
    return (
      <span className="badge" title={intl.formatMessage({ id: 'tenant.label' })} data-testid="tenant">
        {me?.principal.tenant ?? '-'}
      </span>
    );
  }
  return (
    <label className="row">
      <span className="visually-hidden">{intl.formatMessage({ id: 'tenant.label' })}</span>
      <select
        aria-label={intl.formatMessage({ id: 'tenant.label' })}
        value={tenant}
        data-testid="tenant"
        onChange={(e) => void navigate(`/t/${encodeURIComponent(e.target.value)}/settings`)}
        title={intl.formatMessage({ id: 'tenant.dataNote' })}
      >
        {tenants.map((t) => (
          <option key={t.id ?? t.name} value={t.name ?? ''}>
            {t.relation === 'home' ? `${t.name} (${intl.formatMessage({ id: 'tenant.home' })})` : t.path}
          </option>
        ))}
      </select>
    </label>
  );
}

/** Whether the route's tenant is the signed-in user's home tenant. */
export function useIsHomeTenant(): boolean {
  const me = useSession((s) => s.me);
  const { tenant = '' } = useParams();
  return !me || me.principal.tenant === tenant;
}

import { useIsFetching, useIsMutating } from '@tanstack/react-query';
import { useParams } from 'react-router';
import { useIntl } from 'react-intl';
import { useSession } from '../auth/session';
import { useTabs } from '../editor/tabs';
import { Mascot } from '../app/Mascot';
import { useRel } from './WorkspaceTabs';

/** The status bar: connection, tenant, database and gateway,
 * with the mascot showing whether Studio is busy. */
export function StatusBar() {
  const intl = useIntl();
  const { tenant = '' } = useParams();
  const { rel } = useRel();
  const me = useSession((s) => s.me);
  const status = useSession((s) => s.status);
  const busy = useIsFetching() + useIsMutating() > 0;
  const tabs = useTabs((s) => s.tabs);
  const parts = rel.split('/').map(decodeURIComponent);
  const db =
    parts[0] === 'db' ? parts[1] : parts[0] === 'q' ? (tabs.find((t) => t.id === parts[1])?.db ?? null) : null;
  const connected = status === 'active';
  return (
    <footer className="statusbar" aria-label={intl.formatMessage({ id: 'status.label' })}>
      <Mascot pose={!connected ? 'sleeping' : busy ? 'working' : 'avatar'} size={18} />
      <span className={`dot${connected ? ' ok' : ''}`} aria-hidden="true" />
      <span data-testid="status-connection">
        {intl.formatMessage({ id: !connected ? 'status.offline' : busy ? 'status.working' : 'status.connected' })}
      </span>
      <span className="sep" aria-hidden="true">
        |
      </span>
      <span>{[tenant, db].filter(Boolean).join(' · ')}</span>
      {me?.gateway && (
        <>
          <span className="sep" aria-hidden="true">
            |
          </span>
          <span>
            {me.gateway.id} · {me.gateway.dc}
          </span>
        </>
      )}
      <span className="grow" />
      <span>{me?.principal.login}</span>
    </footer>
  );
}

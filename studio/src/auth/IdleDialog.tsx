import { useEffect, useRef } from 'react';
import { FormattedMessage } from 'react-intl';
import { logout, staySignedIn, useSession } from './session';

/** The "stay signed in" prompt two minutes before the idle limit
 * (WCAG 2.2.1: users are warned and can extend). */
export function IdleDialog() {
  const open = useSession((s) => s.idleWarning);
  const ref = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const d = ref.current;
    if (!d) return;
    if (open && !d.open) d.showModal?.();
    if (!open && d.open) d.close();
  }, [open]);
  return (
    <dialog ref={ref} aria-labelledby="idle-title" aria-describedby="idle-body">
      <h2 id="idle-title">
        <FormattedMessage id="session.idleTitle" />
      </h2>
      <p id="idle-body">
        <FormattedMessage id="session.idleBody" />
      </p>
      <div className="row">
        <button className="primary" autoFocus onClick={() => void staySignedIn()}>
          <FormattedMessage id="session.stay" />
        </button>
        <button onClick={() => void logout()}>
          <FormattedMessage id="session.signOut" />
        </button>
      </div>
    </dialog>
  );
}

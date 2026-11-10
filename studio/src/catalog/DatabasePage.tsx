import { Link, useNavigate, useParams } from 'react-router';
import { useSnapshot } from './snapshot';
import { addTab } from '../editor/tabs';

/** A logical database: its objects with estimates. */
export function DatabasePage() {
  const { tenant = '', db = '' } = useParams();
  const snap = useSnapshot(db);
  const navigate = useNavigate();
  const base = `/t/${encodeURIComponent(tenant)}/db/${encodeURIComponent(db)}`;
  return (
    <div className="stack">
      <div className="row">
        <h1 style={{ margin: 0 }}>{db}</h1>
        <button
          onClick={() => {
            const t = addTab({ db, title: db });
            void navigate(`/t/${encodeURIComponent(tenant)}/q/${t.id}`);
          }}
        >
          New query
        </button>
        <Link to={`/t/${encodeURIComponent(tenant)}/settings?scope=database&db=${encodeURIComponent(db)}`}>Settings</Link>
      </div>
      {snap.error && <p className="error">{snap.error.message}</p>}
      <table>
        <thead>
          <tr>
            <th scope="col">Name</th>
            <th scope="col">Kind</th>
            <th scope="col">Rows (estimate)</th>
            <th scope="col">Comment</th>
          </tr>
        </thead>
        <tbody>
          {(snap.data?.objects ?? []).map((o) => (
            <tr key={`${o.kind}:${o.name}`}>
              <td>
                <Link to={`${base}/obj/${o.kind}/${encodeURIComponent(o.name)}`}>{o.name}</Link>
              </td>
              <td>{o.kind}</td>
              <td>{o.rows ?? ''}</td>
              <td>{o.comment}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

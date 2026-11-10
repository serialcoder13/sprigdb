import { useEffect, type ReactNode } from 'react';
import { createBrowserRouter, Navigate, RouterProvider, useLocation } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { IntlProvider } from 'react-intl';
import { en } from '../i18n/en';
import { boot, useSession } from '../auth/session';
import { LoginPage } from '../auth/LoginPage';
import { Shell } from './Shell';
import { ErrorBoundary } from './ErrorBoundary';
import { TenantHome } from './TenantHome';
import { ObjectPage } from '../catalog/ObjectPage';
import { DatabasePage } from '../catalog/DatabasePage';
import { QueryPage } from '../editor/QueryPage';
import { SettingsPage } from '../settings/SettingsPage';
import { IndexingPage } from '../indexing/IndexingPage';
import { AccessPage } from '../access/AccessPage';
import { isApiError } from '../api/errors';

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Studio never retries writes; reads retry once unless refused.
      retry: (n, e) => n < 1 && !(isApiError(e) && e.status < 500),
      refetchOnWindowFocus: false,
      staleTime: 30_000,
    },
    mutations: { retry: false },
  },
});

function Booting() {
  return (
    <main className="main" aria-busy="true">
      <p className="muted">Loading Studio…</p>
    </main>
  );
}

/** Pages that need a session. */
function RequireSession({ children }: { children: ReactNode }) {
  const status = useSession((s) => s.status);
  const loc = useLocation();
  if (status === 'booting') return <Booting />;
  if (status !== 'active') {
    const next = encodeURIComponent(loc.pathname + loc.search);
    return <Navigate to={`/login?next=${next}`} replace />;
  }
  return <>{children}</>;
}

function Home() {
  const me = useSession((s) => s.me);
  const status = useSession((s) => s.status);
  if (status === 'booting') return <Booting />;
  if (!me) return <Navigate to="/login" replace />;
  return <Navigate to={`/t/${encodeURIComponent(me.principal.tenant ?? '-')}`} replace />;
}

export const routes = [
  { path: '/', element: <Home /> },
  { path: '/login', element: <LoginPage /> },
  {
    path: '/t/:tenant',
    element: (
      <RequireSession>
        <Shell />
      </RequireSession>
    ),
    children: [
      { index: true, element: <TenantHome /> },
      { path: 'db/:db', element: <DatabasePage /> },
      { path: 'db/:db/obj/:kind/:name', element: <ObjectPage /> },
      { path: 'q/:tab', element: <QueryPage /> },
      { path: 'indexing', element: <IndexingPage /> },
      { path: 'settings', element: <SettingsPage /> },
      { path: 'access/:area', element: <AccessPage /> },
      { path: 'access', element: <Navigate to="users" replace /> },
    ],
  },
  { path: '*', element: <Navigate to="/" replace /> },
];

const router = createBrowserRouter(routes, { basename: '/studio' });

export function App() {
  useEffect(() => {
    void boot();
    const signedOut = () => queryClient.clear();
    window.addEventListener('sprig-signed-out', signedOut);
    return () => window.removeEventListener('sprig-signed-out', signedOut);
  }, []);
  return (
    <IntlProvider locale="en" messages={en} defaultLocale="en">
      <QueryClientProvider client={queryClient}>
        <ErrorBoundary>
          <RouterProvider router={router} />
        </ErrorBoundary>
      </QueryClientProvider>
    </IntlProvider>
  );
}

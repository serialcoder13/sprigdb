import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import './app/theme.css';
import { App } from './app/App';
import { applyTheme } from './app/theme';
import { reportError } from './app/report';
import { installCursorHooks } from './results/cursor';

applyTheme();
installCursorHooks();
window.addEventListener('error', (e) => reportError('js', e.error ?? e.message));
window.addEventListener('unhandledrejection', (e) => reportError('js', e.reason));

const root = document.getElementById('root');
if (root) {
  createRoot(root).render(
    <StrictMode>
      <App />
    </StrictMode>,
  );
}

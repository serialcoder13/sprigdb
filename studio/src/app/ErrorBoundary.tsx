import { Component, type ErrorInfo, type ReactNode } from 'react';
import { reportError } from './report';

interface State {
  error: Error | null;
}

/** Shows an error with a reload button and reports it (chapter 44). */
export class ErrorBoundary extends Component<{ children: ReactNode }, State> {
  override state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  override componentDidCatch(error: Error, _info: ErrorInfo): void {
    const chunk = /dynamically imported module|Loading chunk|Failed to fetch/i.test(error.message);
    reportError(chunk ? 'chunk_load' : 'js', error);
  }

  override render() {
    const e = this.state.error;
    if (!e) return this.props.children;
    const chunk = /dynamically imported module|Loading chunk/i.test(e.message);
    return (
      <main className="main" role="alert">
        <h1>{chunk ? 'Studio was updated' : 'Something went wrong'}</h1>
        <p>
          {chunk
            ? 'This gateway serves a newer Studio. Reload to continue; your drafts are kept.'
            : e.message}
        </p>
        <button className="primary" onClick={() => window.location.reload()}>
          Reload Studio
        </button>
      </main>
    );
  }
}

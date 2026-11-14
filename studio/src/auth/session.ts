// The browser session.
//
//   boot():   version.json; refresh(); ok → WhoAmI → active; 401 → signed_out
//   login():  refused on plain HTTP from another host before sending the
//             password; POST /v1/auth/login with the cookie transport
//   refresh(): navigator.locks "sprig-refresh": a token another tab got in
//             the last 10 s is taken from the BroadcastChannel, else
//             POST /v1/auth/refresh with X-Sprig-CSRF (the browser sends the
//             HttpOnly cookie); the new token is broadcast to the other tabs
//   the access token lives in memory only, refreshed at 80% of expires_in
//   idle: a warning two minutes before studio.session_idle_minutes, then
//   logout (WCAG 2.2.1)
//
// The lock matters: the gateway treats a reused refresh token as theft and
// revokes the session, so two tabs refreshing at once would sign out.

import { create } from 'zustand';
import { ApiError, errorText } from '../api/errors';
import { CLIENT_HEADER, json, setAuthHooks } from '../api/client';
import type { TokenReply, WhoAmI } from '../api/types';

export type Status = 'booting' | 'signed_out' | 'authenticating' | 'active';

export interface SessionState {
  status: Status;
  token: string | null;
  expiresAt: number;
  me: WhoAmI | null;
  /** Why the user was signed out, for the login page. */
  reason: string | null;
  /** Shown two minutes before the idle limit. */
  idleWarning: boolean;
  lastActivity: number;
  allowInsecureHttp: boolean;
  buildVersion: string;
  /** An impersonation in this tab: who, by whom, until when. */
  impersonating: { user: string; by: string; until: number } | null;
}

export const useSession = create<SessionState>(() => ({
  status: 'booting',
  token: null,
  expiresAt: 0,
  me: null,
  reason: null,
  idleWarning: false,
  lastActivity: Date.now(),
  allowInsecureHttp: false,
  buildVersion: '',
  impersonating: null,
}));

const CHANNEL = 'sprig-studio-auth';
const SHARE_WINDOW_MS = 10_000;
let channel: BroadcastChannel | null = null;
let lastShared: { reply: TokenReply; at: number } | null = null;
let refreshTimer: ReturnType<typeof setTimeout> | null = null;
let idleTimer: ReturnType<typeof setInterval> | null = null;

function openChannel(): void {
  if (channel || typeof BroadcastChannel === 'undefined') return;
  channel = new BroadcastChannel(CHANNEL);
  channel.onmessage = (ev: MessageEvent<{ type: string; reply?: TokenReply; at?: number }>) => {
    const m = ev.data;
    if (m.type === 'token' && m.reply) {
      lastShared = { reply: m.reply, at: m.at ?? Date.now() };
      if (useSession.getState().status === 'active') applyToken(m.reply);
    } else if (m.type === 'logout') {
      signedOut('You signed out in another tab.', false);
    }
  };
}

export function isLoopback(host: string): boolean {
  return host === 'localhost' || host === '[::1]' || host === '::1' || /^127\./.test(host);
}

function applyToken(reply: TokenReply): void {
  const expiresAt = Date.now() + reply.expires_in * 1000;
  // A refreshed token is always one's own session's.
  useSession.setState({ token: reply.access_token, expiresAt, impersonating: null });
  if (refreshTimer) clearTimeout(refreshTimer);
  refreshTimer = setTimeout(() => {
    void refresh();
  }, Math.max(5_000, reply.expires_in * 800));
}

function signedOut(reason: string | null, broadcast: boolean): void {
  if (refreshTimer) clearTimeout(refreshTimer);
  if (idleTimer) clearInterval(idleTimer);
  refreshTimer = null;
  idleTimer = null;
  useSession.setState({ status: 'signed_out', token: null, me: null, reason, idleWarning: false, impersonating: null });
  if (broadcast) channel?.postMessage({ type: 'logout' });
  window.dispatchEvent(new CustomEvent('sprig-signed-out'));
}

async function postRefresh(): Promise<TokenReply | null> {
  const r = await fetch('/v1/auth/refresh', {
    method: 'POST',
    headers: { 'x-sprig-csrf': '1', 'x-sprig-client': CLIENT_HEADER },
    credentials: 'same-origin',
    cache: 'no-store',
  });
  if (!r.ok) {
    let msg = '';
    try {
      const body = (await r.json()) as { code?: number; message?: string };
      if (body.code === 39924) msg = 'Your session ended after a period of inactivity.';
      else if (body.code === 39925) msg = 'Your session reached its maximum length.';
      else if (body.code && body.code !== 1045) msg = body.message ?? '';
    } catch {
      // no body
    }
    lastRefreshError = msg || null;
    return null;
  }
  const reply = (await r.json()) as TokenReply;
  lastShared = { reply, at: Date.now() };
  channel?.postMessage({ type: 'token', reply, at: lastShared.at });
  return reply;
}

let lastRefreshError: string | null = null;
let inflight: Promise<string | null> | null = null;

/** One coordinated refresh across tabs. */
export function refresh(): Promise<string | null> {
  if (inflight) return inflight;
  const run = async (): Promise<string | null> => {
    if (lastShared && Date.now() - lastShared.at < SHARE_WINDOW_MS) {
      applyToken(lastShared.reply);
      return lastShared.reply.access_token;
    }
    const reply = await postRefresh();
    if (!reply) return null;
    applyToken(reply);
    return reply.access_token;
  };
  const locked = (): Promise<string | null> =>
    typeof navigator !== 'undefined' && 'locks' in navigator
      ? navigator.locks.request('sprig-refresh', run)
      : run();
  inflight = locked().finally(() => {
    inflight = null;
  });
  return inflight;
}

async function loadMe(): Promise<void> {
  const me = await json<WhoAmI>('/v1/auth/me');
  useSession.setState({ me, status: 'active', reason: null, lastActivity: Date.now() });
  startIdleWatch();
}

function installHooks(): void {
  openChannel();
  setAuthHooks({
    token: () => useSession.getState().token,
    refresh,
    ended: (e: ApiError) => signedOut(sessionEndReason(e), false),
  });
}

/** Boot: version check, then a silent refresh from the cookie. */
export async function boot(): Promise<void> {
  installHooks();
  try {
    const v = (await (await fetch('/studio/version.json', { cache: 'no-store' })).json()) as {
      build_version?: string;
      min_api?: string;
      allow_insecure_http?: boolean;
    };
    useSession.setState({
      allowInsecureHttp: v.allow_insecure_http === true,
      buildVersion: v.build_version ?? '',
    });
  } catch {
    // An old gateway without version.json: carry on.
  }
  const t = await refresh().catch(() => null);
  if (!t) {
    signedOut(null, false);
    return;
  }
  try {
    await loadMe();
  } catch (e) {
    signedOut(errorText(e), false);
  }
}

export function sessionEndReason(e: ApiError): string {
  if (e.code === 39924) return 'Your session ended after a period of inactivity.';
  if (e.code === 39925) return 'Your session reached its maximum length.';
  return 'Your session ended. Sign in again.';
}

/** Whether a password may be sent from this page. */
export function insecureLoginBlocked(loc: Location = window.location): boolean {
  return loc.protocol === 'http:' && !isLoopback(loc.hostname) && !useSession.getState().allowInsecureHttp;
}

/** What a password login needs next. */
export type LoginStep =
  | { kind: 'done' }
  | { kind: 'challenge'; challenge: string; expiresIn: number }
  | { kind: 'enrol'; ticket: string; expiresIn: number };

interface LoginReply extends Partial<TokenReply> {
  challenge?: string;
  enrol?: string;
  expires_in: number;
}

async function signedIn(reply: TokenReply): Promise<void> {
  lastShared = { reply, at: 0 };
  applyToken(reply);
  await loadMe();
}

export async function login(loginName: string, password: string): Promise<LoginStep> {
  if (insecureLoginBlocked()) {
    throw new Error(
      'Signing in over plain HTTP from another computer is turned off: use https (8443) or ask an operator to set studio.allow_insecure_http.',
    );
  }
  installHooks();
  useSession.setState({ status: 'authenticating' });
  try {
    const reply = await json<LoginReply>('/v1/auth/login', {
      body: { login: loginName, password, refresh_transport: 'cookie', client: 'studio' },
      headers: { 'x-sprig-csrf': '1' },
      auth: false,
    });
    // A second factor: a challenge, or an enrolment first.
    if (reply.challenge) {
      useSession.setState({ status: 'signed_out' });
      return { kind: 'challenge', challenge: reply.challenge, expiresIn: reply.expires_in };
    }
    if (reply.enrol) {
      useSession.setState({ status: 'signed_out' });
      return { kind: 'enrol', ticket: reply.enrol, expiresIn: reply.expires_in };
    }
    await signedIn(reply as TokenReply);
    return { kind: 'done' };
  } catch (e) {
    useSession.setState({ status: 'signed_out' });
    throw e;
  }
}

/** Completes a login with a TOTP code or a recovery code. */
export async function completeMfa(challenge: string, code: string): Promise<{ recoveryLeft?: number }> {
  useSession.setState({ status: 'authenticating' });
  try {
    const reply = await json<TokenReply & { recovery_left?: number }>('/v1/auth/mfa', {
      body: { challenge, code: code.trim(), refresh_transport: 'cookie' },
      headers: { 'x-sprig-csrf': '1' },
      auth: false,
    });
    await signedIn(reply);
    return { recoveryLeft: reply.recovery_left };
  } catch (e) {
    useSession.setState({ status: 'signed_out' });
    throw e;
  }
}

export interface Enrolment {
  secret: string;
  otpauth_uri: string;
}

/** Starts a TOTP enrolment: with an enrol ticket at login, else for the
 * signed-in user. */
export function startEnrolment(ticket?: string): Promise<Enrolment> {
  return json<Enrolment>('/v1/auth/factors:enrol', {
    body: ticket ? { enrol: ticket } : {},
    auth: !ticket,
  });
}

/** Confirms an enrolment with a code; returns the recovery codes (shown
 * once). With an enrol ticket it also signs in, once `finish` is called
 * (after the codes were shown). */
export async function confirmEnrolment(
  code: string,
  ticket?: string,
): Promise<{ codes: string[]; finish: () => Promise<void> }> {
  if (!ticket) {
    const r = await json<{ recovery_codes: string[] }>('/v1/auth/factors:confirm', { body: { code } });
    return { codes: r.recovery_codes, finish: () => Promise.resolve() };
  }
  const r = await json<TokenReply & { recovery_codes: string[] }>('/v1/auth/factors:confirm', {
    body: { code, enrol: ticket, refresh_transport: 'cookie' },
    headers: { 'x-sprig-csrf': '1' },
    auth: false,
  });
  return { codes: r.recovery_codes, finish: () => signedIn(r) };
}

/** Starts an impersonation in this tab: the tab's
 * token becomes the user's until it expires or "Stop impersonating". The
 * administrator's own session stays in its cookie. */
export async function impersonate(p: {
  tenant?: string;
  user: string;
  host?: string;
  reason: string;
  minutes: number;
}): Promise<void> {
  const r = await json<TokenReply & { impersonating: string; by: string }>('/v1/auth/impersonate', { body: p });
  if (refreshTimer) clearTimeout(refreshTimer);
  const until = Date.now() + r.expires_in * 1000;
  useSession.setState({ token: r.access_token, expiresAt: until, impersonating: { user: r.impersonating, by: r.by, until } });
  // No refresh: the impersonation ends with its token.
  refreshTimer = setTimeout(() => void stopImpersonating(), r.expires_in * 1000);
  await loadMe();
  window.dispatchEvent(new CustomEvent('sprig-identity-changed'));
}

/** Ends the impersonation session and returns to one's own. */
export async function stopImpersonating(): Promise<void> {
  const s = useSession.getState();
  if (!s.impersonating) return;
  try {
    // Without the cookie: only the impersonation session ends.
    await fetch('/v1/auth/logout', {
      method: 'POST',
      headers: { 'x-sprig-client': CLIENT_HEADER, ...(s.token ? { authorization: `Bearer ${s.token}` } : {}) },
      credentials: 'omit',
    });
  } catch {
    // It ends with its token anyway.
  }
  useSession.setState({ impersonating: null, token: null });
  lastShared = null;
  const t = await refresh();
  if (!t) {
    signedOut('Your session ended. Sign in again.', false);
    return;
  }
  await loadMe();
  window.dispatchEvent(new CustomEvent('sprig-identity-changed'));
}

export async function logout(reason: string | null = null): Promise<void> {
  const token = useSession.getState().token;
  try {
    await fetch('/v1/auth/logout', {
      method: 'POST',
      headers: {
        'x-sprig-csrf': '1',
        'x-sprig-client': CLIENT_HEADER,
        ...(token ? { authorization: `Bearer ${token}` } : {}),
      },
      credentials: 'same-origin',
    });
  } catch {
    // Signed out locally anyway.
  }
  lastShared = null;
  signedOut(reason, true);
}

/** User activity: keyboard, pointer. */
export function touch(): void {
  const s = useSession.getState();
  if (s.status !== 'active') return;
  useSession.setState({ lastActivity: Date.now(), idleWarning: false });
}

const WARN_BEFORE_MS = 2 * 60_000;

function startIdleWatch(): void {
  if (idleTimer) clearInterval(idleTimer);
  idleTimer = setInterval(checkIdle, 5_000);
}

export function checkIdle(now = Date.now()): void {
  const s = useSession.getState();
  if (s.status !== 'active' || !s.me) return;
  const limit = s.me.session.idle_minutes * 60_000;
  const idle = now - s.lastActivity;
  if (idle >= limit) {
    void logout('You were signed out after a period of inactivity.');
  } else if (idle >= limit - WARN_BEFORE_MS && !s.idleWarning) {
    useSession.setState({ idleWarning: true });
  }
}

/** "Stay signed in": activity, and a refresh so the server sees it too. */
export async function staySignedIn(): Promise<void> {
  touch();
  lastShared = null;
  await refresh();
}

export function lastRefreshProblem(): string | null {
  return lastRefreshError;
}

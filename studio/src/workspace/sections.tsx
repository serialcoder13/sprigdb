// The activity bar's sections (decision R1): today's pages regrouped into a
// few sections, each with the pages it holds. Routes are unchanged.

import type { ComponentType, SVGProps } from 'react';
import type { MessageId } from '../i18n/en';
import {
  Database,
  GitFork,
  Send,
  ServerCog,
  Settings,
  Shield,
  SquareFunction,
  SquareTerminal,
  Zap,
} from '../icons';

type Icon = ComponentType<SVGProps<SVGSVGElement> & { size?: number }>;

export interface SectionLink {
  label: MessageId;
  /** Path relative to the tenant base (`/t/<tenant>/`), or absolute when it starts with `/`. */
  to: string;
  end?: boolean;
  /** Shows the approvals count. */
  badge?: boolean;
}

export interface Section {
  id: string;
  label: MessageId;
  icon: Icon;
  /** Where the activity icon goes (relative to the tenant base, or absolute). */
  to: string;
  /** Pages this section holds, listed in its side panel. */
  links: SectionLink[];
  /** Whether a path relative to the tenant base belongs to this section. */
  owns: (rel: string) => boolean;
  /** At the bottom of the activity bar. */
  bottom?: boolean;
  operatorOnly?: boolean;
  testId?: string;
}

const starts = (rel: string, ...prefixes: string[]) => prefixes.some((p) => rel === p || rel.startsWith(`${p}/`));

export const SECTIONS: Section[] = [
  {
    id: 'explorer',
    label: 'nav.explorer',
    icon: Database,
    to: '',
    links: [{ label: 'nav.redis', to: 'redis' }],
    owns: (rel) => rel === '' || starts(rel, 'db', 'redis'),
  },
  {
    id: 'query',
    label: 'nav.query',
    icon: SquareTerminal,
    to: 'q/',
    links: [],
    owns: (rel) => starts(rel, 'q'),
  },
  {
    id: 'messaging',
    label: 'nav.messaging',
    icon: Send,
    to: 'messaging',
    links: [],
    owns: (rel) => starts(rel, 'messaging'),
  },
  {
    id: 'logic',
    label: 'nav.logic',
    icon: SquareFunction,
    to: 'logic',
    links: [],
    owns: (rel) => starts(rel, 'logic'),
  },
  {
    id: 'performance',
    label: 'nav.performance',
    icon: Zap,
    to: 'indexing',
    links: [
      { label: 'nav.indexing', to: 'indexing' },
      { label: 'nav.inbox', to: 'inbox', end: true, badge: true },
      { label: 'nav.policy', to: 'inbox/policy' },
      { label: 'nav.shards', to: 'shards' },
    ],
    owns: (rel) => starts(rel, 'indexing', 'inbox', 'shards'),
  },
  {
    id: 'inheritance',
    label: 'nav.inheritance',
    icon: GitFork,
    to: 'inheritance',
    links: [],
    owns: (rel) => starts(rel, 'inheritance'),
  },
  {
    id: 'access',
    label: 'nav.accessControl',
    icon: Shield,
    to: 'access/users',
    links: [
      { label: 'nav.access', to: 'access/users' },
      { label: 'nav.policies', to: 'policies' },
      { label: 'nav.security', to: 'security' },
    ],
    owns: (rel) => starts(rel, 'access', 'policies', 'security'),
  },
  {
    id: 'operator',
    label: 'nav.operator',
    icon: ServerCog,
    to: '/ops/clusters',
    links: [],
    owns: () => false,
    bottom: true,
    operatorOnly: true,
    testId: 'nav-operator',
  },
  {
    id: 'settings',
    label: 'nav.settings',
    icon: Settings,
    to: 'settings',
    links: [],
    owns: (rel) => starts(rel, 'settings'),
    bottom: true,
  },
];

/** The section a tenant-relative path belongs to (Explorer when none claims it). */
export function sectionOf(rel: string): Section {
  return SECTIONS.find((s) => s.owns(rel)) ?? SECTIONS[0]!;
}

/** Resolves a section or link target against the tenant base. */
export function resolve(base: string, to: string): string {
  return to.startsWith('/') ? to : to ? `${base}/${to}` : base;
}

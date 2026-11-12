// Studio's icons (the Studio and TUI redesign):
// Lucide (ISC) for common ideas, plus a few drawn for SprigDB on Lucide's
// 24-pixel grid with a 2-pixel round stroke, so the two sets look like one.
// Icons are decorative (aria-hidden); every icon-only control carries a label.

import type { SVGProps } from 'react';

export {
  Braces,
  ChevronDown,
  ChevronRight,
  Command,
  Database,
  Eye,
  GitFork,
  History,
  KeyRound,
  ListOrdered,
  ListTree,
  LogOut,
  Moon,
  Plus,
  RefreshCw,
  Search,
  Send,
  ServerCog,
  Settings,
  Share2,
  Shield,
  ShieldCheck,
  SquareFunction,
  SquareTerminal,
  Star,
  Sun,
  Table2,
  User,
  Users,
  X,
  Zap,
} from 'lucide-react';

type IconProps = SVGProps<SVGSVGElement> & { size?: number };

function Svg({ size = 24, children, ...rest }: IconProps & { children: React.ReactNode }) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      {...rest}
    >
      {children}
    </svg>
  );
}

/** An event stream: an append-only log flowing right. */
export function StreamIcon(p: IconProps) {
  return (
    <Svg {...p}>
      <rect x="3" y="6" width="4" height="12" rx="1" />
      <rect x="9" y="6" width="4" height="12" rx="1" />
      <path d="M15 12h6" />
      <path d="m18 9 3 3-3 3" />
    </Svg>
  );
}

/** A sink: data leaving SprigDB for another system. */
export function SinkIcon(p: IconProps) {
  return (
    <Svg {...p}>
      <ellipse cx="9" cy="6" rx="6" ry="2.5" />
      <path d="M3 6v8c0 1.4 2.7 2.5 6 2.5" />
      <path d="M15 6v4" />
      <path d="M13 17h8" />
      <path d="m18 14 3 3-3 3" />
    </Svg>
  );
}

/** An automatic index: an index with a spark. */
export function AutoIndexIcon(p: IconProps) {
  return (
    <Svg {...p}>
      <path d="M4 6h9" />
      <path d="M4 12h7" />
      <path d="M4 18h9" />
      <path d="m17 3-2 5h4l-2 5" />
    </Svg>
  );
}

/** A shard: one piece of a split table. */
export function ShardIcon(p: IconProps) {
  return (
    <Svg {...p}>
      <rect x="3" y="3" width="8" height="8" rx="1.5" />
      <rect x="13" y="3" width="8" height="8" rx="1.5" />
      <rect x="3" y="13" width="8" height="8" rx="1.5" />
      <path d="M13 17h8" strokeDasharray="2 2" />
      <path d="M17 13v8" strokeDasharray="2 2" />
    </Svg>
  );
}

/** The tenant tree: a parent tenant and its children. */
export function TenantTreeIcon(p: IconProps) {
  return (
    <Svg {...p}>
      <circle cx="12" cy="5" r="2.5" />
      <circle cx="5" cy="19" r="2.5" />
      <circle cx="19" cy="19" r="2.5" />
      <path d="M12 7.5V12" />
      <path d="M5 16.5V14h14v2.5" />
    </Svg>
  );
}

/** Data inherited from a parent tenant: an arrow coming down into a row. */
export function InheritedIcon(p: IconProps) {
  return (
    <Svg {...p}>
      <path d="M12 3v9" />
      <path d="m8 8 4 4 4-4" />
      <rect x="3" y="15" width="18" height="6" rx="1.5" />
    </Svg>
  );
}

/** A local override of inherited data: a row with a pencil mark. */
export function OverrideIcon(p: IconProps) {
  return (
    <Svg {...p}>
      <rect x="3" y="15" width="18" height="6" rx="1.5" />
      <path d="M14 4l4 4-6 6H8v-4z" />
    </Svg>
  );
}

/** The change feed: rows flowing out as events. */
export function ChangeFeedIcon(p: IconProps) {
  return (
    <Svg {...p}>
      <path d="M3 6h8" />
      <path d="M3 12h8" />
      <path d="M3 18h8" />
      <path d="M15 8c3 0 3 8 6 8" />
      <path d="M15 16c3 0 3-8 6-8" />
    </Svg>
  );
}

/** A data centre: a building of racks. */
export function DataCentreIcon(p: IconProps) {
  return (
    <Svg {...p}>
      <rect x="4" y="3" width="16" height="18" rx="2" />
      <path d="M8 7h8" />
      <path d="M8 11h8" />
      <path d="M8 15h8" />
      <path d="M12 19h.01" />
    </Svg>
  );
}

/** The sprout mark: SprigDB's own small leaf pair. */
export function SproutIcon(p: IconProps) {
  return (
    <Svg {...p}>
      <path d="M12 21v-9" />
      <path d="M12 12C12 8 9 5 4 5c0 4 3 7 8 7Z" />
      <path d="M12 12c0-3.5 2.5-6 7-6 0 3.5-2.5 6-7 6Z" />
    </Svg>
  );
}

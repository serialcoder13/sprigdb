// The SprigDB mascot (coral-gold colourway, decision R5): small exported
// copies in public/mascot at 48, 96, 220 and 440 px; the browser picks one.

export type Pose =
  | 'avatar'
  | 'waving'
  | 'confident'
  | 'idea'
  | 'question'
  | 'sleeping'
  | 'sad'
  | 'working'
  | 'celebrating';

const FILE: Record<Pose, string> = {
  avatar: '00-avatar',
  waving: '01-waving',
  confident: '02-confident',
  idea: '03-idea',
  question: '04-question',
  sleeping: '05-sleeping',
  sad: '06-sad',
  working: '07-working',
  celebrating: '08-celebrating',
};

const BASE = `${import.meta.env.BASE_URL}mascot/`;

/** `size` is the displayed size in CSS pixels; `label` makes it meaningful to screen readers. */
export function Mascot({ pose, size, label, className }: { pose: Pose; size: number; label?: string; className?: string }) {
  const f = FILE[pose];
  const small = size <= 48;
  const src = `${BASE}${f}-${small ? 48 : 220}.webp`;
  const srcSet = small ? `${BASE}${f}-48.webp 1x, ${BASE}${f}-96.webp 2x` : `${BASE}${f}-220.webp 1x, ${BASE}${f}-440.webp 2x`;
  return (
    <img
      className={className}
      src={src}
      srcSet={srcSet}
      width={size}
      height={size}
      alt={label ?? ''}
      aria-hidden={label ? undefined : true}
      decoding="async"
      draggable={false}
    />
  );
}

export const number = (value) => value == null ? '—' : value.toLocaleString('en-GB');
export const percent = (value) => value == null ? '—' : `${(value * 100).toFixed(1)}%`;
export const utc = (value) => value ? new Date(value).toISOString().replace('T', ' ').slice(0, 19) : 'Unavailable';
export const old = (value) => !value || Date.now() - Date.parse(value) > 300_000;
export function age(value) {
  if (!value) return 'Unavailable';
  const seconds = Math.floor((Date.now() - Date.parse(value)) / 1000);
  if (seconds < -5) return 'Clock ahead';
  if (seconds < 60) return 'Just now';
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

// This describes a producer report, never the current health of a physical machine.
export function producerState(session, unavailable = false) {
  if (!session) return { label: 'No producer report', tone: 'muted' };
  if (unavailable || ['stale', 'clock_skew'].includes(session.freshness)) return { label: 'Producer unknown', tone: 'warning' };
  if (session.state === 'failed') return { label: 'Session failed', tone: 'danger' };
  if (session.state === 'stopped') return { label: 'Session stopped', tone: 'muted' };
  return { label: session.state === 'stopping' ? 'Producer stopping' : 'Producer running', tone: session.state === 'stopping' ? 'warning' : 'good' };
}

export async function getJson(path, signal) {
  const response = await fetch(path, { signal, cache: 'no-store' });
  const body = await response.json();
  if (!response.ok) throw new Error(body.detail || 'Could not read inspection results');
  return body;
}

// The controls are labelled UTC, regardless of the browser's local timezone.
export const utcFilter = (value) => new Date(`${value}Z`).toISOString();

// Carry the detector with station links, including links opened after a reload.
export function stationHref(row, runId, window = null) {
  const params = new URLSearchParams({ run_id: runId, station: row.line_id || row.station,
    category: row.category, model_version: row.model_version, threshold_version: row.threshold_version });
  if (window) {
    params.set('window_from', window.window_start);
    params.set('window_to', window.window_end);
  }
  return `#${params}`;
}

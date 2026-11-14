export async function getJson(path, signal) {
  const response = await fetch(path, { signal, cache: 'no-store' });
  const body = await response.json();
  if (!response.ok) throw new Error(body.detail || 'Could not read inspection results');
  return body;
}

// The controls are labelled UTC, regardless of the browser's local timezone.
export const utcFilter = (value) => new Date(`${value}Z`).toISOString();

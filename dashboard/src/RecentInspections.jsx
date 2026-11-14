import React, { useEffect, useState } from 'react';
import { getJson, utcFilter } from './api.js';

const utc = (value) => value ? new Date(value).toISOString().replace('T', ' ').slice(0, 19) + ' UTC' : 'Unavailable';

export default function RecentInspections({ context, stationId, from, to, refresh }) {
  const [data, setData] = useState(null);
  const [error, setError] = useState('');
  const [cursor, setCursor] = useState(null);
  const [selectedId, setSelectedId] = useState(null);
  const [imageFailed, setImageFailed] = useState(false);
  const params = new URLSearchParams({ run_id: context.run_id, line_id: stationId, category: context.category,
    model_version: context.model_version, threshold_version: context.threshold_version, limit: '50' });
  if (from) params.set('from_time', utcFilter(from));
  if (to) params.set('to_time', utcFilter(to));
  if (cursor) Object.entries(cursor).forEach(([key, value]) => params.set(key, value));
  const query = params.toString();

  useEffect(() => {
    let active = true;
    let timer;
    let controller;
    async function poll() {
      controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 8000);
      try {
        const result = await getJson(`/api/inspections?${query}`, controller.signal);
        if (!active) return;
        setData(result);
        setSelectedId((current) => result.inspections.some((row) => row.inspection_id === current)
          ? current : result.inspections[0]?.inspection_id || null);
        setError('');
      } catch (failure) {
        if (active) setError(failure.name === 'AbortError' ? 'Item refresh timed out' : failure.message);
      } finally {
        clearTimeout(timeout);
        if (active) timer = setTimeout(poll, 5000);
      }
    }
    setData(null);
    setError('');
    poll();
    return () => { active = false; clearTimeout(timer); controller?.abort(); };
  }, [query, refresh]);

  const selected = data?.inspections.find((row) => row.inspection_id === selectedId);
  useEffect(() => setImageFailed(false), [selectedId, selected?.image_url, refresh]);

  return <section>
    <h3>Recent inspections and photos</h3>
    {error && <p className="warning" role="alert">{error}. {data && 'Showing the last successful item response.'}</p>}
    {!data && !error && <p role="status">Loading individual inspections…</p>}
    {data?.state === 'missing' && <p>No individual inspection history is available yet.</p>}
    {data && data.state !== 'missing' && <>
      <p className="freshness">Item projection batch: {utc(data.projection_batch_at)} · Item response: {utc(data.response_time)}</p>
      <p>Retained samples matching this selection: {data.retained_count}. This is not the production total.</p>
      {data.latest_inspection && <p>Latest observed item in this run: {data.latest_inspection.product_id} at {utc(data.latest_inspection.event_time)}.</p>}
      {!data.inspections.length ? <p>No retained inspections match this page or time range.</p> : <div className="inspection-layout">
        <div className="table-scroll inspection-list"><table>
          <thead><tr><th>Time</th><th>Inspection</th><th>Prediction</th><th>Details</th></tr></thead>
          <tbody>{data.inspections.map((row) => <tr key={row.inspection_id}>
            <td>{utc(row.event_time)}</td><td>{row.inspection_id}</td><td>{row.is_reject ? 'Rejected' : 'Accepted'}</td>
            <td><button aria-label={`View ${row.inspection_id}`} aria-pressed={row.inspection_id === selectedId}
              onClick={() => setSelectedId(row.inspection_id)}>View</button></td>
          </tr>)}</tbody>
        </table></div>
        {selected && <aside className="inspection-detail">
          <h4>Selected inspection</h4>
          <dl>
            <dt>Inspection ID</dt><dd>{selected.inspection_id}</dd>
            <dt>Product</dt><dd>{selected.product_id}</dd>
            <dt>Category / station</dt><dd>{selected.category} / {selected.line_id}</dd>
            <dt>Image ID</dt><dd>{selected.image_id}</dd>
            <dt>Anomaly score</dt><dd>{selected.anomaly_score.toFixed(3)}</dd>
            <dt>Prediction</dt><dd>{selected.is_reject ? 'Predicted reject' : 'Predicted accept'}</dd>
            <dt>Model / threshold version</dt><dd>{selected.model_version} / {selected.threshold_version}</dd>
          </dl>
          {!selected.image_url ? <p>Image storage is not connected.</p> : imageFailed
            ? <p>Image unavailable for this inspection.</p>
            : <img src={selected.image_url} alt={`Inspection ${selected.inspection_id}`} onError={() => setImageFailed(true)} />}
        </aside>}
      </div>}
      <div className="item-pages">
        <button disabled={!cursor} onClick={() => setCursor(null)}>Latest items</button>
        <button disabled={!data.next_cursor} onClick={() => setCursor(data.next_cursor)}>Older items</button>
      </div>
      <small>Items and quality windows update independently. The latest observation is not a machine heartbeat; time filters select individual items within [from, to).</small>
    </>}
  </section>;
}

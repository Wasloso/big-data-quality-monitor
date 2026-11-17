import React, { useEffect, useState } from 'react';
import { getJson, utcFilter } from './api.js';
import StationObservation from './StationObservation.jsx';
import { number, utc } from './display.js';

export default function RecentInspections({ context, stationId, from, to, refresh, threshold }) {
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

  return <section className="panel inspections-panel">
    <div className="panel-heading"><div><span className="eyebrow">Inspection evidence</span><h2>Recent inspections & photos</h2></div><span className="tag">{data ? `${number(data.retained_count)} retained` : 'Awaiting source'}</span></div>
    {error && <p className="notice warning" role="alert">{error}. {data && 'Showing the last successful item snapshot.'}</p>}
    {!data && !error && <p className="empty-state" role="status">Loading individual inspections…</p>}
    {data?.state === 'missing' && <p className="empty-state">No individual inspection history is available yet.</p>}
    {data && data.state !== 'missing' && <>
      <div className="inspection-source"><span>Retained samples matching this selection: <strong>{number(data.retained_count)}</strong></span><span>Projection batch <span className="mono">{utc(data.projection_batch_at)} UTC</span></span></div>
      {!data.inspections.length ? <p className="empty-state">No retained inspections match this page or time range.</p> : <div className="inspection-layout">
        <div className="table-scroll inspection-list"><table>
          <thead><tr><th>Acquired · UTC</th><th>Inspection</th><th className="numeric">Score</th><th>Prediction</th><th><span className="sr-only">Details</span></th></tr></thead>
          <tbody>{data.inspections.map((row) => <tr key={row.inspection_id} className={row.inspection_id === selectedId ? 'selected-row' : ''}>
            <td className="mono">{utc(row.event_time).slice(11)}<small className="cell-sub">{utc(row.event_time).slice(0, 10)}</small></td>
            <td className="mono" title={row.inspection_id}>{row.inspection_id.length > 24 ? `…${row.inspection_id.slice(-20)}` : row.inspection_id}</td>
            <td className="numeric">{row.anomaly_score.toFixed(3)}</td>
            <td><span className={`badge ${row.is_reject ? 'danger' : 'good'}`}>{row.is_reject ? 'Reject' : 'Accept'}</span></td>
            <td><button className="row-button" aria-label={`View ${row.inspection_id}`} aria-pressed={row.inspection_id === selectedId} onClick={() => setSelectedId(row.inspection_id)}>View <span aria-hidden="true">↗</span></button></td>
          </tr>)}</tbody>
        </table></div>
        {selected && <aside className="inspection-detail">
          <div className="image-heading"><span className="section-label">Source image</span><span className={`badge ${selected.is_reject ? 'danger' : 'good'}`}>{selected.is_reject ? 'Predicted reject' : 'Predicted accept'}</span></div>
          <div className="image-stage">
            {!selected.image_url ? <div className="image-placeholder"><span>⊡</span>Image storage is not connected.</div> : imageFailed
              ? <div className="image-placeholder"><span>⊡</span>Image unavailable for this inspection.</div>
              : <img src={selected.image_url} alt={`Inspection ${selected.inspection_id}`} onError={() => setImageFailed(true)} />}
          </div>
          <div className="prediction-readout"><div><span>Anomaly score</span><strong>{selected.anomaly_score.toFixed(3)}</strong></div><div><span>Reject threshold</span><strong>{threshold == null ? '—' : `> ${threshold.toFixed(3)}`}</strong></div></div>
          <p className="score-caption">Model score, not a probability. Threshold is from the matching producer report.</p>
          <div className="inspection-meta"><span className="category">{selected.category} / {selected.line_id}</span><span className="mono">{utc(selected.event_time)} UTC</span></div>
          <StationObservation item={selected} label="Context recorded with this inspection" />
          <details className="detector-details"><summary>Inspection traceability</summary><dl className="trace-list">
            <dt>Inspection ID</dt><dd>{selected.inspection_id}</dd><dt>Product ID</dt><dd>{selected.product_id}</dd>
            <dt>Image ID</dt><dd>{selected.image_id}</dd><dt>Model version</dt><dd>{selected.model_version}</dd><dt>Threshold version</dt><dd>{selected.threshold_version}</dd>
          </dl></details>
        </aside>}
      </div>}
      <div className="inspection-pagination"><div className="item-pages"><button disabled={!cursor} onClick={() => setCursor(null)}>Latest items</button><button disabled={!data.next_cursor} onClick={() => setCursor(data.next_cursor)}>Older items →</button></div><span className="subtle">Up to 50 retained records per page</span></div>
      {data.latest_inspection && <p className="panel-footnote">Latest observed item in this run: <span className="mono">{data.latest_inspection.inspection_id}</span> · {utc(data.latest_inspection.event_time)} UTC</p>}
      <div className="panel-footnote"><span>Retained samples ≠ production total · Item filters use [from, to) · Latest observation is independent of historical filters</span><span>Response {utc(data.response_time)} UTC</span></div>
    </>}
  </section>;
}

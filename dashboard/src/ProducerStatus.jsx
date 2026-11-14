import React, { useEffect, useState } from 'react';
import { getJson } from './api.js';

const utc = (value) => new Date(value).toISOString().replace('T', ' ').slice(0, 19) + ' UTC';

export default function ProducerStatus({ context, stationId, refresh }) {
  const [data, setData] = useState(null);
  const [error, setError] = useState('');
  const params = new URLSearchParams({ run_id: context.run_id, category: context.category,
    model_version: context.model_version, threshold_version: context.threshold_version });
  if (stationId) params.set('line_id', stationId);
  const query = params.toString();

  useEffect(() => { setData(null); setError(''); }, [query]);

  useEffect(() => {
    let active = true;
    let timer;
    let controller;
    async function poll() {
      controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 8000);
      try {
        const result = await getJson(`/api/producer-status?${query}`, controller.signal);
        if (active) { setData(result); setError(''); }
      } catch (failure) {
        if (active) setError(failure.name === 'AbortError' ? 'Producer-status refresh timed out' : failure.message);
      } finally {
        clearTimeout(timeout);
        if (active) timer = setTimeout(poll, 5000);
      }
    }
    poll();
    return () => { active = false; clearTimeout(timer); controller?.abort(); };
  }, [query, refresh]);

  return <section className="producer-status">
    <h3>Producer sessions</h3>
    <p>Session counters cover this run and detector, independently of window time filters. Kafka acknowledgements are not Spark processed counts.</p>
    {error && <p className="warning" role="alert">{error}. {data && 'Showing the last successful status response.'}</p>}
    {!data && !error && <p>Loading producer reports…</p>}
    {data?.state === 'disabled' && <p>Producer status is not connected.</p>}
    {data?.state === 'missing' && <p>Producer-status storage is unavailable.</p>}
    {data?.state === 'partial' && <p className="warning">Some status files could not be read. The session list and latest-session markers may be incomplete.</p>}
    {data && ['ready', 'partial', 'empty'].includes(data.state) && <>
      <p className="freshness">Status response: {utc(data.response_time)} · Refresh every 5 seconds</p>
      {!data.sessions.length ? <p>No readable producer sessions match this selection.</p> : <div className="table-scroll"><table>
        <thead><tr><th>Station / session</th><th>Producer report</th><th>Attempts / predictions / queued</th><th>Kafka acknowledged</th><th>Errors / pending</th></tr></thead>
        <tbody>{data.sessions.map((session) => {
          const unknown = error || ['stale', 'clock_skew'].includes(session.freshness);
          const hasErrors = session.inference_errors + session.enqueue_errors + session.delivery_errors > 0 || session.pending > 0;
          return <tr key={session.session_id}>
            <td><strong>{session.station}</strong> · {session.session_id.slice(0, 8)}
              <p>{session.latest_started_session ? 'Latest started session in this run' : 'Earlier session in this run'}</p>
              <small>Started {utc(session.started_at)}</small></td>
            <td><span className={`badge ${unknown || hasErrors || session.state === 'failed' ? 'stale' : 'clear'}`}>
              {unknown ? 'Unknown — stale or unavailable report' : `Reported ${session.state}`}</span>
              <p>Recorded: {session.state} at {utc(session.updated_at)}</p>
              {session.freshness === 'ended' && <p>Ended session; this does not describe another process.</p>}
              <p>Detector decision: score &gt; {session.threshold.toFixed(4)}</p>
              {session.last_error && <details><summary>Last reported error</summary><p>{session.last_error}</p></details>}</td>
            <td>{session.attempted} / {session.completed} / {session.queued}</td><td>{session.published}</td>
            <td>Inference: {session.inference_errors}<br />Enqueue: {session.enqueue_errors}<br />Delivery: {session.delivery_errors}<br />Pending: {session.pending}</td>
          </tr>;
        })}</tbody>
      </table></div>}
      <small>Reports update with inspection attempts. A missing or old active report leaves current process state unknown. Ended reports describe that session only.</small>
    </>}
  </section>;
}

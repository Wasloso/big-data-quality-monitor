import React, { useEffect, useState } from 'react';
import { getJson, stationHref } from './api.js';
import { age, number, producerState, utc } from './display.js';

export function useProducerStatus(context, stationId, refresh) {
  const [snapshot, setSnapshot] = useState({});
  const params = new URLSearchParams();
  if (context) {
    params.set('run_id', context.run_id);
    if (context.category) {
      params.set('category', context.category);
      params.set('model_version', context.model_version);
      params.set('threshold_version', context.threshold_version);
    }
    if (stationId) params.set('line_id', stationId);
  }
  const query = params.toString();
  useEffect(() => {
    if (!query) return;
    let active = true;
    let timer;
    let controller;
    async function poll() {
      controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 8000);
      try {
        const data = await getJson(`/api/producer-status?${query}`, controller.signal);
        if (active) setSnapshot({ query, data, error: '' });
      } catch (failure) {
        if (active) setSnapshot((previous) => ({ query, data: previous.query === query ? previous.data : null,
          error: failure.name === 'AbortError' ? 'Producer-status refresh timed out' : failure.message }));
      } finally {
        clearTimeout(timeout);
        if (active) timer = setTimeout(poll, 5000);
      }
    }
    poll();
    return () => { active = false; clearTimeout(timer); controller?.abort(); };
  }, [query, refresh]);
  return snapshot.query === query ? snapshot : { data: null, error: '' };
}

export default function ProducerStatus({ context, report }) {
  const { data, error } = report;
  return <section className="panel producer-status">
    <div className="panel-heading"><div><span className="eyebrow">Acquisition & delivery</span><h2>Producer sessions</h2></div>
      <span className="tag">{data ? `${data.sessions.length} sessions` : 'Awaiting source'}</span></div>
    <p className="panel-description">Session counters ignore window filters. Kafka acknowledgements are separate from Spark-processed inspections.</p>
    {error && <p className="notice warning" role="alert">{error}. {data && 'Previous reports retained; current producer state unknown.'}</p>}
    {!data && !error && <p className="empty-state" role="status">Loading producer reports…</p>}
    {data?.state === 'disabled' && <p className="empty-state">Producer status is not connected.</p>}
    {data?.state === 'missing' && <p className="empty-state">Producer-status storage is unavailable.</p>}
    {data?.state === 'partial' && <p className="notice warning">Some reports could not be read. Session ordering and latest-session markers may be incomplete.</p>}
    {data && ['ready', 'partial', 'empty'].includes(data.state) && <>
      {!data.sessions.length ? <p className="empty-state">No producer sessions match this selection.</p> : <div className="table-scroll session-table"><table>
        <thead><tr><th>Station / session</th><th>Producer report</th><th className="numeric">Attempts</th><th className="numeric">Predictions</th><th className="numeric">Queued</th><th className="numeric">Kafka ack.</th><th>Errors · I / E / D</th><th className="numeric">Pending</th><th>Detector / target</th></tr></thead>
        <tbody>{data.sessions.map((session) => {
          const state = producerState(session, error || data.state === 'partial');
          const errors = session.inference_errors + session.enqueue_errors + session.delivery_errors;
          return <tr key={session.session_id}>
            <td><a className="station-link" href={stationHref(session, context.run_id)}>Station {session.station}<span className="arrow">↗</span></a>
              <small className="cell-sub mono" title={session.session_id}>{session.session_id.slice(0, 8)} · {session.latest_started_session ? 'Latest started' : 'Earlier session'}</small></td>
            <td><span className={`badge ${state.tone}`}><i />{state.label}</span><small className="cell-sub" title={`${utc(session.updated_at)} UTC`}>Reported {age(session.updated_at)}</small></td>
            <td className="numeric">{number(session.attempted)}</td><td className="numeric">{number(session.completed)}</td>
            <td className="numeric">{number(session.queued)}</td><td className="numeric">{number(session.published)}</td>
            <td className={errors ? 'text-danger mono' : 'mono'}>{session.inference_errors} / {session.enqueue_errors} / {session.delivery_errors}
              {session.last_error && <details className="inline-details"><summary>Last error</summary><p>{session.last_error}</p></details>}</td>
            <td className={`numeric ${session.pending ? 'text-warning' : ''}`}>{number(session.pending)}</td>
            <td><strong className="category">{session.category}</strong> · score &gt; {session.threshold.toFixed(4)}
              <small className="cell-sub">Target {session.target_rate}/s · {session.model_version}</small>
              <details className="inline-details"><summary>Session provenance</summary><dl className="trace-list">
                <dt>Threshold version</dt><dd>{session.threshold_version}</dd><dt>Started · UTC</dt><dd>{utc(session.started_at)}</dd>
                <dt>Updated · UTC</dt><dd>{utc(session.updated_at)}</dd>{session.stopped_at && <><dt>Ended · UTC</dt><dd>{utc(session.stopped_at)}</dd></>}
              </dl></details></td>
          </tr>;
        })}</tbody>
      </table></div>}
      <div className="panel-footnote"><span>I: inference · E: enqueue · D: delivery</span><span>Source response {utc(data.response_time)} UTC</span></div>
    </>}
    <p className="panel-footnote">A stopped report describes that session only. Old or unavailable active reports leave the current process state unknown.</p>
  </section>;
}

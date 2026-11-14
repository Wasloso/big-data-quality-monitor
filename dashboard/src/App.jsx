import React, { useEffect, useState } from 'react';
import { getJson, utcFilter, stationHref } from './api.js';
import RecentInspections from './RecentInspections.jsx';
import StationObservation from './StationObservation.jsx';
import ProducerStatus from './ProducerStatus.jsx';

const percent = (value) => value == null ? '—' : `${(value * 100).toFixed(1)}%`;
const utc = (value) => value ? new Date(value).toISOString().replace('T', ' ').slice(0, 19) + ' UTC' : 'Unavailable';
const stale = (value) => !value || Date.now() - Date.parse(value) > 300_000;

function Counts({ stations }) {
  const inspections = stations.reduce((sum, row) => sum + row.inspections, 0);
  const rejects = stations.reduce((sum, row) => sum + row.rejects, 0);
  return <div className="counts">
    <div><strong>{inspections}</strong><span>Inspections</span></div>
    <div><strong>{inspections - rejects}</strong><span>Predicted accepted</span></div>
    <div><strong>{rejects}</strong><span>Predicted rejects</span></div>
    <div><strong>{percent(inspections ? rejects / inspections : null)}</strong><span>Reject rate</span></div>
  </div>;
}

function Windows({ rows }) {
  return <div className="table-scroll"><table>
    <thead><tr><th>Window start</th><th>Station</th><th>Inspections</th><th>Rejects</th><th>Rate</th><th>State</th></tr></thead>
    <tbody>{rows.map((row) => <tr key={row.window_key}>
      <td>{utc(row.window_start)}</td><td>{row.line_id}</td><td>{row.inspections}</td><td>{row.rejects}</td>
      <td><meter min="0" max="1" value={row.reject_rate} /> {percent(row.reject_rate)}</td>
      <td>{row.is_final ? 'Final' : 'Provisional'}</td>
    </tr>)}</tbody>
  </table></div>;
}

function Rules({ rows, windows, runId }) {
  const byKey = Object.fromEntries(windows.map((row) => [row.window_key, row]));
  if (!rows.length) return <p>No matching rule results.</p>;
  return <div className="table-scroll"><table>
    <thead><tr><th>Station</th><th>Window start</th><th>Status</th><th>Rate threshold</th><th>Minimum count</th><th>State</th></tr></thead>
    <tbody>{rows.map((row) => <tr key={row.alert_id}>
      <td><a href={stationHref(byKey[row.window_key], runId, byKey[row.window_key])}>{byKey[row.window_key].line_id}</a></td>
      <td>{utc(byKey[row.window_key].window_start)}</td><td><span className={`badge ${row.status}`}>{row.status.replace('_', ' ')}</span></td>
      <td>&gt; {percent(row.threshold)}</td><td>{row.minimum_inspections}</td><td>{row.is_provisional ? 'Provisional' : 'Final'}</td>
    </tr>)}</tbody>
  </table></div>;
}

export default function App() {
  const [runs, setRuns] = useState([]);
  const [selected, setSelected] = useState(() => new URLSearchParams(window.location.hash.slice(1)).get('run_id') || '');
  const [data, setData] = useState(null);
  const [source, setSource] = useState('');
  const [truncated, setTruncated] = useState(false);
  const [statusDiscovery, setStatusDiscovery] = useState('disabled');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const [refresh, setRefresh] = useState(0);
  const [hash, setHash] = useState(window.location.hash);
  const route = new URLSearchParams(hash.slice(1));
  const stationId = route.get('station');
  const summary = route.get('view') === 'summary';
  const routeRun = route.get('run_id');
  const category = route.get('category');
  const model = route.get('model_version');
  const threshold = route.get('threshold_version');
  const windowFrom = route.get('window_from');
  const windowTo = route.get('window_to');
  const hasDetector = category && model && threshold;
  const stations = data?.stations || [];
  const detector = hasDetector ? { category, model_version: model, threshold_version: threshold }
    : stations.find((station) => station.line_id === stationId);
  const context = selected ? { run_id: selected, ...(stationId && detector ? {
    category: detector.category, model_version: detector.model_version, threshold_version: detector.threshold_version,
  } : {}) } : null;
  const runIds = [...new Set(runs.map((run) => run.run_id))];
  const navigation = (view) => `#${new URLSearchParams({ view, run_id: selected })}`;

  useEffect(() => {
    if (routeRun) setSelected(routeRun);
    if (windowFrom && windowTo && Number.isFinite(Date.parse(windowFrom)) && Number.isFinite(Date.parse(windowTo))) {
      setFrom(new Date(windowFrom).toISOString().slice(0, 19));
      setTo(new Date(windowTo).toISOString().slice(0, 19));
    }
  }, [routeRun, windowFrom, windowTo]);

  useEffect(() => {
    const navigate = () => setHash(window.location.hash);
    window.addEventListener('hashchange', navigate);
    return () => window.removeEventListener('hashchange', navigate);
  }, []);

  useEffect(() => {
    let active = true;
    let timer;
    let controller;
    async function poll() {
      controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 8000);
      try {
        const listing = await getJson('/api/runs', controller.signal);
        if (!active) return;
        setRuns(listing.runs);
        setSource(listing.state);
        setTruncated(listing.truncated);
        setStatusDiscovery(listing.producer_status_state);
        if (!listing.runs.length) {
          setData(null);
          setError('');
          return;
        }
        const current = listing.runs.find((run) => run.run_id === selected);
        if (!current) {
          if (!selected) setSelected(listing.runs[0].run_id);
          else throw new Error('Selected run is unavailable in the bounded run listing');
          return;
        }
        const params = new URLSearchParams({ run_id: current.run_id });
        if (stationId && hasDetector) {
          params.set('category', category);
          params.set('model_version', model);
          params.set('threshold_version', threshold);
        }
        if (stationId) params.set('line_id', stationId);
        if (from) params.set('from_time', utcFilter(from));
        if (to) params.set('to_time', utcFilter(to));
        const result = await getJson(`/api/overview?${params}`, controller.signal);
        if (active) { setData(result); setError(''); }
      } catch (failure) {
        if (active) setError(failure.name === 'AbortError' ? 'Refresh timed out' : failure.message);
      } finally {
        clearTimeout(timeout);
        if (active) { setLoading(false); timer = setTimeout(poll, 5000); }
      }
    }
    setData(null);
    setError('');
    setLoading(true);
    poll();
    return () => { active = false; clearTimeout(timer); controller?.abort(); };
  }, [selected, from, to, stationId, category, model, threshold, refresh]);

  const windows = data?.windows || [];
  const alerts = data?.alerts || [];
  const activeAlerts = alerts.filter((row) => row.status === 'active');

  return <main>
    <header><div><h1>Quality monitor</h1><p>Inspection results from simulated production stations</p></div>
      <button onClick={() => setRefresh((value) => value + 1)}>Refresh</button></header>
    <nav><a href={navigation('stations')} aria-current={!stationId && !summary ? 'page' : undefined}>Stations</a>
      <a href={navigation('summary')} aria-current={!stationId && summary ? 'page' : undefined}>Summary &amp; alerts</a></nav>
    <section className="filters">
      <label>Run<select value={selected} onChange={(event) => {
        setSelected(event.target.value);
        window.location.hash = new URLSearchParams({ view: 'stations', run_id: event.target.value }).toString();
      }} disabled={!runs.length}>
        {!runs.length && <option value="">No runs available</option>}
        {selected && !runIds.includes(selected) && <option value={selected}>{selected} (unavailable)</option>}
        {runIds.map((runId) => <option key={runId} value={runId}>{runId}</option>)}
      </select></label>
      <label>From (UTC)<input type="datetime-local" step="1" value={from} onChange={(event) => setFrom(event.target.value)} /></label>
      <label>To (UTC)<input type="datetime-local" step="1" value={to} onChange={(event) => setTo(event.target.value)} /></label>
    </section>
    {truncated && <p className="notice">Showing the 100 most recently updated run/detector combinations.</p>}
    {['partial', 'unavailable'].includes(statusDiscovery) && <p className="warning">Producer run discovery is incomplete. Stored quality runs remain listed.</p>}
    {error && <p className="warning" role="alert">{error}. {data && 'Showing the last successful response.'}</p>}
    {loading && <p role="status">Loading results…</p>}
    {!loading && !error && !runs.length && <p className="notice">{source === 'missing' ? 'No serving database yet.' : 'No inspection results yet.'}</p>}
    {stationId && context && <h2>Station {stationId}</h2>}
    {data && <>
      <p className="freshness">Response: {utc(data.response_time)} · Last stored update: {utc(data.updated_at)} · Refresh every 5 seconds</p>
      {data.updated_at && stale(data.updated_at) && <p className="warning">No recent stored results. The source may be idle or processing may have stopped.</p>}
      {stations.some((station) => station.model_version.startsWith('fixture')) && <p className="notice">Fixture mode: predictions and image references are synthetic.</p>}
      {data.observation_state === 'unavailable' && <p className="warning">Station observations are unavailable. Quality-window results remain available.</p>}
      <p>Counts cover selected stored windows. Provisional results can change; predicted accepts are not confirmed product passes.</p>
      {!stations.length ? <p className="notice">No stored windows match this station or time range.</p> : stationId ? <>
        <Counts stations={stations} />
        <section><h3>Latest station context</h3><p>Model: {context?.model_version} · Category: {context?.category} · Threshold version: {context?.threshold_version}</p>
          <StationObservation item={stations[0]?.latest_observation} /></section>
        <section><h3>Window history</h3><Windows rows={windows} /></section>
        <section><h3>Rule evaluations</h3><Rules rows={alerts} windows={windows} runId={selected} /></section>
      </> : summary ? <>
        <h2>All-stations summary</h2><Counts stations={stations} />
        <section><h3>Active quality alerts ({activeAlerts.length})</h3><Rules rows={activeAlerts} windows={windows} runId={selected} /></section>
        <section><h3>Window history</h3><Windows rows={windows} /></section>
        <details><summary>All rule evaluations ({alerts.length})</summary><Rules rows={alerts} windows={windows} runId={selected} /></details>
      </> : <>
        <h2>Stations</h2><div className="stations">{stations.map((station) => <section key={station.line_id}>
          <h3><a href={stationHref(station, selected)}>Station {station.line_id}</a></h3>
          <p><span className={`badge ${stale(station.updated_at) ? 'stale' : 'clear'}`}>{stale(station.updated_at) ? 'Stale results' : 'Recent results'}</span></p>
          <Counts stations={[station]} /><p>Category: {station.category} · Model: {station.model_version} · Threshold: {station.threshold_version}</p>
          <StationObservation item={station.latest_observation} /><small>Last stored update: {utc(station.updated_at)}</small>
        </section>)}</div>
      </>}
    </>}
    {context && <ProducerStatus key={`${selected}|${stationId || 'all'}`} context={context} stationId={stationId} refresh={refresh} />}
    {stationId && windowFrom && windowTo && <p className="notice">Rule links select the window’s retained photo examples. These are not evidence of what caused the alert.</p>}
    {stationId && context?.category && <RecentInspections key={`${selected}|${stationId}|${context.category}|${context.model_version}|${context.threshold_version}|${from}|${to}`}
      context={context} stationId={stationId} from={from} to={to} refresh={refresh} />}
  </main>;
}

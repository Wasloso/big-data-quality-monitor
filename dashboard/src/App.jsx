import React, { useEffect, useState } from 'react';

const runKey = (run) => JSON.stringify([run.run_id, run.category, run.model_version, run.threshold_version]);
const percent = (value) => value == null ? '—' : `${(value * 100).toFixed(1)}%`;
const utc = (value) => value ? new Date(value).toISOString().replace('T', ' ').slice(0, 19) + ' UTC' : 'Unavailable';
const stale = (value) => !value || Date.now() - Date.parse(value) > 300_000;

async function getJson(path, signal) {
  const response = await fetch(path, { signal, cache: 'no-store' });
  const body = await response.json();
  if (!response.ok) throw new Error(body.detail || 'Could not read quality results');
  return body;
}

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

function Rules({ rows, windows }) {
  const byKey = Object.fromEntries(windows.map((row) => [row.window_key, row]));
  if (!rows.length) return <p>No matching rule results.</p>;
  return <div className="table-scroll"><table>
    <thead><tr><th>Station</th><th>Window start</th><th>Status</th><th>Rate threshold</th><th>Minimum count</th><th>State</th></tr></thead>
    <tbody>{rows.map((row) => <tr key={row.alert_id}>
      <td><a href={`#station=${encodeURIComponent(byKey[row.window_key].line_id)}`}>{byKey[row.window_key].line_id}</a></td>
      <td>{utc(byKey[row.window_key].window_start)}</td><td><span className={`badge ${row.status}`}>{row.status.replace('_', ' ')}</span></td>
      <td>&gt; {percent(row.threshold)}</td><td>{row.minimum_inspections}</td><td>{row.is_provisional ? 'Provisional' : 'Final'}</td>
    </tr>)}</tbody>
  </table></div>;
}

export default function App() {
  const [runs, setRuns] = useState([]);
  const [selected, setSelected] = useState('');
  const [data, setData] = useState(null);
  const [source, setSource] = useState('');
  const [truncated, setTruncated] = useState(false);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const [refresh, setRefresh] = useState(0);
  const [hash, setHash] = useState(window.location.hash);
  const route = new URLSearchParams(hash.slice(1));
  const stationId = route.get('station');
  const summary = route.get('view') === 'summary';
  const context = runs.find((run) => runKey(run) === selected);

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
        if (!listing.runs.length) {
          setData(null);
          setError('');
          return;
        }
        const current = listing.runs.find((run) => runKey(run) === selected);
        if (!current) {
          setSelected(runKey(listing.runs[0]));
          return;
        }
        const params = new URLSearchParams({ run_id: current.run_id, category: current.category,
          model_version: current.model_version, threshold_version: current.threshold_version });
        if (stationId) params.set('line_id', stationId);
        if (from) params.set('from_time', `${from}:00+00:00`);
        if (to) params.set('to_time', `${to}:00+00:00`);
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
  }, [selected, from, to, stationId, refresh]);

  const stations = data?.stations || [];
  const windows = data?.windows || [];
  const alerts = data?.alerts || [];
  const activeAlerts = alerts.filter((row) => row.status === 'active');

  return <main>
    <header><div><h1>Quality monitor</h1><p>Inspection results from simulated production stations</p></div>
      <button onClick={() => setRefresh((value) => value + 1)}>Refresh</button></header>
    <nav><a href="#view=stations" aria-current={!stationId && !summary ? 'page' : undefined}>Stations</a>
      <a href="#view=summary" aria-current={!stationId && summary ? 'page' : undefined}>Summary &amp; alerts</a></nav>
    <section className="filters">
      <label>Run / detector<select value={selected} onChange={(event) => setSelected(event.target.value)} disabled={!runs.length}>
        {!runs.length && <option value="">No runs available</option>}
        {runs.map((run) => <option key={runKey(run)} value={runKey(run)}>{run.run_id} · {run.category} · {run.model_version} · {run.threshold_version}</option>)}
      </select></label>
      <label>From (UTC)<input type="datetime-local" value={from} onChange={(event) => setFrom(event.target.value)} /></label>
      <label>To (UTC)<input type="datetime-local" value={to} onChange={(event) => setTo(event.target.value)} /></label>
    </section>
    {truncated && <p className="notice">Showing the 100 most recently updated run/detector combinations.</p>}
    {error && <p className="warning" role="alert">{error}. {data && 'Showing the last successful response.'}</p>}
    {loading && <p role="status">Loading results…</p>}
    {!loading && !error && !runs.length && <p className="notice">{source === 'missing' ? 'No serving database yet.' : 'No inspection results yet.'}</p>}
    {data && <>
      <p className="freshness">Response: {utc(data.response_time)} · Last stored update: {utc(data.updated_at)} · Refresh every 5 seconds</p>
      {stale(data.updated_at) && <p className="warning">No recent stored results. The source may be idle or processing may have stopped.</p>}
      {context?.model_version.startsWith('fixture') && <p className="notice">Fixture mode: predictions and image references are synthetic.</p>}
      <p>Counts cover selected stored windows. Provisional results can change; predicted accepts are not confirmed product passes.</p>
      {!stations.length ? <p className="notice">No stored windows match this station or time range.</p> : stationId ? <>
        <h2>Station {stationId}</h2><Counts stations={stations} />
        <section><h3>Latest station context</h3><p>Model: {context?.model_version} · Category: {context?.category} · Threshold version: {context?.threshold_version}</p>
          <p>Work order, current item, machine state, temperature and conveyor speed: unavailable.</p></section>
        <section><h3>Inspection photos</h3><p>Individual inspection records and photos are not connected yet.</p></section>
        <section><h3>Window history</h3><Windows rows={windows} /></section>
        <section><h3>Rule evaluations</h3><Rules rows={alerts} windows={windows} /></section>
      </> : summary ? <>
        <h2>All-stations summary</h2><Counts stations={stations} />
        <section><h3>Active quality alerts ({activeAlerts.length})</h3><Rules rows={activeAlerts} windows={windows} /></section>
        <section><h3>Window history</h3><Windows rows={windows} /></section>
        <details><summary>All rule evaluations ({alerts.length})</summary><Rules rows={alerts} windows={windows} /></details>
      </> : <>
        <h2>Stations</h2><div className="stations">{stations.map((station) => <section key={station.line_id}>
          <h3><a href={`#station=${encodeURIComponent(station.line_id)}`}>Station {station.line_id}</a></h3>
          <p><span className={`badge ${stale(station.updated_at) ? 'stale' : 'clear'}`}>{stale(station.updated_at) ? 'Stale results' : 'Recent results'}</span></p>
          <Counts stations={[station]} /><p>Model: {context?.model_version}</p>
          <p>Machine state / temperature / work order: unavailable</p><small>Last stored update: {utc(station.updated_at)}</small>
        </section>)}</div>
      </>}
    </>}
  </main>;
}

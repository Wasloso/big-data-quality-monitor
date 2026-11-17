import React, { useEffect, useState } from 'react';
import { getJson, utcFilter, stationHref } from './api.js';
import { age, number, old, percent, producerState, utc } from './display.js';
import RecentInspections from './RecentInspections.jsx';
import StationObservation from './StationObservation.jsx';
import ProducerStatus, { useProducerStatus } from './ProducerStatus.jsx';

function Icon({ name }) {
  const paths = { grid: 'M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z',
    chart: 'M4 3v17h17 M8 15l4-5 4 2 5-7', refresh: 'M20 7v5h-5 M4 17v-5h5 M6 6a8 8 0 0113 3 M18 18A8 8 0 015 15',
    station: 'M4 21V9l6 3V6l6 4V3h4v18H4z M8 16h1 M13 16h1 M18 16h1', arrow: 'M5 12h14 M13 6l6 6-6 6' };
  return <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d={paths[name]} /></svg>;
}

function Counts({ stations, alerts, compact = false, available = true }) {
  const inspections = stations.reduce((sum, row) => sum + (row.inspections || 0), 0);
  const rejects = stations.reduce((sum, row) => sum + (row.rejects || 0), 0);
  const metrics = [
    { label: 'Inspected', value: number(available ? inspections : null), note: 'Processed unique inspections' },
    { label: 'Predicted accepted', value: number(available ? inspections - rejects : null), note: 'Model decisions', tone: 'good' },
    { label: 'Predicted rejects', value: number(available ? rejects : null), note: 'Model decisions', tone: rejects ? 'danger' : '' },
    { label: 'Reject rate', value: percent(available && inspections ? rejects / inspections : null), note: 'Weighted by inspection count' },
  ];
  if (alerts !== undefined) metrics.push({ label: 'Active quality alerts', value: number(available ? alerts : null), note: 'Selected stored windows', tone: alerts ? 'danger' : '' });
  return <div className={`counts ${compact ? 'compact' : ''}`}>{metrics.map((metric) => <div key={metric.label} className={`metric ${metric.tone || ''}`}>
    <span className="metric-label">{metric.label}</span><strong>{metric.value}</strong>{!compact && <small>{metric.note}</small>}
  </div>)}</div>;
}

function Windows({ rows }) {
  return <div className="table-scroll"><table>
    <thead><tr><th>Window · UTC</th><th>Station / category</th><th className="numeric">Inspected</th><th className="numeric">Rejects</th><th>Reject rate</th><th>Finality</th></tr></thead>
    <tbody>{rows.map((row) => <tr key={row.window_key}>
      <td className="mono">{utc(row.window_start)}<small className="cell-sub">to {utc(row.window_end).slice(11)}</small></td>
      <td><strong>{row.line_id}</strong><small className="cell-sub category">{row.category}</small></td>
      <td className="numeric">{number(row.inspections)}</td><td className={`numeric ${row.rejects ? 'text-danger' : ''}`}>{number(row.rejects)}</td>
      <td><div className="rate-cell"><span className="rate-track"><i style={{ width: `${row.reject_rate * 100}%` }} /></span><span className="mono">{percent(row.reject_rate)}</span></div></td>
      <td><span className={`badge ${row.is_final ? 'neutral' : 'warning'}`}>{row.is_final ? 'Final' : 'Provisional'}</span></td>
    </tr>)}</tbody>
  </table></div>;
}

function Rules({ rows, windows, runId }) {
  const byKey = Object.fromEntries(windows.map((row) => [row.window_key, row]));
  if (!rows.length) return <div className="empty-state"><span className="empty-symbol">—</span>No matching rule results in the selected windows.</div>;
  return <div className="table-scroll"><table>
    <thead><tr><th>Station / category</th><th>Window · UTC</th><th>Prediction count</th><th>Rate / rule</th><th>Status</th></tr></thead>
    <tbody>{rows.map((row) => {
      const window = byKey[row.window_key];
      return <tr key={row.alert_id}>
        <td><a className="station-link" href={stationHref(window, runId, window)}>Station {window.line_id}<span className="arrow">↗</span></a><small className="cell-sub category">{window.category}</small></td>
        <td className="mono">{utc(window.window_start)}<small className="cell-sub">{row.is_provisional ? 'Provisional' : 'Final'}</small></td>
        <td><span className={window.rejects ? 'text-danger' : ''}>{number(window.rejects)} rejects</span><small className="cell-sub">{number(window.inspections)} inspected</small></td>
        <td><strong>{percent(window.reject_rate)}</strong><small className="cell-sub">Rule &gt; {percent(row.threshold)} · n ≥ {row.minimum_inspections}</small></td>
        <td><span className={`badge ${row.status === 'active' ? 'danger' : row.status === 'insufficient_data' ? 'muted' : 'good'}`}><i />{row.status.replaceAll('_', ' ')}</span></td>
      </tr>;
    })}</tbody>
  </table></div>;
}

function QualityChart({ windows }) {
  const buckets = new Map();
  for (const row of windows) {
    const key = `${row.window_start}|${row.window_end}`;
    const bucket = buckets.get(key) || { start: row.window_start, end: row.window_end, inspections: 0, rejects: 0 };
    bucket.inspections += row.inspections; bucket.rejects += row.rejects; buckets.set(key, bucket);
  }
  const rows = [...buckets.values()].sort((a, b) => a.start.localeCompare(b.start)).slice(-12);
  const width = 860, left = 46, right = 12, top = 20, bottom = 160;
  const step = (width - left - right) / (rows.length || 1);
  return <section className="panel quality-chart"><div className="panel-heading"><div><span className="eyebrow">Quality over time</span><h2>Reject rate by window</h2></div><span className="tag">{rows.length} windows</span></div>
    {!rows.length ? <p className="empty-state">Awaiting stored quality windows.</p> : <figure>
      <svg viewBox={`0 0 ${width} 196`} role="img" aria-label="Weighted predicted reject rates for the last twelve selected stored windows">
        {[0, .25, .5, .75, 1].map((value) => <g key={value}><line x1={left} x2={width - right} y1={bottom - value * (bottom - top)} y2={bottom - value * (bottom - top)} className="chart-grid" /><text x={left - 10} y={bottom - value * (bottom - top) + 4} textAnchor="end" className="chart-label">{value * 100}%</text></g>)}
        {rows.map((row, index) => {
          const rate = row.inspections ? row.rejects / row.inspections : 0;
          const barWidth = Math.min(42, step * .55), height = Math.max(2, rate * (bottom - top));
          return <g key={row.start}><title>{utc(row.start)} – {utc(row.end)} UTC: {row.rejects}/{row.inspections} predicted rejects ({percent(rate)})</title>
            <rect x={left + step * (index + .5) - barWidth / 2} y={bottom - height} width={barWidth} height={height} rx="2" className="chart-bar" />
            <text x={left + step * (index + .5)} y={bottom - height - 7} textAnchor="middle" className="chart-value">{percent(rate)}</text>
            <text x={left + step * (index + .5)} y={bottom + 23} textAnchor="middle" className="chart-label">{utc(row.start).slice(11, 16)}</text>
          </g>;
        })}
      </svg><figcaption>UTC · Last 12 selected windows · Weighted across selected stations · Provisional results may change</figcaption>
    </figure>}
  </section>;
}

function StationCard({ station, session, unavailable, windows, alerts, runId }) {
  const state = producerState(session, unavailable);
  const keys = new Set(windows.filter((row) => row.line_id === station.line_id && row.category === station.category && row.model_version === station.model_version && row.threshold_version === station.threshold_version).map((row) => row.window_key));
  const active = alerts.filter((row) => keys.has(row.window_key) && row.status === 'active').length;
  const errors = session ? session.inference_errors + session.enqueue_errors + session.delivery_errors : null;
  return <section className={`panel station-card ${active ? 'has-alert' : ''}`}>
    <div className="station-card-heading"><div className="station-identity"><span className="station-symbol"><Icon name="station" /></span><div><span className="eyebrow category">{station.category}</span><h2><a href={stationHref(station, runId)}>Station {station.line_id}<span className="arrow">↗</span></a></h2></div></div>
      <span className={`badge ${state.tone}`} title={session ? `Producer report: ${session.state} at ${utc(session.updated_at)} UTC` : ''}><i />{state.label}</span></div>
    <div className="station-health"><span className={active ? 'text-danger' : 'subtle'}>{active ? `${active} active quality ${active === 1 ? 'alert' : 'alerts'}` : station.inspections == null ? 'Awaiting processed windows' : 'No active quality alerts'}</span><span className="subtle">{station.updated_at ? `Stored ${age(station.updated_at)}` : 'No window snapshot'}</span></div>
    <Counts stations={[station]} compact available={station.inspections != null} />
    <StationObservation item={station.latest_observation} />
    <div className="station-session-strip"><span>Latest session Kafka ack.<strong>{number(session?.published)}</strong></span><span>Session errors<strong className={errors ? 'text-danger' : ''}>{number(errors)}</strong></span><span>Pending<strong className={session?.pending ? 'text-warning' : ''}>{number(session?.pending)}</strong></span></div>
    <div className="station-card-footer"><span className="mono" title={`${station.model_version} / ${station.threshold_version}`}>{station.model_version}</span><a href={stationHref(station, runId)}>Inspect station <Icon name="arrow" /></a></div>
  </section>;
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
  const clearWindowLink = () => {
    const params = new URLSearchParams(window.location.hash.slice(1));
    params.delete('window_from'); params.delete('window_to');
    window.location.hash = params.toString();
  };
  const producer = useProducerStatus(context, stationId, refresh);
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
  const producerUnavailable = !!producer.error || producer.data?.state === 'partial';
  const latestSessions = (producer.data?.sessions || []).filter((session) => session.latest_started_session);
  // Producer-only stations stay visible before Spark has produced the first window.
  const cards = [...stations];
  if (!error) for (const session of latestSessions) {
    if (!cards.some((row) => row.line_id === session.station && row.category === session.category && row.model_version === session.model_version && row.threshold_version === session.threshold_version)) {
      cards.push({ line_id: session.station, category: session.category, model_version: session.model_version, threshold_version: session.threshold_version });
    }
  }
  const matchingSession = (station) => latestSessions.find((session) => session.station === station.line_id && session.category === station.category && session.model_version === station.model_version && session.threshold_version === station.threshold_version);
  const title = stationId ? `Station ${stationId}` : summary ? 'Quality & alerts' : 'Station overview';
  const provisional = windows.filter((row) => !row.is_final).length;

  return <div className="app-shell">
    <aside className="sidebar">
      <a className="brand" href={navigation('stations')}><span className="brand-mark"><Icon name="station" /></span><span>QUALITY<span className="brand-sub">OPERATIONS MONITOR</span></span></a>
      <div className="sidebar-section">Workspace</div>
      <nav aria-label="Main navigation"><a href={navigation('stations')} aria-current={!stationId && !summary ? 'page' : undefined}><Icon name="grid" />Station overview</a>
        <a href={navigation('summary')} aria-current={!stationId && summary ? 'page' : undefined}><Icon name="chart" />Quality & alerts{data && <span className={`nav-count ${activeAlerts.length ? 'has-count' : ''}`}>{activeAlerts.length}</span>}</a></nav>
      {!!cards.length && <><div className="sidebar-section">Stations in this selection</div><div className="station-nav">{cards.map((station) => <a key={`${station.line_id}|${station.category}|${station.model_version}|${station.threshold_version}`} href={stationHref(station, selected)} aria-current={stationId === station.line_id && context?.model_version === station.model_version ? 'page' : undefined}><span className="station-nav-id">{station.line_id}</span><span>Station {station.line_id}<small className="category">{station.category}</small></span><span className={`status-dot ${producerState(matchingSession(station), producerUnavailable).tone}`} /></a>)}</div></>}
      <div className="sidebar-footer"><span className="environment-label"><span className="status-dot neutral" />LOCAL SIMULATION</span><p>Recorded model predictions<br />Simulated station readings</p><div>Snapshot interval <strong>5 s</strong></div></div>
    </aside>
    <div className="workspace">
      <header className="topbar"><span className="breadcrumb">Operations <span>/</span> {stationId ? `Station ${stationId}` : summary ? 'Quality' : 'Stations'}</span>
        <div className="topbar-status"><span className={`status-dot ${error ? 'warning' : data ? 'good' : 'muted'}`} />{error ? 'Overview unavailable' : data ? 'API snapshot received' : 'Awaiting snapshot'}<span className="topbar-divider" /><span className="mono">UTC</span></div></header>
      <main>
        <div className="page-heading"><div><span className="eyebrow">{stationId ? `${context?.category || 'Inspection'} / station detail` : 'Production monitoring'}</span><h1>{title}</h1><p>{stationId ? 'Inspection evidence, operating context and quality history.' : summary ? 'Windowed quality results and rule evaluations across the selected run.' : 'Acquisition status, processed output and recorded station conditions.'}</p></div>
          <button className="refresh-button" onClick={() => setRefresh((value) => value + 1)}><Icon name="refresh" />Refresh</button></div>
        <section className="filter-bar" aria-label="Data filters">
          <label className="run-filter">Run<select value={selected} onChange={(event) => {
            setSelected(event.target.value);
            window.location.hash = new URLSearchParams({ view: 'stations', run_id: event.target.value }).toString();
          }} disabled={!runs.length}>
            {!runs.length && <option value="">No runs available</option>}
            {selected && !runIds.includes(selected) && <option value={selected}>{selected} (unavailable)</option>}
            {runIds.map((runId) => <option key={runId} value={runId}>{runId}</option>)}
          </select></label>
          <label>From (UTC)<input type="datetime-local" step="1" value={from} onChange={(event) => { setFrom(event.target.value); clearWindowLink(); }} /></label>
          <label>To (UTC)<input type="datetime-local" step="1" value={to} onChange={(event) => { setTo(event.target.value); clearWindowLink(); }} /></label>
          {(from || to) && <button className="text-button" onClick={() => { setFrom(''); setTo(''); clearWindowLink(); }}>Clear range</button>}
        </section>
        {truncated && <p className="notice">Showing the 100 most recently updated run/detector contexts.</p>}
        {['partial', 'unavailable'].includes(statusDiscovery) && <p className="notice warning">Producer discovery incomplete. Stored quality runs remain available.</p>}
        {error && <p className="notice warning" role="alert">{error}. {data && 'Showing the last successful snapshot.'}</p>}
        {loading && <p className="loading-state" role="status">Loading results…</p>}
        {!loading && !error && !runs.length && <div className="panel empty-state">{source === 'missing' ? 'No serving database yet.' : 'No inspection results yet.'}</div>}
        {(data || cards.length > 0) && <>
          <div className="snapshot-strip"><span><span className={`status-dot ${old(data?.updated_at) ? 'warning' : 'good'}`} />Quality snapshot <strong title={`${utc(data?.updated_at)} UTC`}>{age(data?.updated_at)}</strong></span>
            <span>{provisional > 0 && <span className="tag warning">{provisional} provisional windows</span>}<span className="subtle">{windows.length - provisional} final · {cards.length} station contexts</span></span></div>
          {data?.updated_at && old(data.updated_at) && <p className="notice warning">Stored results are old. Acquisition may be stopped or processing delayed; current machine state is unknown.</p>}
          {stations.some((station) => station.model_version.startsWith('fixture')) && <p className="notice">Fixture mode · Scores and image references are synthetic.</p>}
          {data?.observation_state === 'unavailable' && <p className="notice warning">Station observations unavailable. Stored quality totals remain available.</p>}
          <Counts stations={stations} alerts={activeAlerts.length} available={!!data && windows.length > 0} />
        </>}
        {!stationId && !summary && !error && <>
          <div className="section-heading"><h2>Inspection stations</h2><span className="subtle">Producer reports and processed totals are independent</span></div>
          {!cards.length && !loading ? <div className="panel empty-state">No stations match this selection.</div> : <div className="stations">{cards.map((station) => <StationCard key={`${station.line_id}|${station.category}|${station.model_version}|${station.threshold_version}`} station={station} session={matchingSession(station)} unavailable={producerUnavailable} windows={windows} alerts={alerts} runId={selected} />)}</div>}
        </>}
        {stationId && context && <section className="panel station-detail-context"><div className="panel-heading"><div><span className="eyebrow">Recorded operating context</span><h2>Station conditions</h2></div><span className={`badge ${producerState(latestSessions[0], producerUnavailable).tone}`}><i />{producerState(latestSessions[0], producerUnavailable).label}</span></div>
          <StationObservation item={stations[0]?.latest_observation} />
          <details className="detector-details"><summary>Detector provenance</summary><dl className="trace-list"><dt>Category</dt><dd>{context.category || 'Unknown'}</dd><dt>Model</dt><dd>{context.model_version || 'Unknown'}</dd><dt>Threshold version</dt><dd>{context.threshold_version || 'Unknown'}</dd></dl></details></section>}
        {data && <>
          <div className="quality-grid"><QualityChart windows={windows} /><section className="panel quality-state"><div className="panel-heading"><div><span className="eyebrow">Window evaluation</span><h2>Quality status</h2></div><span className={`badge ${activeAlerts.length ? 'danger' : 'neutral'}`}>{activeAlerts.length} active</span></div>
            <dl className="quality-totals"><div><dt>Evaluated rule results</dt><dd>{number(alerts.length)}</dd></div><div><dt>Active quality alerts</dt><dd className={activeAlerts.length ? 'text-danger' : ''}>{number(activeAlerts.length)}</dd></div><div><dt>Insufficient data</dt><dd>{number(alerts.filter((row) => row.status === 'insufficient_data').length)}</dd></div><div><dt>Provisional / final windows</dt><dd>{provisional} / {windows.length - provisional}</dd></div></dl>
            <p className="panel-description">Alerts apply to stored station windows. Provisional results can change; accepted predictions are not confirmed product passes.</p></section></div>
          {(summary || stationId || activeAlerts.length > 0) && <section className="panel"><div className="panel-heading"><div><span className="eyebrow">Requires review</span><h2>Active quality alerts</h2></div><span className="tag">{activeAlerts.length} results</span></div><Rules rows={activeAlerts} windows={windows} runId={selected} /></section>}
        </>}
        {stationId && context?.category && <>
          {windowFrom && windowTo && <p className="notice">Rule link selected a window’s retained photos. Photos are related examples, not causal evidence.</p>}
          <RecentInspections key={`${selected}|${stationId}|${context.category}|${context.model_version}|${context.threshold_version}|${from}|${to}`} context={context} stationId={stationId} from={from} to={to} refresh={refresh} threshold={latestSessions.find((session) => session.category === context.category && session.model_version === context.model_version && session.threshold_version === context.threshold_version)?.threshold} />
        </>}
        {data && <section className="panel"><div className="panel-heading"><div><span className="eyebrow">Processed quality snapshots</span><h2>Window history</h2></div><span className="tag">{windows.length} windows</span></div>
          {windows.length ? <Windows rows={windows} /> : <p className="empty-state">No stored windows match this station or time range.</p>}
          <div className="panel-footnote"><span>Complete windows within selected bounds · counts exclude invalid and duplicate inspections</span><span>Snapshot {utc(data.response_time)} UTC</span></div></section>}
        {data && <details className="panel rule-history"><summary>All rule evaluations <span className="tag">{alerts.length}</span></summary><Rules rows={alerts} windows={windows} runId={selected} /></details>}
        {context && <ProducerStatus context={context} report={producer} />}
        <footer className="workspace-footer"><span>Quality monitor <span className="footer-separator">/</span> Local industrial inspection demo</span><span>UTC timestamps · HTTP polling every 5 s</span></footer>
      </main>
    </div>
  </div>;
}

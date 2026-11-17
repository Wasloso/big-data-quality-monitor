import React from 'react';
import { age, old, utc } from './display.js';

export default function StationObservation({ item, label = 'Latest station observation' }) {
  const context = item?.station_context;
  if (!context) return <div className="observation-empty"><span className="status-dot muted" />Station readings unavailable</div>;
  return <div className="station-observation">
    <div className="section-label"><span>{label}</span><span className="tag">Simulated</span></div>
    <div className="readings">
      <div><span>Temperature</span><strong>{context.temperature_c.toFixed(1)}<small>°C</small></strong></div>
      <div><span>Conveyor speed</span><strong>{context.conveyor_speed_m_min.toFixed(1)}<small>m/min</small></strong></div>
    </div>
    <dl className="context-grid">
      <div><dt>Work order</dt><dd>{context.work_order}</dd></div>
      <div><dt>Operation</dt><dd>{context.operation}</dd></div>
      <div><dt>Recorded state</dt><dd>{context.reported_state}</dd></div>
      <div><dt>Observed</dt><dd title={`${utc(item.event_time)} UTC`}>{age(item.event_time)}</dd></div>
    </dl>
    <p className={`observation-age ${old(item.event_time) ? 'text-warning' : ''}`}>
      {old(item.event_time) ? 'Old observation · current machine state unknown' : 'Updates with inspections · not a machine heartbeat'}
    </p>
  </div>;
}

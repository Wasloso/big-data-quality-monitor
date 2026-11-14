import React from 'react';

export default function StationObservation({ item, label = 'Latest observation in this run' }) {
  const context = item?.station_context;
  if (!context) return <p>Machine context unavailable.</p>;
  const observed = new Date(item.event_time);
  const stale = Date.now() - observed.getTime() > 300_000;
  return <div className="station-observation">
    <p><strong>{label} · simulated readings</strong></p>
    <p>Work order: {context.work_order} · Operation: {context.operation}</p>
    <p>Temperature: {context.temperature_c.toFixed(2)} °C · Conveyor: {context.conveyor_speed_m_min.toFixed(2)} m/min</p>
    <p>Reported state at observation: {context.reported_state}</p>
    <small>{observed.toISOString().replace('T', ' ').slice(0, 19)} UTC · Observations update with inspections.</small>
    {stale && <p className="warning">Old observation. Current machine state is unknown.</p>}
  </div>;
}

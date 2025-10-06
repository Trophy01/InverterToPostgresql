const deviceSel = document.getElementById('device');
const windowSel = document.getElementById('window');
const btnRefresh = document.getElementById('refresh');

const kSOC = document.getElementById('k-soc');
const kV = document.getElementById('k-v');
const kI = document.getElementById('k-i');
const kCycles = document.getElementById('k-cycles');
const kStatus = document.getElementById('k-status');

let charts = {};

async function fetchJSON(url){
  const r = await fetch(url);
  if(!r.ok) throw new Error('HTTP '+r.status);
  return r.json();
}

async function loadDevices(){
  const ids = await fetchJSON('/api/devices');
  deviceSel.innerHTML = '';
  for(const id of ids){
    const opt = document.createElement('option');
    opt.value = id; opt.textContent = id;
    deviceSel.appendChild(opt);
  }
  if(ids.length){
    deviceSel.value = ids[0];
  }
}

function setKpi(summary){
  const s = summary.status || {};
  kSOC.textContent = s.soc_percent != null ? (s.soc_percent.toFixed(1)+'%') : '–';
  kV.textContent = s.total_voltage_mv != null ? (s.total_voltage_mv+' mV') : '–';
  kI.textContent = s.current_amps != null ? (s.current_amps.toFixed(1)+' A') : '–';
  kCycles.textContent = s.loop_cycles != null ? s.loop_cycles : '–';
  kStatus.textContent = s.status_text || '–';
}

function lineChart(ctxId, labels, datasets){
  if(charts[ctxId]) charts[ctxId].destroy();
  const ctx = document.getElementById(ctxId);
  charts[ctxId] = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      resizeDelay: 100,
      scales: { x: { ticks: { color: '#a8adb3' } }, y: { ticks: { color: '#a8adb3' }, grid: { color: '#2a2f33' } } },
      plugins: { legend: { labels: { color: '#e7e9ea' } } }
    }
  });
}

function renderSeries(series){
  const status = series.status || [];
  const labels = status.map(p => new Date(p.t).toLocaleTimeString());
  lineChart('c-status', labels, [
    { label: 'SOC %', data: status.map(p => p.soc), borderColor: '#5fbf6b', tension: 0.2 },
    { label: 'Voltage mV', data: status.map(p => p.v), borderColor: '#77c2ff', tension: 0.2 },
    { label: 'Current A', data: status.map(p => p.i), borderColor: '#e7c85a', tension: 0.2 }
  ]);

  const temps = series.temps || [];
  const tlabels = temps.map(p => new Date(p.t).toLocaleTimeString());
  const bms = temps.map(p => (p.bms && p.bms.length) ? p.bms.reduce((a,b)=>a+b,0)/p.bms.length : null);
  const cells = temps.map(p => (p.cells && p.cells.length) ? p.cells.reduce((a,b)=>a+b,0)/p.cells.length : null);
  lineChart('c-temps', tlabels, [
    { label: 'BMS °C', data: bms, borderColor: '#e57373', tension: 0.2 },
    { label: 'Cells °C', data: cells, borderColor: '#e7c85a', tension: 0.2 }
  ]);

  const cellsSeries = series.cells || [];
  const clabels = cellsSeries.map(p => new Date(p.t).toLocaleTimeString());
  const maxLen = Math.max(0, ...cellsSeries.map(p => (p.mv||[]).length));
  const ds = [];
  for(let i=0;i<maxLen;i++){
    ds.push({ label: 'C'+(i+1), data: cellsSeries.map(p => (p.mv||[])[i] ?? null), borderColor: i%2? '#5fbf6b':'#77c2ff', tension: 0.2, borderWidth: 1 });
  }
  lineChart('c-cells', clabels, ds);

  const pos = series.pos || [];
  const plabels = pos.map(p => new Date(p.t).toLocaleTimeString());
  lineChart('c-pos', plabels, [
    { label: 'Satellites', data: pos.map(p => p.sats), borderColor: '#5fbf6b', tension: 0.2 },
    { label: 'Direction', data: pos.map(p => p.dir ?? 0), borderColor: '#e7c85a', tension: 0.2 }
  ]);
}

async function refresh(){
  const id = deviceSel.value;
  if(!id) return;
  const hours = windowSel.value;
  const [sum, ser] = await Promise.all([
    fetchJSON(`/api/summary/${id}`),
    fetchJSON(`/api/series/${id}?hours=${hours}`)
  ]);
  setKpi(sum);
  renderSeries(ser);

  // Build Google Maps links for each valid position
  const pos = (ser.pos || []).filter(p => typeof p.lat === 'number' && typeof p.lon === 'number');
  const list = document.getElementById('pos-links-list');
  if(list){
    list.innerHTML = '';
    for(const p of pos.slice(-8).reverse()){
      let y = parseFloat(p.lat), x = parseFloat(p.lon);
      if (Math.abs(y) > 90 && Math.abs(x) <= 90) { const t=y; y=x; x=t; }
      while (x > 180) x -= 360; while (x < -180) x += 360; if (y > 90) y = 90; if (y < -90) y = -90;
      const a = document.createElement('a');
      a.className = 'pos-link';
      a.href = `https://www.google.com/maps?q=${y},${x}`;
      a.target = '_blank'; a.rel = 'noopener';
      const when = new Date(p.t).toLocaleTimeString();
      a.textContent = `View on Maps • ${when}`;
      list.appendChild(a);
    }
  }
}

(async function init(){
  await loadDevices();
  await refresh();
  btnRefresh.addEventListener('click', refresh);
  deviceSel.addEventListener('change', refresh);
  windowSel.addEventListener('change', refresh);
  setInterval(refresh, 15000);
})();

// Removed embedded Leaflet map; using external Google Maps links instead.



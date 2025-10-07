const deviceSel = document.getElementById('device');
const lastUpdateEl = document.getElementById('last-update');
const btnRefresh = document.getElementById('refresh');

const kSOC = document.getElementById('k-soc');
const kV = document.getElementById('k-v');
const kI = document.getElementById('k-i');
const kCycles = document.getElementById('k-cycles');
const kStatus = document.getElementById('k-status');
const kTemp = document.getElementById('k-temp');

let charts = {};
let batteryMap = null;

async function fetchJSON(url){
  const r = await fetch(url);
  if(!r.ok) throw new Error('HTTP '+r.status);
  return r.json();
}

async function loadDevices(){
  // If there is no device selector on the page, fall back to DEFAULT_DEVICE
  if(!deviceSel){ return; }
  const ids = await fetchJSON('/api/devices');
  deviceSel.innerHTML = '';
  for(const id of ids){
    const opt = document.createElement('option');
    opt.value = id; opt.textContent = id;
    deviceSel.appendChild(opt);
  }
  if(ids.length){
    // Prefer current user's battery id if present
    const def = window.DEFAULT_DEVICE;
    deviceSel.value = ids.includes(def) ? def : ids[0];
  }
}

function setKpi(summary){
  const s = summary.status || {};
  // Hero SOC emphasized
  kSOC.textContent = s.soc_percent != null ? (Math.round(s.soc_percent)+'%') : '–';
  kV.textContent = s.total_voltage_mv != null ? (s.total_voltage_mv+' mV') : '–';
  kI.textContent = s.current_amps != null ? (s.current_amps.toFixed(1)+' A') : '–';
  kCycles.textContent = s.loop_cycles != null ? s.loop_cycles : '–';
  kStatus.textContent = s.status_text || '–';
  // Show average temp if available
  if (Array.isArray(s.temps_c) && s.temps_c.length){
    const avg = s.temps_c.reduce((a,b)=>a+b,0)/s.temps_c.length;
    kTemp.textContent = avg.toFixed(1)+' °C';
  }
}

function lineChart(ctxId, labels, datasets){
  const el = document.getElementById(ctxId);
  if(!el) return;
  const traces = datasets.map((ds, i) => ({
    x: labels,
    y: ds.data,
    type: 'scatter',
    mode: 'lines',
    name: ds.label,
    line: { color: ds.borderColor || '#8B5CF6', width: 2 },
    fill: 'tozeroy',
    fillcolor: 'rgba(139,92,246,0.08)'
  }));
  const layout = {
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: 'rgba(0,0,0,0)',
    margin: { l: 40, r: 20, t: 10, b: 30 },
    xaxis: { tickfont: { color: '#6b7280' }, gridcolor: '#e5e7eb' },
    yaxis: { tickfont: { color: '#6b7280' }, gridcolor: '#e5e7eb' },
    showlegend: true,
    legend: { font: { color: '#6b7280' } }
  };
  Plotly.react(el, traces, layout, {displayModeBar: false, responsive: true});
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

  // Empty/old placeholders
  function ensurePlaceholder(chartId, seriesArr){
    const container = document.getElementById(chartId)?.parentElement;
    if(!container) return;
    let placeholder = container.querySelector('.chart-empty');
    const hasData = Array.isArray(seriesArr) && seriesArr.length > 0;
    // Detect very old data (> 7 days)
    let oldMsg = null;
    if(hasData){
      const lastIso = seriesArr[seriesArr.length-1]?.t;
      if(lastIso){
        const last = new Date(lastIso);
        const ageDays = (Date.now() - last.getTime()) / (1000*60*60*24);
        if(ageDays > 7){ oldMsg = `Last data from: ${last.toLocaleString()}`; }
      }
    }
    if(!hasData || oldMsg){
      if(!placeholder){
        placeholder = document.createElement('div');
        placeholder.className = 'chart-empty';
        container.appendChild(placeholder);
      }
      placeholder.textContent = oldMsg || 'No data yet';
      placeholder.style.display = 'flex';
    }else if(placeholder){
      placeholder.style.display = 'none';
    }
  }

  ensurePlaceholder('c-status', status);
  ensurePlaceholder('c-temps', temps);
  ensurePlaceholder('c-cells', cellsSeries);
  ensurePlaceholder('c-pos', pos);
}

// Google Maps integration
function initMap() {
  // This function is called by Google Maps API when it loads
  console.log('Google Maps API loaded');
  updateBatteryMap();
}

function updateBatteryMap() {
  const mapElement = document.getElementById('battery-map');
  const placeholder = document.getElementById('map-placeholder');
  
  if (!mapElement || !placeholder) return;
  
  // Check if Google Maps is available
  if (typeof google === 'undefined' || !google.maps) {
    placeholder.querySelector('.map-placeholder-text').textContent = 'Google Maps API not loaded';
    placeholder.querySelector('.map-placeholder-note').textContent = 'Check your API key configuration';
    return;
  }
  
  // Get latest position from summary data
  const latestPos = window.latestPositionData;
  if (!latestPos || !latestPos.lat || !latestPos.lon) {
    placeholder.querySelector('.map-placeholder-text').textContent = 'No location data available';
    placeholder.querySelector('.map-placeholder-note').textContent = 'Battery position not yet reported';
    return;
  }
  
  // Initialize map
  const batteryLocation = { lat: latestPos.lat, lng: latestPos.lon };
  
  batteryMap = new google.maps.Map(mapElement, {
    zoom: 15,
    center: batteryLocation,
    styles: [
      {
        featureType: 'all',
        elementType: 'geometry.fill',
        stylers: [{ color: '#f8fafc' }]
      },
      {
        featureType: 'water',
        elementType: 'geometry.fill',
        stylers: [{ color: '#e0f2fe' }]
      },
      {
        featureType: 'road',
        elementType: 'geometry.stroke',
        stylers: [{ color: '#e5e7eb' }]
      }
    ]
  });
  
  // Add battery marker
  new google.maps.Marker({
    position: batteryLocation,
    map: batteryMap,
    title: `Battery ${window.DEFAULT_DEVICE || 'Location'}`,
    icon: {
      path: google.maps.SymbolPath.CIRCLE,
      scale: 8,
      fillColor: '#8B5CF6',
      fillOpacity: 1,
      strokeColor: '#ffffff',
      strokeWeight: 2
    }
  });
  
  // Hide placeholder
  placeholder.style.display = 'none';
}

// Store latest position data for map updates
function setKpi(summary){
  const s = summary.status || {};
  // Hero SOC emphasized
  kSOC.textContent = s.soc_percent != null ? (Math.round(s.soc_percent)+'%') : '–';
  kV.textContent = s.total_voltage_mv != null ? (s.total_voltage_mv+' mV') : '–';
  kI.textContent = s.current_amps != null ? (s.current_amps.toFixed(1)+' A') : '–';
  kCycles.textContent = s.loop_cycles != null ? s.loop_cycles : '–';
  kStatus.textContent = s.status_text || '–';
  // Show average temp if available
  if (Array.isArray(s.temps_c) && s.temps_c.length){
    const avg = s.temps_c.reduce((a,b)=>a+b,0)/s.temps_c.length;
    kTemp.textContent = avg.toFixed(1)+' °C';
  }
  
  // Store position data for map
  if (summary.position) {
    window.latestPositionData = summary.position;
    updateBatteryMap();
  }
}

async function refresh(){
  // Resolve device id: prefer selector, else DEFAULT_DEVICE
  const id = deviceSel && deviceSel.value ? deviceSel.value : (window.DEFAULT_DEVICE || '');
  if(!id) return;
  // Fixed lookback window (e.g., 12 hours) now that selector is removed
  const hours = 12;
  const [sum, ser] = await Promise.all([
    fetchJSON(`/api/summary/${id}`),
    fetchJSON(`/api/series/${id}?hours=${hours}`)
  ]);
  setKpi(sum);
  // Update Last update banner using unified latest_time or fallback to status time
  if(lastUpdateEl){
    const iso = (sum && sum.latest_time) || (sum && sum.status && sum.status.time) || null;
    if(iso){
      const d = new Date(iso);
      lastUpdateEl.textContent = `Last update: ${d.toLocaleString()}`;
      const ageSec = (Date.now() - d.getTime())/1000;
      if(ageSec > 600){ // older than 10 minutes -> warn color
        lastUpdateEl.style.color = '#ef4444';
      }else if(ageSec > 120){ // older than 2 minutes -> muted
        lastUpdateEl.style.color = '#eab308';
      }else{
        lastUpdateEl.style.color = '';
      }
    }else{
      lastUpdateEl.textContent = 'Last update: –';
      lastUpdateEl.style.color = '';
    }
  }
  // SOC highlight pulse when value changes
  if(typeof sum?.status?.soc_percent === 'number'){
    const next = Math.round(sum.status.soc_percent);
    if(typeof window.__lastSOC === 'number' && next !== window.__lastSOC){
      kSOC.classList.remove('highlight');
      void kSOC.offsetWidth; // reflow
      kSOC.classList.add('highlight');
    }
    window.__lastSOC = next;
  }
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
  if(deviceSel){ deviceSel.addEventListener('change', refresh); }
  setInterval(refresh, 15000);

  // Page load cascade animations
  const hero = document.getElementById('reveal-hero');
  if(hero){ setTimeout(()=>hero.classList.add('show'), 400); }
  const pills = document.querySelectorAll('.chip');
  pills.forEach((p,i)=> setTimeout(()=>p.classList.add('show'), 600 + i*100));
  const cards = document.querySelectorAll('.reveal-scale');
  cards.forEach((c,i)=> setTimeout(()=>c.classList.add('show'), 900 + i*120));
})();

// Removed embedded Leaflet map; using external Google Maps links instead.



const btnRefresh = document.getElementById('refresh');
const batteriesGrid = document.getElementById('batteries-grid');
const controlModal = document.getElementById('control-modal');
const controlResponse = document.getElementById('control-response');

let batteries = [];
let systemStatus = { database: false, mqtt: false };

async function fetchJSON(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

function updateSystemStatus() {
  const dbStatus = document.getElementById('db-status');
  const mqttStatus = document.getElementById('mqtt-status');
  
  dbStatus.textContent = `Database: ${systemStatus.database ? 'Online' : 'Offline'}`;
  dbStatus.className = `status-indicator ${systemStatus.database ? 'online' : 'offline'}`;
  
  mqttStatus.textContent = `MQTT: ${systemStatus.mqtt ? 'Online' : 'Offline'}`;
  mqttStatus.className = `status-indicator ${systemStatus.mqtt ? 'online' : 'offline'}`;
}

function getBatteryStatus(battery) {
  if (!battery.last_seen) return 'unknown';
  
  const lastSeen = new Date(battery.last_seen);
  const ageMinutes = (Date.now() - lastSeen.getTime()) / (1000 * 60);
  
  if (ageMinutes < 10) return 'online';
  if (ageMinutes < 60) return 'warning';
  return 'offline';
}

function formatTime(isoString) {
  if (!isoString) return 'Never';
  return new Date(isoString).toLocaleString();
}

function formatValue(value, unit = '') {
  if (value === null || value === undefined) return '-';
  return `${value}${unit}`;
}

function createBatteryCard(battery) {
  const status = getBatteryStatus(battery);
  const card = document.createElement('div');
  card.className = 'battery-card';
  card.innerHTML = `
    <div class="battery-header">
      <div class="battery-id">${battery.device_id}</div>
      <div class="battery-status ${status}">${status}</div>
    </div>
    
    <div class="battery-metrics">
      <div class="metric">
        <div class="metric-value">${formatValue(battery.status?.soc_percent, '%')}</div>
        <div class="metric-label">SOC</div>
      </div>
      <div class="metric">
        <div class="metric-value">${formatValue(battery.status?.total_voltage_mv, 'mV')}</div>
        <div class="metric-label">Voltage</div>
      </div>
      <div class="metric">
        <div class="metric-value">${formatValue(battery.status?.current_amps, 'A')}</div>
        <div class="metric-label">Current</div>
      </div>
      <div class="metric">
        <div class="metric-value">${formatValue(battery.position?.sats_total)}</div>
        <div class="metric-label">Satellites</div>
      </div>
    </div>
    
    <div class="battery-controls">
      <button class="control-btn" data-device="${battery.device_id}" data-type="1" data-value="0">Enable Discharge</button>
      <button class="control-btn" data-device="${battery.device_id}" data-type="1" data-value="1">Disable Discharge</button>
      <button class="control-btn" data-device="${battery.device_id}" data-type="2" data-value="0">Enable Charge</button>
      <button class="control-btn" data-device="${battery.device_id}" data-type="2" data-value="1">Disable Charge</button>
    </div>
    
    <div class="battery-actions">
      <button class="action-btn" onclick="viewHistory('${battery.device_id}')">View History</button>
      <button class="action-btn" onclick="viewLocation('${battery.device_id}')">View Location</button>
    </div>
    
    <div style="margin-top: 12px; font-size: 11px; color: #6b7280;">
      Last seen: ${formatTime(battery.last_seen)}
    </div>
  `;
  
  // Add control button event listeners
  card.querySelectorAll('.control-btn').forEach(btn => {
    btn.addEventListener('click', (e) => {
      const deviceId = e.target.dataset.device;
      const controlType = parseInt(e.target.dataset.type);
      const value = parseInt(e.target.dataset.value);
      sendControlCommand(deviceId, controlType, value);
    });
  });
  
  return card;
}

function updateBatteriesGrid() {
  batteriesGrid.innerHTML = '';
  
  if (batteries.length === 0) {
    batteriesGrid.innerHTML = '<div style="grid-column: 1/-1; text-align: center; color: #6b7280; padding: 40px;">No batteries found</div>';
    return;
  }
  
  batteries.forEach(battery => {
    const card = createBatteryCard(battery);
    batteriesGrid.appendChild(card);
  });
}

function updateStats() {
  const totalBatteries = batteries.length;
  const onlineBatteries = batteries.filter(b => getBatteryStatus(b) === 'online').length;
  const offlineBatteries = batteries.filter(b => getBatteryStatus(b) === 'offline').length;
  const avgSoc = batteries
    .filter(b => b.status?.soc_percent !== null)
    .reduce((sum, b) => sum + b.status.soc_percent, 0) / batteries.filter(b => b.status?.soc_percent !== null).length;
  
  document.getElementById('total-batteries').textContent = totalBatteries;
  document.getElementById('online-batteries').textContent = onlineBatteries;
  document.getElementById('offline-batteries').textContent = offlineBatteries;
  document.getElementById('avg-soc').textContent = avgSoc ? `${Math.round(avgSoc)}%` : '-';
}

async function loadBatteries() {
  try {
    batteries = await fetchJSON('/api/admin/batteries');
    updateBatteriesGrid();
    updateStats();
  } catch (error) {
    console.error('Failed to load batteries:', error);
    batteriesGrid.innerHTML = '<div style="grid-column: 1/-1; text-align: center; color: #ef4444; padding: 40px;">Failed to load batteries</div>';
  }
}

async function loadSystemStatus() {
  try {
    systemStatus = await fetchJSON('/api/admin/status');
    updateSystemStatus();
  } catch (error) {
    console.error('Failed to load system status:', error);
    systemStatus = { database: false, mqtt: false };
    updateSystemStatus();
  }
}

async function sendControlCommand(deviceId, controlType, value) {
  try {
    const response = await fetchJSON('/api/admin/control', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ device_id: deviceId, control_type: controlType, value: value })
    });
    
    showControlResponse(response);
  } catch (error) {
    console.error('Control command failed:', error);
    showControlResponse({
      success: false,
      message: 'Control command failed',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
}

function showControlResponse(response) {
  const statusClass = response.success ? 'success' : 'error';
  const statusText = response.success ? 'Success' : 'Error';
  
  controlResponse.innerHTML = `
    <div class="response-status ${statusClass}">
      <strong>${statusText}</strong>
    </div>
    <div class="response-message">${response.message}</div>
    ${response.command ? `<div class="response-command">Command: ${response.command}</div>` : ''}
    ${response.error ? `<div class="response-error">Error: ${response.error}</div>` : ''}
    <div class="response-time">Time: ${new Date(response.timestamp).toLocaleString()}</div>
  `;
  
  controlModal.style.display = 'block';
}

function viewHistory(deviceId) {
  // This would open a modal or navigate to a history view
  console.log('View history for:', deviceId);
  // For now, just show an alert
  alert(`History view for ${deviceId} - Feature coming soon`);
}

function viewLocation(deviceId) {
  const battery = batteries.find(b => b.device_id === deviceId);
  if (battery?.position?.lat && battery?.position?.lon) {
    const url = `https://www.google.com/maps?q=${battery.position.lat},${battery.position.lon}`;
    window.open(url, '_blank');
  } else {
    alert('No location data available for this battery');
  }
}

// Modal functionality
document.querySelector('.modal-close').addEventListener('click', () => {
  controlModal.style.display = 'none';
});

window.addEventListener('click', (e) => {
  if (e.target === controlModal) {
    controlModal.style.display = 'none';
  }
});

// Event listeners
btnRefresh.addEventListener('click', async () => {
  await Promise.all([loadBatteries(), loadSystemStatus()]);
});

// Auto-refresh every 30 seconds
setInterval(async () => {
  await Promise.all([loadBatteries(), loadSystemStatus()]);
}, 30000);

// Initialize
(async function init() {
  await Promise.all([loadBatteries(), loadSystemStatus()]);
})();

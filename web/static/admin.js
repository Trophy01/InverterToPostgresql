const btnRefresh = document.getElementById('refresh');
const batteriesGrid = document.getElementById('batteries-grid');
const controlModal = document.getElementById('control-modal');
const controlResponse = document.getElementById('control-response');
const historyModal = document.getElementById('history-modal');
const historyTitle = document.getElementById('history-title');

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
    
    if (response.success && response.command_id) {
      // Start polling for response
      pollControlResponse(response.command_id, response);
    } else {
      showControlResponse(response);
    }
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

async function pollControlResponse(commandId, initialResponse) {
  const maxAttempts = 30; // Poll for up to 30 seconds
  let attempts = 0;
  
  const poll = async () => {
    try {
      const status = await fetchJSON(`/api/admin/control/${commandId}/status`);
      
      if (status.status === 'completed' || status.status === 'failed') {
        showControlResponse({
          success: status.status === 'completed',
          message: status.status === 'completed' ? 
            `Command completed successfully. Battery response: ${status.response || 'No response data'}` :
            `Command failed: ${status.error || 'Unknown error'}`,
          command: initialResponse.command,
          timestamp: status.sent_time,
          response: status.response
        });
        return;
      }
      
      attempts++;
      if (attempts < maxAttempts) {
        setTimeout(poll, 1000); // Poll every second
      } else {
        showControlResponse({
          success: false,
          message: 'Command timeout - no response from battery',
          command: initialResponse.command,
          timestamp: initialResponse.timestamp
        });
      }
    } catch (error) {
      console.error('Error polling control status:', error);
      showControlResponse({
        success: false,
        message: 'Error checking command status',
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  };
  
  // Show initial "sent" response
  showControlResponse({
    success: true,
    message: 'Command sent to battery, waiting for response...',
    command: initialResponse.command,
    timestamp: initialResponse.timestamp,
    status: 'waiting'
  });
  
  // Start polling
  setTimeout(poll, 1000);
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

async function viewHistory(deviceId) {
  try {
    historyTitle.textContent = `Battery History - ${deviceId}`;
    historyModal.style.display = 'block';
    
    // Show loading state
    document.querySelectorAll('.tab-content tbody').forEach(tbody => {
      tbody.innerHTML = '<tr><td colspan="10" style="text-align: center; padding: 20px;">Loading...</td></tr>';
    });
    
    const history = await fetchJSON(`/api/admin/battery/${deviceId}/history`);
    
    // Populate status data
    const statusTbody = document.getElementById('status-tbody');
    if (history.status && history.status.length > 0) {
      statusTbody.innerHTML = history.status.map(record => `
        <tr>
          <td>${new Date(record.time).toLocaleString()}</td>
          <td>${formatValue(record.current_amps)}</td>
          <td>${formatValue(record.current_type)}</td>
          <td>${formatValue(record.soc_percent, '%')}</td>
          <td>${formatValue(record.total_voltage_mv)}</td>
          <td>${formatValue(record.remaining_capacity_ah)}</td>
          <td>${formatValue(record.total_capacity_ah)}</td>
          <td>${formatValue(record.loop_cycles)}</td>
          <td>${formatValue(record.status_text)}</td>
        </tr>
      `).join('');
    } else {
      statusTbody.innerHTML = '<tr><td colspan="9" style="text-align: center; padding: 20px;">No status data available</td></tr>';
    }
    
    // Populate position data
    const positionTbody = document.getElementById('position-tbody');
    if (history.position && history.position.length > 0) {
      positionTbody.innerHTML = history.position.map(record => `
        <tr>
          <td>${new Date(record.time).toLocaleString()}</td>
          <td>${formatValue(record.lat)}</td>
          <td>${formatValue(record.lon)}</td>
          <td>${formatValue(record.direction)}</td>
          <td>${formatValue(record.sats_total)}</td>
          <td>${formatValue(record.sats_gps)}</td>
          <td>${formatValue(record.sats_beidou)}</td>
          <td>${formatValue(record.hemisphere)}</td>
        </tr>
      `).join('');
    } else {
      positionTbody.innerHTML = '<tr><td colspan="8" style="text-align: center; padding: 20px;">No position data available</td></tr>';
    }
    
    // Populate temperature data
    const temperaturesTbody = document.getElementById('temperatures-tbody');
    if (history.temperatures && history.temperatures.length > 0) {
      temperaturesTbody.innerHTML = history.temperatures.map(record => `
        <tr>
          <td>${new Date(record.time).toLocaleString()}</td>
          <td>${Array.isArray(record.bms_temps_c) ? record.bms_temps_c.join(', ') : formatValue(record.bms_temps_c)}</td>
          <td>${Array.isArray(record.cell_temps_c) ? record.cell_temps_c.join(', ') : formatValue(record.cell_temps_c)}</td>
        </tr>
      `).join('');
    } else {
      temperaturesTbody.innerHTML = '<tr><td colspan="3" style="text-align: center; padding: 20px;">No temperature data available</td></tr>';
    }
    
    // Populate cell data
    const cellsTbody = document.getElementById('cells-tbody');
    if (history.cells && history.cells.length > 0) {
      cellsTbody.innerHTML = history.cells.map(record => `
        <tr>
          <td>${new Date(record.time).toLocaleString()}</td>
          <td>${Array.isArray(record.cell_voltages_mv) ? record.cell_voltages_mv.join(', ') : formatValue(record.cell_voltages_mv)}</td>
        </tr>
      `).join('');
    } else {
      cellsTbody.innerHTML = '<tr><td colspan="2" style="text-align: center; padding: 20px;">No cell data available</td></tr>';
    }
    
    // Populate network data
    const networkTbody = document.getElementById('network-tbody');
    if (history.network && history.network.length > 0) {
      networkTbody.innerHTML = history.network.map(record => `
        <tr>
          <td>${new Date(record.time).toLocaleString()}</td>
          <td>${formatValue(record.rssi)}</td>
          <td>${formatValue(record.rsrp)}</td>
          <td>${formatValue(record.rsrq)}</td>
          <td>${formatValue(record.snr)}</td>
          <td>${formatValue(record.network_type)}</td>
        </tr>
      `).join('');
    } else {
      networkTbody.innerHTML = '<tr><td colspan="6" style="text-align: center; padding: 20px;">No network data available</td></tr>';
    }
    
  } catch (error) {
    console.error('Failed to load history:', error);
    alert(`Failed to load history for ${deviceId}: ${error.message}`);
  }
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
document.querySelectorAll('.modal-close').forEach(closeBtn => {
  closeBtn.addEventListener('click', (e) => {
    const modal = e.target.closest('.modal');
    modal.style.display = 'none';
  });
});

window.addEventListener('click', (e) => {
  if (e.target.classList.contains('modal')) {
    e.target.style.display = 'none';
  }
});

// History tab functionality
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', (e) => {
    const tabName = e.target.dataset.tab;
    
    // Update active tab button
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    e.target.classList.add('active');
    
    // Update active tab content
    document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
    document.getElementById(`history-${tabName}`).classList.add('active');
  });
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

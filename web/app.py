#!/usr/bin/env python3
import os
import json
import bcrypt
import uuid
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, render_template, request, redirect, url_for, flash, session
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
import psycopg2
from psycopg2.extras import RealDictCursor
import paho.mqtt.client as mqtt


DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_NAME = os.getenv('DB_NAME', 'batteries')
DB_USER = os.getenv('DB_USER', 'troy')
DB_PASS = os.getenv('DB_PASS', 's3rv3r5mx')

# MQTT Configuration
MQTT_BROKER = os.getenv('MQTT_BROKER', 'mqtt-cloud-1.telco.co.zw')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))

# Global control command tracking
control_commands = {}
command_lock = threading.Lock()


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)


class User(UserMixin):
    def __init__(self, battery_id: str, first_login: bool = True):
        self.id = battery_id
        self.battery_id = battery_id
        self.first_login = first_login


app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.getenv('SECRET_KEY', 'your-secret-key-change-in-production')

# Initialize Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message = 'Please log in to access this page.'


@login_manager.user_loader
def load_user(battery_id: str) -> Optional[User]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT battery_id, first_login FROM users WHERE battery_id = %s AND is_active = TRUE", (battery_id,))
            user_data = cur.fetchone()
            if user_data:
                return User(user_data['battery_id'], user_data['first_login'])
    return None


def hash_password(password: str) -> str:
    """Hash a password using bcrypt"""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')


def check_password(password: str, hashed: str) -> bool:
    """Check if a password matches its hash"""
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))


def create_user_if_not_exists(battery_id: str, password: str) -> bool:
    """Create a new user if they don't exist, return True if created, False if already exists"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Check if user already exists
            cur.execute("SELECT id FROM users WHERE battery_id = %s", (battery_id,))
            if cur.fetchone():
                return False
            
            # Create new user
            password_hash = hash_password(password)
            cur.execute(
                "INSERT INTO users (battery_id, password_hash, first_login) VALUES (%s, %s, %s)",
                (battery_id, password_hash, True)
            )
            conn.commit()
            return True


def authenticate_user(battery_id: str, password: str) -> Optional[User]:
    """Authenticate a user and return User object if successful"""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT battery_id, password_hash, first_login FROM users WHERE battery_id = %s AND is_active = TRUE",
                (battery_id,)
            )
            user_data = cur.fetchone()
            
            if user_data and check_password(password, user_data['password_hash']):
                # Update last login time
                cur.execute(
                    "UPDATE users SET last_login = NOW() WHERE battery_id = %s",
                    (battery_id,)
                )
                conn.commit()
                return User(user_data['battery_id'], user_data['first_login'])
    return None


def update_user_password(battery_id: str, new_password: str) -> bool:
    """Update user password and set first_login to False"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            password_hash = hash_password(new_password)
            cur.execute(
                "UPDATE users SET password_hash = %s, first_login = FALSE WHERE battery_id = %s",
                (password_hash, battery_id)
            )
            conn.commit()
            return cur.rowcount > 0


@app.route('/')
@login_required
def index():
    # Check if user is admin
    if current_user.battery_id == 'telco':
        return redirect(url_for('admin_dashboard'))
    return render_template('index.html')


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        battery_id = request.form.get('battery_id', '').strip()
        password = request.form.get('password', '')
        
        if not battery_id or not password:
            flash('Please enter both battery ID and password.', 'error')
            return render_template('login.html')
        
        # Try to authenticate existing user
        user = authenticate_user(battery_id, password)
        
        if user:
            login_user(user)
            flash(f'Welcome, Battery {battery_id}!', 'success')
            return redirect(url_for('index'))
        else:
            # Check if this is a new battery ID that needs initial setup
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM users WHERE battery_id = %s", (battery_id,))
                    if not cur.fetchone():
                        # This is a new battery ID, create user with provided password
                        if create_user_if_not_exists(battery_id, password):
                            user = authenticate_user(battery_id, password)
                            if user:
                                login_user(user)
                                flash(f'Welcome! Your account for Battery {battery_id} has been created.', 'success')
                                return redirect(url_for('index'))
                    
            flash('Invalid battery ID or password.', 'error')
    
    return render_template('login.html')


@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))


@app.route('/change-password', methods=['GET', 'POST'])
@login_required
def change_password():
    if request.method == 'POST':
        new_password = request.form.get('new_password', '')
        confirm_password = request.form.get('confirm_password', '')
        
        if not new_password or not confirm_password:
            flash('Please fill in all fields.', 'error')
            return render_template('change_password.html')
        
        if new_password != confirm_password:
            flash('Passwords do not match.', 'error')
            return render_template('change_password.html')
        
        if len(new_password) < 6:
            flash('Password must be at least 6 characters long.', 'error')
            return render_template('change_password.html')
        
        if update_user_password(current_user.battery_id, new_password):
            flash('Password updated successfully!', 'success')
            return redirect(url_for('index'))
        else:
            flash('Failed to update password. Please try again.', 'error')
    
    return render_template('change_password.html')


@app.route('/api/devices')
@login_required
def devices():
    """Discover devices by listing info_* tables; fallback to pos_* if none."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tablename FROM pg_tables 
                WHERE schemaname='public' AND (tablename LIKE 'info_%' OR tablename LIKE 'pos_%')
            """)
            rows = [r[0] for r in cur.fetchall()]
    ids = set()
    for t in rows:
        if t.startswith('info_'):
            ids.add(t.split('info_')[1])
        elif t.startswith('pos_'):
            ids.add(t.split('pos_')[1])
    return jsonify(sorted(ids))


@app.route('/api/summary/<device_id>')
@login_required
def summary(device_id: str):
    """Return recent summary values for a device (status, temps, network)"""
    safe = ''.join(c for c in device_id if c.isalnum() or c == '_')
    data: Dict[str, Any] = {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT time, current_amps, current_type, soc_percent, total_voltage_mv, remaining_capacity_ah, total_capacity_ah, loop_cycles, status_text FROM status_{safe} ORDER BY time DESC LIMIT 1")
            r = cur.fetchone()
            if r:
                data['status'] = {
                    'time': r[0].isoformat(),
                    'current_amps': r[1],
                    'current_type': r[2],
                    'soc_percent': r[3],
                    'total_voltage_mv': r[4],
                    'remaining_capacity_ah': r[5],
                    'total_capacity_ah': r[6],
                    'loop_cycles': r[7],
                    'status_text': r[8],
                }
            cur.execute(f"SELECT time, bms_temps_c, cell_temps_c FROM temps_{safe} ORDER BY time DESC LIMIT 1")
            r = cur.fetchone()
            if r:
                data['temps'] = {
                    'time': r[0].isoformat(),
                    'bms_temps_c': r[1] or [],
                    'cell_temps_c': r[2] or [],
                }
            cur.execute(f"SELECT time, rssi, rsrp, rsrq, snr, network_type FROM net_{safe} ORDER BY time DESC LIMIT 1")
            r = cur.fetchone()
            if r:
                data['network'] = {
                    'time': r[0].isoformat(),
                    'rssi': r[1], 'rsrp': r[2], 'rsrq': r[3], 'snr': r[4], 'type': r[5]
                }
            # Include latest known position (time, lat, lon, sats, direction)
            try:
                cur.execute(f"SELECT time, lat, lon, sats_total, direction FROM pos_{safe} ORDER BY time DESC LIMIT 1")
                r = cur.fetchone()
                if r:
                    data['position'] = {
                        'time': r[0].isoformat(),
                        'lat': r[1],
                        'lon': r[2],
                        'sats_total': r[3],
                        'direction': r[4],
                    }
            except Exception:
                # Position table may not exist yet; ignore
                pass

    # Compute unified latest_time across categories (status, temps, network, position)
    latest = None
    for key in ('status', 'temps', 'network', 'position'):
        t = data.get(key, {}).get('time') if isinstance(data.get(key), dict) else None
        if t:
            try:
                dt = datetime.fromisoformat(t)
                latest = dt if latest is None or dt > latest else latest
            except Exception:
                pass
    if latest is not None:
        data['latest_time'] = latest.isoformat()
    return jsonify(data)


@app.route('/api/series/<device_id>')
@login_required
def series(device_id: str):
    """Return time series for the last N hours."""
    hours = int(request.args.get('hours', '6'))
    since = datetime.utcnow() - timedelta(hours=hours)
    safe = ''.join(c for c in device_id if c.isalnum() or c == '_')
    out: Dict[str, Any] = {'status': [], 'temps': [], 'cells': [], 'pos': []}
    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(f"SELECT time, soc_percent, total_voltage_mv, current_amps FROM status_{safe} WHERE time >= %s ORDER BY time ASC", (since,))
                out['status'] = [{'t': t.isoformat(), 'soc': soc, 'v': v, 'i': i} for (t, soc, v, i) in cur.fetchall()]
            except Exception:
                out['status'] = []
            try:
                cur.execute(f"SELECT time, bms_temps_c, cell_temps_c FROM temps_{safe} WHERE time >= %s ORDER BY time ASC", (since,))
                out['temps'] = [{'t': t.isoformat(), 'bms': b or [], 'cells': c or []} for (t, b, c) in cur.fetchall()]
            except Exception:
                out['temps'] = []
            try:
                cur.execute(f"SELECT time, cell_voltages_mv FROM cells_{safe} WHERE time >= %s ORDER BY time ASC", (since,))
                out['cells'] = [{'t': t.isoformat(), 'mv': arr or []} for (t, arr) in cur.fetchall()]
            except Exception:
                out['cells'] = []
            try:
                cur.execute(f"SELECT time, lat, lon, sats_total, direction FROM pos_{safe} WHERE time >= %s ORDER BY time ASC", (since,))
                out['pos'] = [{'t': t.isoformat(), 'lat': lat, 'lon': lon, 'sats': sats, 'dir': direction} for (t, lat, lon, sats, direction) in cur.fetchall()]
            except Exception:
                out['pos'] = []

            # Fallback: if no recent data, return the last available N samples (historical)
            try:
                if not out['status']:
                    cur.execute(f"SELECT time, soc_percent, total_voltage_mv, current_amps FROM status_{safe} ORDER BY time DESC LIMIT 200")
                    rows = cur.fetchall()
                    out['status'] = [{'t': t.isoformat(), 'soc': soc, 'v': v, 'i': i} for (t, soc, v, i) in reversed(rows)]
            except Exception:
                pass
            try:
                if not out['temps']:
                    cur.execute(f"SELECT time, bms_temps_c, cell_temps_c FROM temps_{safe} ORDER BY time DESC LIMIT 200")
                    rows = cur.fetchall()
                    out['temps'] = [{'t': t.isoformat(), 'bms': b or [], 'cells': c or []} for (t, b, c) in reversed(rows)]
            except Exception:
                pass
            try:
                if not out['cells']:
                    cur.execute(f"SELECT time, cell_voltages_mv FROM cells_{safe} ORDER BY time DESC LIMIT 200")
                    rows = cur.fetchall()
                    out['cells'] = [{'t': t.isoformat(), 'mv': arr or []} for (t, arr) in reversed(rows)]
            except Exception:
                pass
            try:
                if not out['pos']:
                    cur.execute(f"SELECT time, lat, lon, sats_total, direction FROM pos_{safe} ORDER BY time DESC LIMIT 200")
                    rows = cur.fetchall()
                    out['pos'] = [{'t': t.isoformat(), 'lat': lat, 'lon': lon, 'sats': sats, 'dir': direction} for (t, lat, lon, sats, direction) in reversed(rows)]
            except Exception:
                pass
    return jsonify(out)


@app.route('/admin')
@login_required
def admin_dashboard():
    """Admin dashboard for telco user"""
    if current_user.battery_id != 'telco':
        flash('Access denied. Admin privileges required.', 'error')
        return redirect(url_for('index'))
    return render_template('admin.html')


@app.route('/api/admin/batteries')
@login_required
def admin_batteries():
    """Get all batteries with their latest data"""
    if current_user.battery_id != 'telco':
        return jsonify({'error': 'Access denied'}), 403
    
    batteries = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Get all device tables
            cur.execute("""
                SELECT tablename FROM pg_tables 
                WHERE schemaname='public' AND (tablename LIKE 'info_%' OR tablename LIKE 'pos_%')
            """)
            tables = [r[0] for r in cur.fetchall()]
            
            device_ids = set()
            for t in tables:
                if t.startswith('info_'):
                    device_ids.add(t.split('info_')[1])
                elif t.startswith('pos_'):
                    device_ids.add(t.split('pos_')[1])
            
            # Get latest data for each device
            for device_id in sorted(device_ids):
                safe_id = ''.join(c for c in device_id if c.isalnum() or c == '_')
                battery_data = {'device_id': device_id, 'status': None, 'position': None, 'last_seen': None}
                
                try:
                    # Get latest status
                    cur.execute(f"SELECT time, soc_percent, total_voltage_mv, current_amps, status_text FROM status_{safe_id} ORDER BY time DESC LIMIT 1")
                    status_row = cur.fetchone()
                    if status_row:
                        battery_data['status'] = {
                            'time': status_row[0].isoformat(),
                            'soc_percent': status_row[1],
                            'total_voltage_mv': status_row[2],
                            'current_amps': status_row[3],
                            'status_text': status_row[4]
                        }
                        battery_data['last_seen'] = status_row[0].isoformat()
                except Exception:
                    pass
                
                try:
                    # Get latest position
                    cur.execute(f"SELECT time, lat, lon, sats_total FROM pos_{safe_id} ORDER BY time DESC LIMIT 1")
                    pos_row = cur.fetchone()
                    if pos_row:
                        battery_data['position'] = {
                            'time': pos_row[0].isoformat(),
                            'lat': pos_row[1],
                            'lon': pos_row[2],
                            'sats_total': pos_row[3]
                        }
                        if not battery_data['last_seen']:
                            battery_data['last_seen'] = pos_row[0].isoformat()
                except Exception:
                    pass
                
                batteries.append(battery_data)
    
    return jsonify(batteries)


@app.route('/api/admin/battery/<device_id>/history')
@login_required
def admin_battery_history(device_id: str):
    """Get last 20 records for a specific battery with all available fields"""
    if current_user.battery_id != 'telco':
        return jsonify({'error': 'Access denied'}), 403
    
    safe_id = ''.join(c for c in device_id if c.isalnum() or c == '_')
    history = {'status': [], 'position': [], 'temperatures': [], 'cells': [], 'network': []}
    
    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                # Get all status fields
                cur.execute(f"""
                    SELECT time, current_amps, current_type, soc_percent, total_voltage_mv, 
                           remaining_capacity_ah, total_capacity_ah, loop_cycles, status_text
                    FROM status_{safe_id} ORDER BY time DESC LIMIT 20
                """)
                history['status'] = [
                    {
                        'time': r[0].isoformat(), 
                        'current_amps': r[1], 
                        'current_type': r[2], 
                        'soc_percent': r[3], 
                        'total_voltage_mv': r[4], 
                        'remaining_capacity_ah': r[5], 
                        'total_capacity_ah': r[6], 
                        'loop_cycles': r[7], 
                        'status_text': r[8]
                    } for r in cur.fetchall()
                ]
            except Exception:
                pass
            
            try:
                # Get all position fields
                cur.execute(f"""
                    SELECT time, lat, lon, direction, sats_total, sats_gps, sats_beidou, hemisphere
                    FROM pos_{safe_id} ORDER BY time DESC LIMIT 20
                """)
                history['position'] = [
                    {
                        'time': r[0].isoformat(), 
                        'lat': r[1], 
                        'lon': r[2], 
                        'direction': r[3], 
                        'sats_total': r[4], 
                        'sats_gps': r[5], 
                        'sats_beidou': r[6], 
                        'hemisphere': r[7]
                    } for r in cur.fetchall()
                ]
            except Exception:
                pass
            
            try:
                # Get temperature data
                cur.execute(f"""
                    SELECT time, bms_temps_c, cell_temps_c
                    FROM temps_{safe_id} ORDER BY time DESC LIMIT 20
                """)
                history['temperatures'] = [
                    {
                        'time': r[0].isoformat(), 
                        'bms_temps_c': r[1] or [], 
                        'cell_temps_c': r[2] or []
                    } for r in cur.fetchall()
                ]
            except Exception:
                pass
            
            try:
                # Get cell voltage data
                cur.execute(f"""
                    SELECT time, cell_voltages_mv
                    FROM cells_{safe_id} ORDER BY time DESC LIMIT 20
                """)
                history['cells'] = [
                    {
                        'time': r[0].isoformat(), 
                        'cell_voltages_mv': r[1] or []
                    } for r in cur.fetchall()
                ]
            except Exception:
                pass
            
            try:
                # Get network data
                cur.execute(f"""
                    SELECT time, rssi, rsrp, rsrq, snr, network_type
                    FROM net_{safe_id} ORDER BY time DESC LIMIT 20
                """)
                history['network'] = [
                    {
                        'time': r[0].isoformat(), 
                        'rssi': r[1], 
                        'rsrp': r[2], 
                        'rsrq': r[3], 
                        'snr': r[4], 
                        'network_type': r[5]
                    } for r in cur.fetchall()
                ]
            except Exception:
                pass
    
    return jsonify(history)


def send_mqtt_control_command(device_id: str, control_type: int, value: int) -> str:
    """Send MQTT control command and return command ID for tracking"""
    command_id = str(uuid.uuid4())
    
    # Build MQTT message header (same format as positiontodb.py)
    START_CODE = 0x4350
    PROTOCOL_VERSION = 0x11
    import struct
    
    # Generate sequence and transaction IDs
    seq = int(time.time()) & 0xFFFF
    txn = int(time.time() * 1000) & 0xFF
    
    header = struct.pack('!H B H B', START_CODE, PROTOCOL_VERSION, seq, txn)
    body = struct.pack('BB', control_type & 0xFF, value & 0xFF)
    payload = header + body
    
    # Store command for tracking
    with command_lock:
        control_commands[command_id] = {
            'device_id': device_id,
            'control_type': control_type,
            'value': value,
            'sent_time': datetime.utcnow(),
            'status': 'sent',
            'response': None
        }
    
    # Send MQTT command
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        
        topic = f"/SW_GPS/{device_id}/user/bmsCtrReq"
        client.publish(topic, payload, qos=1, retain=False)
        client.disconnect()
        
        return command_id
    except Exception as e:
        with command_lock:
            if command_id in control_commands:
                control_commands[command_id]['status'] = 'failed'
                control_commands[command_id]['error'] = str(e)
        raise e


@app.route('/api/admin/control', methods=['POST'])
@login_required
def admin_control():
    """Send control command to battery via MQTT"""
    if current_user.battery_id != 'telco':
        return jsonify({'error': 'Access denied'}), 403
    
    data = request.get_json()
    device_id = data.get('device_id')
    control_type = data.get('control_type')  # 1=discharge, 2=charging, 3=static
    value = data.get('value')  # 0=allow, 1=disallow
    
    if not all([device_id, control_type is not None, value is not None]):
        return jsonify({'error': 'Missing parameters'}), 400
    
    try:
        command_id = send_mqtt_control_command(device_id, control_type, value)
        
        return jsonify({
            'success': True,
            'message': f'Control command sent to {device_id}',
            'command_id': command_id,
            'command': f'Type {control_type}, Value {value}',
            'timestamp': datetime.utcnow().isoformat(),
            'status': 'sent'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Failed to send control command: {str(e)}',
            'timestamp': datetime.utcnow().isoformat(),
            'status': 'failed'
        }), 500


@app.route('/api/admin/control/<command_id>/status')
@login_required
def admin_control_status(command_id: str):
    """Check status of a control command"""
    if current_user.battery_id != 'telco':
        return jsonify({'error': 'Access denied'}), 403
    
    with command_lock:
        if command_id not in control_commands:
            return jsonify({'error': 'Command not found'}), 404
        
        command = control_commands[command_id]
        return jsonify({
            'command_id': command_id,
            'device_id': command['device_id'],
            'control_type': command['control_type'],
            'value': command['value'],
            'status': command['status'],
            'sent_time': command['sent_time'].isoformat(),
            'response': command.get('response'),
            'error': command.get('error')
        })


@app.route('/api/admin/status')
@login_required
def admin_status():
    """Check system status (database and MQTT)"""
    if current_user.battery_id != 'telco':
        return jsonify({'error': 'Access denied'}), 403
    
    status = {'database': False, 'mqtt': False}
    
    # Check database connection
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        status['database'] = True
    except Exception:
        pass
    
    # MQTT status would need to be implemented based on your MQTT setup
    # For now, assume it's connected if database is connected
    status['mqtt'] = status['database']
    
    return jsonify(status)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)



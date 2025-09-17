#!/usr/bin/env python3
import os
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List

from flask import Flask, jsonify, render_template, request
import psycopg2


DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_NAME = os.getenv('DB_NAME', 'batteries')
DB_USER = os.getenv('DB_USER', 'troy')
DB_PASS = os.getenv('DB_PASS', 's3rv3r5mx')


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)


app = Flask(__name__, static_folder='static', template_folder='templates')


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/devices')
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
    return jsonify(data)


@app.route('/api/series/<device_id>')
def series(device_id: str):
    """Return time series for the last N hours."""
    hours = int(request.args.get('hours', '6'))
    since = datetime.utcnow() - timedelta(hours=hours)
    safe = ''.join(c for c in device_id if c.isalnum() or c == '_')
    out: Dict[str, Any] = {'status': [], 'temps': [], 'cells': [], 'pos': []}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT time, soc_percent, total_voltage_mv, current_amps FROM status_{safe} WHERE time >= %s ORDER BY time ASC", (since,))
            out['status'] = [{'t': t.isoformat(), 'soc': soc, 'v': v, 'i': i} for (t, soc, v, i) in cur.fetchall()]
            cur.execute(f"SELECT time, bms_temps_c, cell_temps_c FROM temps_{safe} WHERE time >= %s ORDER BY time ASC", (since,))
            out['temps'] = [{'t': t.isoformat(), 'bms': b or [], 'cells': c or []} for (t, b, c) in cur.fetchall()]
            cur.execute(f"SELECT time, cell_voltages_mv FROM cells_{safe} WHERE time >= %s ORDER BY time ASC", (since,))
            out['cells'] = [{'t': t.isoformat(), 'mv': arr or []} for (t, arr) in cur.fetchall()]
            cur.execute(f"SELECT time, lat, lon, sats_total, direction FROM pos_{safe} WHERE time >= %s ORDER BY time ASC", (since,))
            out['pos'] = [{'t': t.isoformat(), 'lat': lat, 'lon': lon, 'sats': sats, 'dir': direction} for (t, lat, lon, sats, direction) in cur.fetchall()]
    return jsonify(out)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=False)



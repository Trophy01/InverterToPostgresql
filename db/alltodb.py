#!/usr/bin/env python3
"""
All to DB Orchestrator

- Discovers batteries on MQTT and ingests both telemetry and GPS position
  using existing db.telemetrytodb.TelemetryToDB and db.positiontodb.PositionToDB.
- Enforces a maximum concurrency of 5 active batteries at a time.
- Starts an SSH tunnel to the database if requested.
- Starts the Flask dashboard in web/app.py and prints its address.
- Prints when messages are received (topic + device + hex), throttling
  frequent telemetry prints to once every 30 seconds per device.

Usage:
  python -m db.alltodb --broker mqtt-cloud-1.telco.co.zw --limit 5

SSH Tunneling (optional):
  Provide --ssh-host, --ssh-user and optionally --ssh-key. The script will
  create: ssh -N -L <local_db_port>:<db_host>:<db_port> <ssh_user>@<ssh_host>
  You can override DB host/port with --db-host/--db-port used by both the
  tunnel and the dashboard.
"""

import argparse
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from typing import Dict, Optional, Set, Tuple

import paho.mqtt.client as mqtt

# Import existing workers without modifying them
from db.telemetrytodb import TelemetryToDB, DatabaseConfig as TDBConfig, AppConfig as TAppCfg
from db.positiontodb import PositionToDB, DatabaseConfig as PDBConfig, AppConfig as PAppCfg


class SSHTunnel:
    def __init__(self, ssh_host: Optional[str], ssh_user: Optional[str], ssh_key: Optional[str], db_host: str, db_port: int, local_port: int):
        self.ssh_host = ssh_host
        self.ssh_user = ssh_user
        self.ssh_key = ssh_key
        self.db_host = db_host
        self.db_port = db_port
        self.local_port = local_port
        self.proc: Optional[subprocess.Popen] = None

    def start(self) -> None:
        if not (self.ssh_host and self.ssh_user):
            return
        dest = f"{self.ssh_user}@{self.ssh_host}"
        forward = f"{self.local_port}:{self.db_host}:{self.db_port}"
        cmd = ["ssh", "-N", "-L", forward, dest]
        if self.ssh_key:
            cmd = ["ssh", "-i", self.ssh_key, "-N", "-L", forward, dest]
        # Run non-interactive in background
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
            except Exception:
                pass


class WebDashboard:
    def __init__(self, base_dir: str, db_env: Dict[str, str], host: str = "0.0.0.0", port: int = 5000):
        self.base_dir = base_dir
        self.db_env = db_env
        self.host = host
        self.port = port
        self.proc: Optional[subprocess.Popen] = None

    def start(self) -> None:
        env = os.environ.copy()
        env.update(self.db_env)
        # Launch web/app.py
        self.proc = subprocess.Popen([sys.executable, os.path.join(self.base_dir, "web", "app.py")], env=env)

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
            except Exception:
                pass


class DiscoveryMQTT:
    """Lightweight MQTT client to discover devices and print incoming topics.

    It does not write to DB. It only tracks devices and applies print throttling
    for verbose telemetry topics.
    """

    TELEMETRY_TYPES = {"batPropertyRprt"}

    def __init__(self, broker: str, port: int, on_new_device):
        self.broker = broker
        self.port = port
        self.on_new_device = on_new_device
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.seen_devices: Set[str] = set()
        self.last_telemetry_print: Dict[str, float] = {}

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            topic = "/SW_GPS/+/user/#"
            client.subscribe(topic, qos=1)
            print(f"✅ Discovery connected. Subscribed to {topic}")
        else:
            print(f"❌ Discovery connect failed: {reason_code}")

    @staticmethod
    def _extract_device(topic: str) -> str:
        parts = topic.split('/')
        return parts[2] if len(parts) >= 3 else 'unknown'

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        device = self._extract_device(topic)
        msg_type = topic.split('/')[-1]
        try:
            payload = msg.payload
            # Honor JSON {payload: hex} style
            hex_str = None
            try:
                txt = payload.decode('utf-8')
                if txt.strip().startswith('{') and 'payload' in txt:
                    import json as _json
                    obj = _json.loads(txt)
                    hex_str = (obj.get('payload') or '').strip()
            except Exception:
                pass
            if hex_str is None:
                try:
                    hex_str = payload.hex()
                except Exception:
                    hex_str = "<hex-error>"
        except Exception:
            hex_str = "<hex-error>"

        # Throttle telemetry prints to once per 30s per device
        if msg_type in self.TELEMETRY_TYPES:
            now = time.time()
            prev = self.last_telemetry_print.get(device, 0)
            if now - prev < 30:
                return
            self.last_telemetry_print[device] = now

        print(f"📥 MQTT | device={device} | type={msg_type} | hex={hex_str[:200]}{'...' if len(hex_str) > 200 else ''}")

        if device != 'unknown' and device not in self.seen_devices:
            self.seen_devices.add(device)
            self.on_new_device(device)

    def start(self):
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()


class IngestManager:
    def __init__(self, broker: str, port: int, db_host: str, db_port: int, db_name: str, db_user: str, db_password: str, limit: int = 5, verbose: bool = False):
        self.broker = broker
        self.port = port
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.limit = max(1, limit)
        self.verbose = verbose

        self.active: Dict[str, Tuple[threading.Thread, threading.Thread]] = {}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

    def can_start_more(self) -> bool:
        with self.lock:
            return len(self.active) < self.limit

    def start_for_device(self, device: str) -> None:
        with self.lock:
            if device in self.active or len(self.active) >= self.limit:
                return

        # Telemetry worker (device-filtered)
        def run_telemetry():
            db_cfg = TDBConfig(host=self.db_host, port=self.db_port, database=self.db_name, user=self.db_user, password=self.db_password)
            app_cfg = TAppCfg(verbose=self.verbose, capture_seconds=0, request_interval=3)
            app = TelemetryToDB(self.broker, self.port, db_cfg, app_cfg, device)
            try:
                app.run()
            except Exception as e:
                print(f"Telemetry worker for {device} exited: {e}")

        # Position worker (device-filtered)
        def run_position():
            db_cfg = PDBConfig(host=self.db_host, port=self.db_port, database=self.db_name, user=self.db_user, password=self.db_password)
            app_cfg = PAppCfg(verbose=self.verbose, broker=self.broker, port=self.port)
            app = PositionToDB(device, db_cfg, app_cfg, all_mode=False, silent=not self.verbose)
            try:
                app.run()
            except Exception as e:
                print(f"Position worker for {device} exited: {e}")

        t1 = threading.Thread(target=run_telemetry, name=f"telemetry-{device}", daemon=True)
        t2 = threading.Thread(target=run_position, name=f"position-{device}", daemon=True)
        t1.start()
        # Stagger startup slightly to avoid burst on broker
        time.sleep(0.5)
        t2.start()

        with self.lock:
            self.active[device] = (t1, t2)
            print(f"🚀 Started ingestion for {device} (active={len(self.active)}/{self.limit})")

    def stop_all(self) -> None:
        # Workers are daemonic and will exit when process ends; nothing to signal
        pass


def main() -> int:
    parser = argparse.ArgumentParser(description='Orchestrate telemetry+position ingestion, SSH tunnel, and dashboard')
    parser.add_argument('--broker', default='mqtt-cloud-1.telco.co.zw', help='MQTT broker host')
    parser.add_argument('--port', type=int, default=1883, help='MQTT broker port')

    parser.add_argument('--db-host', default='127.0.0.1', help='DB host (local forward target)')
    parser.add_argument('--db-port', type=int, default=5432, help='DB port (local forward target)')
    parser.add_argument('--db-name', default='batteries', help='DB name')
    parser.add_argument('--db-user', default='troy', help='DB user')
    parser.add_argument('--db-password', default='s3rv3r5mx', help='DB password')

    parser.add_argument('--ssh-host', help='SSH bastion host for DB tunnel (optional)')
    parser.add_argument('--ssh-user', help='SSH user for DB tunnel (optional)')
    parser.add_argument('--ssh-key', help='Path to SSH private key (optional)')
    parser.add_argument('--local-db-port', type=int, default=5432, help='Local forwarded DB port')

    parser.add_argument('--dashboard-host', default='0.0.0.0', help='Dashboard host bind')
    parser.add_argument('--dashboard-port', type=int, default=5000, help='Dashboard port')

    parser.add_argument('--limit', type=int, default=5, help='Max concurrent batteries')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logs for workers')
    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Start SSH tunnel if configured
    tunnel = SSHTunnel(args.ssh_host, args.ssh_user, args.ssh_key, args.db_host, args.db_port, args.local_db_port)
    tunnel.start()
    if args.ssh_host and args.ssh_user:
        print(f"🔐 SSH tunnel started: localhost:{args.local_db_port} -> {args.db_host}:{args.db_port} via {args.ssh_user}@{args.ssh_host}")

    # Start web dashboard
    db_env = {
        'DB_HOST': '127.0.0.1' if (args.ssh_host and args.ssh_user) else args.db_host,
        'DB_PORT': str(args.local_db_port if (args.ssh_host and args.ssh_user) else args.db_port),
        'DB_NAME': args.db_name,
        'DB_USER': args.db_user,
        'DB_PASS': args.db_password,
    }
    dashboard = WebDashboard(base_dir, db_env, host=args.dashboard_host, port=args.dashboard_port)
    dashboard.start()
    print(f"🖥️ Dashboard: http://127.0.0.1:{args.dashboard_port}")

    # Ingestion manager with concurrency control
    manager = IngestManager(
        broker=args.broker,
        port=args.port,
        db_host=db_env['DB_HOST'],
        db_port=int(db_env['DB_PORT']),
        db_name=args.db_name,
        db_user=args.db_user,
        db_password=args.db_password,
        limit=args.limit,
        verbose=args.verbose,
    )

    # Discovery client will trigger on_new_device
    def on_new_device(device_id: str):
        if manager.can_start_more():
            manager.start_for_device(device_id)
        else:
            print(f"⏳ Device {device_id} discovered but at concurrency limit ({args.limit}). Waiting...")

    discovery = DiscoveryMQTT(args.broker, args.port, on_new_device)
    discovery.start()

    # Graceful shutdown
    stop = threading.Event()

    def handle_sig(signum, frame):
        stop.set()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    try:
        while not stop.is_set():
            time.sleep(1)
    finally:
        print("Shutting down...")
        discovery.stop()
        manager.stop_all()
        dashboard.stop()
        tunnel.stop()

    return 0


if __name__ == '__main__':
    sys.exit(main())

#!/usr/bin/env python3
"""
All to DB - Combined Battery Data Collector

Combines functionality of positiontodb.py and telemetrytodb.py to collect
all battery data (position and telemetry) with battery limit management,
SSH tunnel creation, and web dashboard integration.

Usage:
  python -m db.alltodb
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict

import paho.mqtt.client as mqtt
import psycopg2
import struct

# Import the definitive GPS decoder from the main Gps_decoder.py
sys.path.append('/home/trophy/BatteryMonitoring2')
from Gps_decoder import DefinitiveGPSDecoder


# --- Configuration Classes ---

@dataclass
class DatabaseConfig:
    host: str = "127.0.0.1"
    port: int = 5432
    database: str = "batteries"
    user: str = "troy"
    password: str = "s3rv3r5mx"

    @property
    def dsn(self) -> str:
        return f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"


@dataclass
class AppConfig:
    verbose: bool = False
    broker: str = "mqtt-cloud-1.telco.co.zw"
    port: int = 1883
    max_batteries: int = 5
    telemetry_log_interval: int = 30  # seconds
    position_ping_interval: int = 10   # seconds (give batteries time to respond)
    ssh_tunnel_host: str = "sa@154.119.80.42"
    ssh_tunnel_port: int = 5432
    web_app_port: int = 5000


# --- Data Processing Classes ---

class BatteryDataProcessor:
    @staticmethod
    def sanitize_identifier(identifier: str) -> str:
        return ''.join(c for c in identifier if c.isalnum() or c == '_')

    @staticmethod
    def parse_float_prefix(text: Optional[str]) -> Optional[float]:
        if not text:
            return None
        try:
            token = str(text).split()[0].replace('%', '')
            return float(token)
        except Exception:
            return None

    @staticmethod
    def parse_int_prefix(text: Optional[str]) -> Optional[int]:
        val = BatteryDataProcessor.parse_float_prefix(text)
        return int(val) if val is not None else None

    @staticmethod
    def safe_timestamp(candidate: Optional[datetime] = None) -> datetime:
        now = datetime.now(timezone.utc)
        if candidate is None:
            return now
        if candidate.year < 2020 or candidate.year > 2035:
            return now
        if candidate.tzinfo is None:
            candidate = candidate.replace(tzinfo=timezone.utc)
        return candidate


# --- Database Manager ---

class DatabaseManager:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def get_connection(self):
        class _ConnCtx:
            def __init__(self, outer):
                self.outer = outer
                self.conn = None
            def __enter__(self):
                self.conn = psycopg2.connect(self.outer.config.dsn)
                self.conn.autocommit = True
                return self.conn
            def __exit__(self, exc_type, exc_val, exc_tb):
                try:
                    if exc_type is not None and self.conn:
                        self.conn.rollback()
                finally:
                    if self.conn:
                        self.conn.close()
        return _ConnCtx(self)

    def create_all_tables(self, device_id: str):
        safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
        
        # Position table
        pos_table = f"pos_{safe_id}"
        pos_schema = """
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                time TIMESTAMPTZ NOT NULL,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION,
                direction DOUBLE PRECISION,
                sats_total INTEGER,
                sats_gps INTEGER,
                sats_beidou INTEGER,
                hemisphere TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS {table}_time_idx ON {table} (time);
        """
        
        # Status table
        status_table = f"status_{safe_id}"
        status_schema = """
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                time TIMESTAMPTZ NOT NULL,
                current_amps DOUBLE PRECISION,
                current_type TEXT,
                soc_percent DOUBLE PRECISION,
                total_voltage_mv INTEGER,
                remaining_capacity_ah DOUBLE PRECISION,
                total_capacity_ah DOUBLE PRECISION,
                loop_cycles INTEGER,
                status_text TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS {table}_time_idx ON {table} (time);
        """
        
        # Temperature table
        temps_table = f"temps_{safe_id}"
        temps_schema = """
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                time TIMESTAMPTZ NOT NULL,
                bms_temps_c INTEGER[],
                cell_temps_c INTEGER[],
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS {table}_time_idx ON {table} (time);
        """
        
        # Cell voltages table
        cells_table = f"cells_{safe_id}"
        cells_schema = """
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                time TIMESTAMPTZ NOT NULL,
                cell_voltages_mv INTEGER[],
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS {table}_time_idx ON {table} (time);
        """
        
        # Network table
        net_table = f"net_{safe_id}"
        net_schema = """
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                time TIMESTAMPTZ NOT NULL,
                rssi INTEGER,
                rsrp INTEGER,
                rsrq INTEGER,
                snr INTEGER,
                network_type TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS {table}_time_idx ON {table} (time);
        """
        
        # Device info table
        info_table = f"info_{safe_id}"
        info_schema = """
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                product_sn TEXT,
                gps_sn TEXT,
                gps_imsi TEXT,
                gps_imei TEXT,
                gps_sw TEXT,
                gps_hw TEXT,
                bms_sn TEXT,
                bms_sw TEXT,
                bms_hw TEXT
            );
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(pos_schema.format(table=pos_table))
                cur.execute(status_schema.format(table=status_table))
                cur.execute(temps_schema.format(table=temps_table))
                cur.execute(cells_schema.format(table=cells_table))
                cur.execute(net_schema.format(table=net_table))
                cur.execute(info_schema.format(table=info_table))

    def insert_position(self, device_id: str, position_data: Dict[str, Any]):
        safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
        table = f"pos_{safe_id}"
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {table}
                        (time, lat, lon, direction, sats_total, sats_gps, sats_beidou, hemisphere)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        position_data.get('utc_time'),
                        position_data.get('latitude'),
                        position_data.get('longitude'),
                        position_data.get('direction'),
                        position_data.get('satellites_total'),
                        position_data.get('satellites_gps'),
                        position_data.get('satellites_beidou'),
                        position_data.get('hemisphere'),
                    ),
                )

    def insert_status(self, device_id: str, status_data: Dict[str, Any]):
        safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
        table = f"status_{safe_id}"
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {table}
                        (time, current_amps, current_type, soc_percent, total_voltage_mv,
                         remaining_capacity_ah, total_capacity_ah, loop_cycles, status_text)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        status_data.get('timestamp'),
                        status_data.get('current'),
                        status_data.get('current_type'),
                        status_data.get('soc'),
                        status_data.get('total_voltage_mv'),
                        status_data.get('remaining_capacity_ah'),
                        status_data.get('total_capacity_ah'),
                        status_data.get('loop_cycles'),
                        status_data.get('status_text'),
                    ),
                )

    def insert_temperatures(self, device_id: str, temp_data: Dict[str, Any]):
        safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
        table = f"temps_{safe_id}"
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {table} (time, bms_temps_c, cell_temps_c)
                        VALUES (%s, %s, %s)""",
                    (
                        temp_data.get('timestamp'),
                        temp_data.get('bms_temps'),
                        temp_data.get('cell_temps'),
                    ),
                )

    def insert_cells(self, device_id: str, cell_data: Dict[str, Any]):
        safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
        table = f"cells_{safe_id}"
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {table} (time, cell_voltages_mv)
                        VALUES (%s, %s)""",
                    (
                        cell_data.get('timestamp'),
                        cell_data.get('cell_voltages_mv'),
                    ),
                )

    def insert_network(self, device_id: str, network_data: Dict[str, Any]):
        safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
        table = f"net_{safe_id}"
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {table}
                        (time, rssi, rsrp, rsrq, snr, network_type)
                        VALUES (%s, %s, %s, %s, %s, %s)""",
                    (
                        network_data.get('timestamp'),
                        network_data.get('rssi'),
                        network_data.get('rsrp'),
                        network_data.get('rsrq'),
                        network_data.get('snr'),
                        network_data.get('network_type'),
                    ),
                )

    def insert_device_info(self, device_id: str, info_data: Dict[str, Any]):
        safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
        table = f"info_{safe_id}"
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {table}")
                cur.execute(
                    f"""INSERT INTO {table}
                        (product_sn, gps_sn, gps_imsi, gps_imei, gps_sw, gps_hw,
                         bms_sn, bms_sw, bms_hw)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        info_data.get('product_sn'),
                        info_data.get('gps_sn'),
                        info_data.get('gps_imsi'),
                        info_data.get('gps_imei'),
                        info_data.get('gps_sw'),
                        info_data.get('gps_hw'),
                        info_data.get('bms_sn'),
                        info_data.get('bms_sw'),
                        info_data.get('bms_hw'),
                    ),
                )


# --- Inline Decoder (Combined from both scripts) ---

class InlineDecoder:
    START_CODE = 0x4350
    PROTOCOL_VERSION = 0x11
    HEADER_SIZE = 6

    @staticmethod
    def build_header(seq: int, txn: int) -> bytes:
        return struct.pack('!H B H B', InlineDecoder.START_CODE, InlineDecoder.PROTOCOL_VERSION, seq & 0xFFFF, txn & 0xFF)

    @staticmethod
    def decode_payload_bytes(msg_payload: bytes) -> bytes:
        try:
            txt = msg_payload.decode('utf-8')
            if txt.strip().startswith('{'):
                obj = json.loads(txt)
                hex_str = obj.get('payload', '').strip()
                return bytes.fromhex(hex_str) if hex_str else msg_payload
            else:
                return bytes.fromhex(txt.strip())
        except Exception:
            return msg_payload

    @staticmethod
    def parse_header(payload: bytes) -> Dict[str, Any]:
        if len(payload) < InlineDecoder.HEADER_SIZE:
            raise ValueError("Payload shorter than header size")
        start_code, proto_ver, seq, txn = struct.unpack('!H B H B', payload[:InlineDecoder.HEADER_SIZE])
        return {
            'start_code': start_code,
            'protocol_version': proto_ver,
            'sequence_number': seq,
            'transaction_id': txn,
            'valid': start_code == InlineDecoder.START_CODE and proto_ver == InlineDecoder.PROTOCOL_VERSION,
        }

    @staticmethod
    def extract_device_id(topic: str) -> str:
        parts = topic.split('/')
        return parts[2] if len(parts) >= 3 else 'unknown'

    @staticmethod
    def decode_battery_status(status_bytes: bytes) -> Dict[str, Any]:
        status_int = int.from_bytes(status_bytes, byteorder='big')
        bits = {
            'charging_status':   (status_int >> 0) & 1,
            'full_state':        (status_int >> 1) & 1,
            'charge_overcurrent':(status_int >> 2) & 1,
            'cell_overvoltage':  (status_int >> 3) & 1,
            'discharge_state':   (status_int >> 4) & 1,
            'short_circuit_alarm': (status_int >> 5) & 1,
            'discharge_overflow':  (status_int >> 6) & 1,
            'cell_undervoltage':   (status_int >> 7) & 1,
            'cell_open_circuit':   (status_int >> 8) & 1,
            'temp_detect_open':    (status_int >> 9) & 1,
            'cell_high_temp':      (status_int >> 10) & 1,
            'cell_low_temp':       (status_int >> 11) & 1,
            'bms_high_temp':       (status_int >> 12) & 1,
            'reserved_bit13':      (status_int >> 13) & 1,
            'no_charge':           (status_int >> 14) & 1,
            'no_discharge':        (status_int >> 15) & 1,
            'discharge_mos_failed':(status_int >> 16) & 1,
            'charging_mos_status': (status_int >> 17) & 1,
            'discharge_mos_status':(status_int >> 18) & 1,
            'charge_mos_failure':  (status_int >> 19) & 1,
            'high_low_voltage_failure': (status_int >> 20) & 1,
            'ultra_high_temp_failure':  (status_int >> 21) & 1,
            'cell_pressure_difference': (status_int >> 22) & 1,
            'battery_temp_difference':  (status_int >> 23) & 1,
        }
        active_alarms = []
        if bits['charge_overcurrent']: active_alarms.append('Charge overcurrent')
        if bits['cell_overvoltage']: active_alarms.append('Cell overvoltage')
        if bits['short_circuit_alarm']: active_alarms.append('Short circuit')
        if bits['discharge_overflow']: active_alarms.append('Discharge overflow')
        if bits['cell_undervoltage']: active_alarms.append('Cell undervoltage')
        if bits['cell_open_circuit']: active_alarms.append('Cell detection line open circuit')
        if bits['temp_detect_open']: active_alarms.append('Temperature detection line open')
        if bits['cell_high_temp']: active_alarms.append('Cell high temperature')
        if bits['cell_low_temp']: active_alarms.append('Cell low temperature')
        if bits['bms_high_temp']: active_alarms.append('BMS high temperature')
        if bits['discharge_mos_failed']: active_alarms.append('Discharge MOS failed')
        if bits['charge_mos_failure']: active_alarms.append('Charge MOS failure')
        if bits['high_low_voltage_failure']: active_alarms.append('High/low voltage cell failure')
        if bits['ultra_high_temp_failure']: active_alarms.append('Ultra high temperature failure')
        if bits['cell_pressure_difference']: active_alarms.append('Large cell pressure difference')
        if bits['battery_temp_difference']: active_alarms.append('Large battery temperature difference')
        descriptions = {
            'charging': 'Currently charging' if bits['charging_status'] else 'Not charging',
            'discharging': 'Currently discharging' if bits['discharge_state'] else 'Not discharging',
            'charging_switch': 'Charging switch ON' if bits['charging_mos_status'] else 'Charging switch OFF',
            'discharge_switch': 'Discharge switch ON' if bits['discharge_mos_status'] else 'Discharge switch OFF',
            'charging_allowed': 'Charging prohibited' if bits['no_charge'] else 'Charging allowed',
            'discharging_allowed': 'Discharging prohibited' if bits['no_discharge'] else 'Discharging allowed',
        }
        return {
            'raw': status_int,
            'bits': bits,
            'descriptions': descriptions,
            'active_alarms': active_alarms,
            'has_alarms': len(active_alarms) > 0,
        }

    @staticmethod
    def decode_battery_property_report(payload: bytes, device_id: str) -> Optional[Dict[str, Any]]:
        try:
            header = InlineDecoder.parse_header(payload)
            body = payload[InlineDecoder.HEADER_SIZE:]
            offset = 0
            if offset >= len(body):
                return None
            sn_length = body[offset]
            offset += 1
            if offset + sn_length > len(body):
                return None
            sn_bytes = body[offset:offset+sn_length]
            if all(b == 0xFF for b in sn_bytes):
                product_sn = 'UNKNOWN_SN'
            else:
                try:
                    product_sn = sn_bytes.decode('ascii', errors='replace')
                except Exception:
                    product_sn = 'HEX:' + sn_bytes.hex()
            offset += sn_length
            if offset + 4 > len(body):
                return None
            status_bytes = body[offset:offset+4]
            battery_status = InlineDecoder.decode_battery_status(status_bytes)
            offset += 4
            if offset >= len(body):
                return None
            battery_data_length = body[offset]
            offset += 1
            if offset + battery_data_length > len(body):
                return None
            if offset + 2 > len(body):
                return None
            current = int.from_bytes(body[offset:offset+2], 'big', signed=True) / 10
            offset += 2
            if offset >= len(body):
                return None
            cell_vol_len = body[offset]
            offset += 1
            if offset + cell_vol_len > len(body):
                return None
            cell_voltages = []
            for _ in range(cell_vol_len // 2):
                if offset + 2 > len(body):
                    break
                cell_voltages.append(int.from_bytes(body[offset:offset+2], 'big'))
                offset += 2
            if offset >= len(body):
                return None
            bms_temp_len = body[offset]
            offset += 1
            if offset + bms_temp_len > len(body):
                return None
            bms_temps = []
            for _ in range(bms_temp_len):
                if offset >= len(body):
                    break
                t = body[offset]
                if t > 127:
                    t -= 256
                bms_temps.append(t)
                offset += 1
            if offset >= len(body):
                return None
            cell_temp_len = body[offset]
            offset += 1
            if offset + cell_temp_len > len(body):
                return None
            cell_temps = []
            for _ in range(cell_temp_len):
                if offset >= len(body):
                    break
                t = body[offset]
                if t > 127:
                    t -= 256
                cell_temps.append(t)
                offset += 1
            if offset + 2 > len(body):
                return None
            loop_cycles = int.from_bytes(body[offset:offset+2], 'big')
            offset += 2
            if offset + 2 > len(body):
                return None
            remaining_capacity = int.from_bytes(body[offset:offset+2], 'big') / 10
            offset += 2
            if offset + 2 > len(body):
                return None
            total_capacity = int.from_bytes(body[offset:offset+2], 'big') / 10
            offset += 2
            network_status: Dict[str, Any] = {}
            if offset < len(body):
                rssi = body[offset]
                network_status['rssi'] = None if rssi in (0, 255) else -int(rssi)
                offset += 1
                if offset + 6 <= len(body):
                    network_status['plmn'] = body[offset:offset+6].decode('ascii', errors='replace')
                    offset += 6
                if offset + 2 <= len(body):
                    network_status['lac'] = body[offset:offset+2].hex()
                    offset += 2
                if offset + 4 <= len(body):
                    network_status['cell_id'] = body[offset:offset+4].hex()
                    offset += 4
                if offset < len(body):
                    network_status['rat'] = body[offset]
                    offset += 1
            soc_percentage = (remaining_capacity / total_capacity) * 100 if total_capacity > 0 else 0
            total_voltage = sum(cell_voltages) if cell_voltages else 0
            return {
                'Device ID': device_id if product_sn == 'UNKNOWN_SN' else product_sn,
                'Product SN': product_sn,
                'Battery Status': battery_status,
                'Current': f"{current} A",
                'Current_Type': 'Charging' if current > 0 else 'Discharging' if current < 0 else 'Idle',
                'Cell Voltages': cell_voltages,
                'Total Battery Voltage': f"{total_voltage} mV",
                'Cell Voltage Min': min(cell_voltages) if cell_voltages else 0,
                'Cell Voltage Max': max(cell_voltages) if cell_voltages else 0,
                'Cell Voltage Avg': (sum(cell_voltages)/len(cell_voltages)) if cell_voltages else 0,
                'Cell Voltage Diff': (max(cell_voltages)-min(cell_voltages)) if cell_voltages else 0,
                'BMS Temperatures': bms_temps,
                'Cell Temperatures': cell_temps,
                'Loop Cycles': loop_cycles,
                'Remaining Capacity': f"{remaining_capacity} Ah",
                'Total Capacity': f"{total_capacity} Ah",
                'State of Charge': f"{soc_percentage:.1f}%",
                'Network Status': network_status,
                'Battery Data Length': battery_data_length,
            }
        except Exception as e:
            logging.error(f"Error decoding property report: {e}")
            return None

    @staticmethod
    def decode_battery_property_ext(payload: bytes, device_id: str) -> Optional[Dict[str, Any]]:
        try:
            _ = InlineDecoder.parse_header(payload)
            body = payload[InlineDecoder.HEADER_SIZE:]
            offset = 0
            def read_lv() -> Optional[str]:
                nonlocal offset
                if offset >= len(body):
                    return None
                ln = body[offset]
                offset += 1
                if offset + ln > len(body):
                    return None
                data = body[offset:offset+ln]
                offset += ln
                if all(b == 0xFF for b in data):
                    return None
                try:
                    return data.decode('ascii', errors='replace')
                except Exception:
                    return 'HEX:' + data.hex()
            product_sn = read_lv() or 'UNKNOWN_SN'
            gps_sn = read_lv() or 'UNKNOWN_GPS_SN'
            gps_imsi = read_lv() or 'UNKNOWN_GPS_IMSI'
            gps_imei = read_lv() or 'UNKNOWN_GPS_IMEI'
            gps_sw = read_lv() or 'UNKNOWN_GPS_SW_VER'
            gps_hw = read_lv() or 'UNKNOWN_GPS_HW_VER'
            bms_sn = read_lv() or 'UNKNOWN_BMS_SN'
            bms_sw = read_lv()
            bms_hw = read_lv()
            return {
                'Device ID': device_id,
                'Product SN': product_sn,
                'GPS SN': gps_sn,
                'GPS IMSI': gps_imsi,
                'GPS IMEI': gps_imei,
                'GPS Software Version': gps_sw,
                'GPS Hardware Version': gps_hw,
                'BMS SN': bms_sn,
                'BMS Software Version': bms_sw,
                'BMS Hardware Version': bms_hw,
            }
        except Exception as e:
            logging.error(f"Error decoding property ext: {e}")
            return None

    @staticmethod
    def decode_battery_position(payload: bytes, device_id: str) -> Optional[Dict[str, Any]]:
        try:
            header = InlineDecoder.parse_header(payload)
            body = payload[InlineDecoder.HEADER_SIZE:]
            if not body:
                return {'header': header, 'position_count': 0, 'positions': [], 'raw_payload_hex': payload.hex()}
            
            # Use the imported DefinitiveGPSDecoder from Gps_decoder.py
            gps_decoder = DefinitiveGPSDecoder()
            
            # Try protocol-specific parsing first
            proto_result = gps_decoder.parse_position_info_protocol(payload)
            if proto_result:
                # Use protocol result
                latitude = proto_result['lat']
                longitude = proto_result['lon']
                time_data = proto_result['time']
                hour = time_data[0]
                minute = time_data[1]
                second = time_data[2] if len(time_data) > 2 else 0
                beidou_sat = proto_result.get('bd_sats', 0)
                gps_sat = proto_result.get('gps_sats', 0)
                speed_kmh = proto_result.get('speed_kmh', 0.0)
                direction_deg = proto_result.get('direction_deg', 0.0)
                date_data = proto_result.get('date', (1, 1, 24))
                day, month, year = date_data
                
                # Create timestamp string
                timestamp = f"{2000+year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"
            else:
                # Fallback to coordinate scanning methods
                coord_result = gps_decoder.decode_coordinates_search(payload)
                if coord_result is None:
                    coord_result = gps_decoder.decode_coordinates_bcd_scan(payload)
                
                if coord_result is None:
                    return {'header': header, 'position_count': 0, 'positions': [], 'raw_payload_hex': payload.hex()}
                
                latitude = coord_result['latitude']
                longitude = coord_result['longitude']
                
                # Try to decode time
                time_result = gps_decoder.decode_time_search(payload)
                if time_result:
                    hour = time_result['hour']
                    minute = time_result['minute']
                    second = 0
                else:
                    # Use current time as fallback
                    now = datetime.now()
                    hour = now.hour
                    minute = now.minute
                    second = now.second
                
                # Default values for other fields
                beidou_sat = 0
                gps_sat = 0
                speed_kmh = 0.0
                direction_deg = 0.0
                now = datetime.now()
                timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
            
            # Apply hemisphere corrections based on flags if available
            if proto_result and 'flags' in proto_result:
                flags = proto_result['flags']
                # Treat only bit7 (0x80) as West; others ignored
                west = bool(flags & 0x80)
                south = bool(flags & 0x08 or flags & 0x02 or flags & 0x01)
                # Latitude: S -> negative
                latitude = -abs(latitude) if south else abs(latitude)
                # Longitude: only set negative if W bit; otherwise force positive (East)
                longitude = -abs(longitude) if west else abs(longitude)
            else:
                # Default hemisphere handling
                latitude = -abs(latitude)  # Assume South
                longitude = abs(longitude)  # Assume East
            
            satellites = beidou_sat + gps_sat
            
            positions = [{
                'latitude': latitude,
                'longitude': longitude,
                'timestamp': timestamp,
                'timestamp_utc': '',
                'beidou_satellites': beidou_sat,
                'gps_satellites': gps_sat,
                'satellites': satellites,
                'speed_kmh': speed_kmh,
                'direction_deg': direction_deg,
                'is_west': longitude < 0,
                'is_south': latitude < 0,
            }]
            return {'header': header, 'position_count': len(positions), 'positions': positions, 'raw_payload_hex': payload.hex()}
        except Exception as e:
            logging.error(f"Error decoding battery position: {e}")
            return None


# --- SSH Tunnel Manager ---

class SSHTunnelManager:
    def __init__(self, ssh_host: str, local_port: int, remote_port: int = 5432):
        self.ssh_host = ssh_host
        self.local_port = local_port
        self.remote_port = remote_port
        self.process = None
        self.logger = logging.getLogger(__name__)

    def start_tunnel(self) -> bool:
        """Start SSH tunnel in background with password authentication"""
        try:
            # Check if sshpass is available
            try:
                subprocess.run(['sshpass', '-V'], capture_output=True, check=True)
            except (subprocess.CalledProcessError, FileNotFoundError):
                self.logger.error("sshpass is not installed. Please install it with: sudo apt-get install sshpass")
                return False
            
            # Use sshpass for password authentication
            cmd = [
                'sshpass', '-p', 's3rv3r5mx$',
                'ssh', '-N', '-L', f'{self.local_port}:127.0.0.1:{self.remote_port}',
                '-o', 'StrictHostKeyChecking=no',  # Skip host key verification
                '-o', 'UserKnownHostsFile=/dev/null',  # Don't save host keys
                self.ssh_host
            ]
            self.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            time.sleep(3)  # Give tunnel more time to establish with password auth
            if self.process.poll() is None:
                self.logger.info(f"SSH tunnel established: localhost:{self.local_port} -> {self.ssh_host}:{self.remote_port}")
                return True
            else:
                # Check for error output
                stdout, stderr = self.process.communicate()
                self.logger.error(f"SSH tunnel failed to start. stderr: {stderr.decode()}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to start SSH tunnel: {e}")
            return False

    def stop_tunnel(self):
        """Stop SSH tunnel"""
        if self.process:
            self.process.terminate()
            self.process.wait()
            self.logger.info("SSH tunnel stopped")


# --- Web App Manager ---

class WebAppManager:
    def __init__(self, port: int = 5000):
        self.port = port
        self.process = None
        self.logger = logging.getLogger(__name__)

    def start_web_app(self) -> bool:
        """Start web application in background"""
        try:
            web_dir = '/home/trophy/BatteryMonitoring2/web'
            cmd = ['python3', 'app.py']
            self.process = subprocess.Popen(
                cmd, 
                cwd=web_dir,
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE
            )
            time.sleep(3)  # Give web app time to start
            if self.process.poll() is None:
                self.logger.info(f"Web application started on http://localhost:{self.port}")
                print(f"🌐 Dashboard available at: http://localhost:{self.port}")
                return True
            else:
                self.logger.error("Web application failed to start")
                return False
        except Exception as e:
            self.logger.error(f"Failed to start web application: {e}")
            return False

    def stop_web_app(self):
        """Stop web application"""
        if self.process:
            self.process.terminate()
            self.process.wait()
            self.logger.info("Web application stopped")


# --- Main Application Class ---

class AllToDB:
    def __init__(self, db_cfg: DatabaseConfig, app_cfg: AppConfig, known_device: str = None):
        self.db_cfg = db_cfg
        self.app_cfg = app_cfg
        self.known_device = known_device
        self.db = DatabaseManager(db_cfg)
        self.proc = BatteryDataProcessor()
        self.decoder = InlineDecoder()
        self.ssh_tunnel = SSHTunnelManager(app_cfg.ssh_tunnel_host, app_cfg.ssh_tunnel_port)
        self.web_app = WebAppManager(app_cfg.web_app_port)
        
        # Battery management
        self.active_batteries: Set[str] = set()
        self.seen_batteries: Set[str] = set()
        self.battery_last_telemetry: Dict[str, float] = defaultdict(float)
        self._ext_requested: Set[str] = set()  # Track which devices we've requested ext info from
        
        # MQTT client setup
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        # Sequence counters
        self._seq = 0
        self._txn = 0
        
        # Logging setup
        level = logging.DEBUG if app_cfg.verbose else logging.INFO
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('alltodb.log')
            ],
        )
        self.logger = logging.getLogger(__name__)

    def next_header(self) -> bytes:
        self._seq = (self._seq + 1) & 0xFFFF
        self._txn = (self._txn + 1) & 0xFF
        return self.decoder.build_header(self._seq, self._txn)

    def _send_header_only(self, device_id: str, request_type: str) -> None:
        """Send header-only request to device (like original script)"""
        header = self.next_header()
        topic = f"/SW_GPS/{device_id}/user/{request_type}"
        self.client.publish(topic, payload=header, qos=1, retain=False)
        self.logger.info(f"📤 Sent {request_type} to {device_id} | hex: {header.hex()}")

    def _send_control_command(self, device_id: str, ctrl_type: int, value: int) -> None:
        """Send control command to wake up battery (like decoder.py)"""
        header = self.next_header()
        body = struct.pack("BB", ctrl_type & 0xFF, value & 0xFF)
        topic = f"/SW_GPS/{device_id}/user/bmsCtrReq"
        self.client.publish(topic, payload=header + body, qos=1, retain=False)
        self.logger.info(f"📤 Sent bmsCtrReq to {device_id} | type={ctrl_type}, value={value} | hex: {(header + body).hex()}")

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            self.logger.info("✅ Connected to MQTT broker")
            # Subscribe to all battery topics (like original script)
            topic = "/SW_GPS/#"
            client.subscribe(topic, qos=1)
            self.logger.info(f"📡 Subscribed to {topic}")
            
            # Add a known device ID to start monitoring
            if self.known_device and self.known_device not in self.active_batteries:
                self.active_batteries.add(self.known_device)
                self.seen_batteries.add(self.known_device)
                self.logger.info(f"🔋 Added known device {self.known_device} to active monitoring")
        else:
            self.logger.error(f"MQTT connection failed: {reason_code}")

    def on_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            device_id = self.decoder.extract_device_id(topic)
            msg_type = topic.split('/')[-1]
            
            # Add to seen batteries
            self.seen_batteries.add(device_id)
            
            # Manage active battery limit
            if device_id not in self.active_batteries:
                if len(self.active_batteries) < self.app_cfg.max_batteries:
                    self.active_batteries.add(device_id)
                    self.logger.info(f"🔋 Added battery {device_id} to active monitoring (total: {len(self.active_batteries)})")
                else:
                    # Skip processing if we're at the limit
                    return
            
            payload_bytes = self.decoder.decode_payload_bytes(msg.payload)
            hex_payload = payload_bytes.hex()
            
            # Log data reception with timing control
            current_time = time.time()
            should_log = True
            
            if msg_type in ('batPropertyRprt', 'batPropertyRsp'):
                # Only log telemetry every 30 seconds
                last_log = self.battery_last_telemetry.get(device_id, 0)
                if current_time - last_log < self.app_cfg.telemetry_log_interval:
                    should_log = False
                else:
                    self.battery_last_telemetry[device_id] = current_time
            
            if should_log:
                print(f"📊 Received {msg_type} from battery {device_id} | hex: {hex_payload[:120]}{'...' if len(hex_payload) > 120 else ''}")
            
            # Auto-request device info for new devices (like original script)
            if device_id not in self._ext_requested and msg_type not in ("batPropertyExtRprt", "batPropertyExtRsp"):
                try:
                    self._send_header_only(device_id, 'batPropertyExtReq')
                    self._ext_requested.add(device_id)
                    self.logger.info(f"📤 Auto-requested device info for {device_id}")
                except Exception as e:
                    self.logger.warning(f"Auto ext request failed for {device_id}: {e}")
            
            # Process different message types
            if msg_type in ('batPropertyRprt', 'batPropertyRsp'):
                self._handle_property_report(device_id, payload_bytes)
            elif msg_type in ('batPropertyExtRprt', 'batPropertyExtRsp'):
                self._handle_property_ext(device_id, payload_bytes)
            elif msg_type in ('batPositonRprt', 'batPositionRprt'):
                self._handle_position_report(device_id, payload_bytes)
                
        except Exception as e:
            self.logger.error(f"Message handling error: {e}")

    def _handle_property_report(self, device_id: str, payload: bytes):
        decoded = self.decoder.decode_battery_property_report(payload, device_id)
        if not decoded:
            return
        
        now = datetime.now(timezone.utc)
        status = decoded.get('Battery Status', {}) or {}
        has_alarms = bool(status.get('has_alarms'))
        status_text = 'No active alarms' if not has_alarms else ', '.join(status.get('active_alarms', []))
        
        # Prepare data for database
        status_row = {
            'timestamp': now,
            'current': self.proc.parse_float_prefix(decoded.get('Current')),
            'current_type': decoded.get('Current_Type'),
            'soc': self.proc.parse_float_prefix(decoded.get('State of Charge')),
            'total_voltage_mv': self.proc.parse_int_prefix(decoded.get('Total Battery Voltage')),
            'remaining_capacity_ah': self.proc.parse_float_prefix(decoded.get('Remaining Capacity')),
            'total_capacity_ah': self.proc.parse_float_prefix(decoded.get('Total Capacity')),
            'loop_cycles': decoded.get('Loop Cycles'),
            'status_text': status_text,
        }
        
        temps_row = {
            'timestamp': now,
            'bms_temps': decoded.get('BMS Temperatures', []),
            'cell_temps': decoded.get('Cell Temperatures', []),
        }
        
        cell_voltages = decoded.get('Cell Voltages', []) or []
        cells_row = None
        if cell_voltages:
            cells_row = {'timestamp': now, 'cell_voltages_mv': cell_voltages}
        
        network = decoded.get('Network Status', {}) or {}
        network_row = None
        if network:
            network_row = {
                'timestamp': now,
                'rssi': network.get('rssi'),
                'rsrp': network.get('rsrp'),
                'rsrq': network.get('rsrq'),
                'snr': network.get('snr'),
                'network_type': network.get('rat'),
            }
        
        # Store in database
        try:
            self.db.create_all_tables(device_id)
            self.db.insert_status(device_id, status_row)
            self.db.insert_temperatures(device_id, temps_row)
            if cells_row:
                self.db.insert_cells(device_id, cells_row)
            if network_row:
                self.db.insert_network(device_id, network_row)
            
            # Enhanced logging with data details
            current_val = status_row.get('current', 'N/A')
            soc_val = status_row.get('soc', 'N/A')
            voltage_val = status_row.get('total_voltage_mv', 'N/A')
            print(f"💾 DATABASE: Stored telemetry for {device_id} | Current: {current_val}A | SOC: {soc_val}% | Voltage: {voltage_val}mV")
            self.logger.info(f"💾 Stored telemetry for {device_id} - Current: {current_val}A, SOC: {soc_val}%, Voltage: {voltage_val}mV")
        except Exception as e:
            print(f"❌ DATABASE ERROR: Failed to store telemetry for {device_id}: {e}")
            self.logger.error(f"Database error storing telemetry for {device_id}: {e}")

    def _handle_property_ext(self, device_id: str, payload: bytes):
        decoded = self.decoder.decode_battery_property_ext(payload, device_id)
        if not decoded:
            return
        
        info_row = {
            'product_sn': decoded.get('Product SN'),
            'gps_sn': decoded.get('GPS SN'),
            'gps_imsi': decoded.get('GPS IMSI'),
            'gps_imei': decoded.get('GPS IMEI'),
            'gps_sw': decoded.get('GPS Software Version'),
            'gps_hw': decoded.get('GPS Hardware Version'),
            'bms_sn': decoded.get('BMS SN'),
            'bms_sw': decoded.get('BMS Software Version'),
            'bms_hw': decoded.get('BMS Hardware Version'),
        }
        
        try:
            self.db.create_all_tables(device_id)
            self.db.insert_device_info(device_id, info_row)
            product_sn = info_row.get('product_sn', 'N/A')
            gps_sn = info_row.get('gps_sn', 'N/A')
            print(f"💾 DATABASE: Stored device info for {device_id} | Product SN: {product_sn} | GPS SN: {gps_sn}")
            self.logger.info(f"💾 Stored device info for {device_id} - Product SN: {product_sn}, GPS SN: {gps_sn}")
        except Exception as e:
            print(f"❌ DATABASE ERROR: Failed to store device info for {device_id}: {e}")
            self.logger.error(f"Database error storing device info for {device_id}: {e}")

    def _handle_position_report(self, device_id: str, payload: bytes):
        decoded = self.decoder.decode_battery_position(payload, device_id)
        if not decoded:
            return
        
        for pos in decoded.get('positions', []):
            # Use current timestamp (UTC) for DB writes
            safe_time = datetime.now(timezone.utc)
            raw_direction = pos.get('direction_deg')
            sats_total = pos.get('satellites')

            # Always store a numeric direction
            direction = 0.0
            try:
                cand = raw_direction
                if isinstance(cand, str):
                    cleaned = ''.join(ch for ch in cand if (ch.isdigit() or ch in '.-'))
                    cand = float(cleaned) if cleaned else None
                if isinstance(cand, (int, float)):
                    direction = float(cand) % 360
            except Exception:
                direction = 0.0
            
            position_data = {
                'utc_time': safe_time,
                'latitude': pos.get('latitude'),
                'longitude': pos.get('longitude'),
                'direction': direction,
                'satellites_total': sats_total,
                'satellites_gps': pos.get('gps_satellites'),
                'satellites_beidou': pos.get('beidou_satellites'),
                'hemisphere': f"{'S' if pos.get('is_south') else 'N'}{'W' if pos.get('is_west') else 'E'}",
            }
            
            try:
                self.db.create_all_tables(device_id)
                self.db.insert_position(device_id, position_data)
                lat = position_data.get('latitude', 'N/A')
                lon = position_data.get('longitude', 'N/A')
                sats = position_data.get('satellites_total', 'N/A')
                print(f"💾 DATABASE: Stored position for {device_id} | Lat: {lat} | Lon: {lon} | Satellites: {sats}")
                self.logger.info(f"💾 Stored position for {device_id}: lat={lat}, lon={lon}, satellites={sats}")
            except Exception as e:
                print(f"❌ DATABASE ERROR: Failed to store position for {device_id}: {e}")
                self.logger.error(f"Database error storing position for {device_id}: {e}")

    def send_ping_requests(self):
        """Send ping requests to active batteries (like original scripts)"""
        for device_id in list(self.active_batteries):
            try:
                # Send multiple request types like positiontodb.py
                self._send_header_only(device_id, 'batPropertyExtReq')
                time.sleep(0.2)
                self._send_header_only(device_id, 'batPropertyReq')
                time.sleep(0.2)
                self._send_header_only(device_id, 'batPositionReq')
                time.sleep(0.2)
                self._send_header_only(device_id, 'batPositonReq')
                time.sleep(0.2)
            except Exception as e:
                self.logger.warning(f"Ping request error for {device_id}: {e}")
        
        # Note: We can't send broadcast pings with wildcards in MQTT publish topics
        # Instead, we rely on the wildcard subscriptions to catch any new devices

    def run(self) -> int:
        """Main application loop"""
        try:
            # Test database connection first
            print("🗄️ Testing database connection...")
            try:
                with self.db.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        result = cur.fetchone()
                        if result:
                            print("✅ Database connection successful")
                        else:
                            print("❌ Database connection failed")
                            return 1
            except Exception as e:
                print(f"❌ Database connection error: {e}")
                return 1
            
            # Start SSH tunnel
            print("🔗 Starting SSH tunnel...")
            if not self.ssh_tunnel.start_tunnel():
                print("❌ Failed to start SSH tunnel")
                return 1
            
            # Start web application
            print("🌐 Starting web dashboard...")
            if not self.web_app.start_web_app():
                print("❌ Failed to start web application")
                self.ssh_tunnel.stop_tunnel()
                return 1
            
            # Connect to MQTT
            print("📡 Connecting to MQTT broker...")
            self.client.connect(self.app_cfg.broker, self.app_cfg.port, 60)
            # Start request loop in separate thread (like original script)
            stop_event = threading.Event()
            def request_loop():
                while not stop_event.is_set():
                    try:
                        self.send_ping_requests()
                    except Exception as e:
                        self.logger.warning(f"Request loop error: {e}")
                    stop_event.wait(self.app_cfg.position_ping_interval)
            
            t = threading.Thread(target=request_loop, daemon=True)
            t.start()
            
            self.client.loop_start()
            
            print(f"🚀 AllToDB started successfully!")
            print(f"📊 Monitoring up to {self.app_cfg.max_batteries} batteries")
            print(f"🔋 Active batteries: {len(self.active_batteries)}")
            print(f"👀 Seen batteries: {len(self.seen_batteries)}")
            print("Press Ctrl+C to stop...")
            
            # Main loop
            try:
                status_counter = 0
                while True:
                    time.sleep(1.0)  # Check every second
                    status_counter += 1
                    # Print status update every 10 seconds
                    if status_counter >= 10 and len(self.seen_batteries) > 0:
                        print(f"🔋 Status: {len(self.active_batteries)} active, {len(self.seen_batteries)} total seen")
                        status_counter = 0
                        
            except KeyboardInterrupt:
                print("\n🛑 Shutting down...")
                stop_event.set()
                
        except Exception as e:
            self.logger.error(f"Application error: {e}")
            return 1
        finally:
            # Cleanup
            self.client.loop_stop()
            self.client.disconnect()
            self.web_app.stop_web_app()
            self.ssh_tunnel.stop_tunnel()
            print("✅ Cleanup completed")
        
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description='All-in-one battery data collector with web dashboard')
    parser.add_argument('--broker', default='mqtt-cloud-1.telco.co.zw', help='MQTT broker host')
    parser.add_argument('--port', type=int, default=1883, help='MQTT broker port')
    parser.add_argument('--max-batteries', type=int, default=5, help='Maximum number of batteries to monitor')
    parser.add_argument('--known-device', default='862317043590129', help='Known device ID to start monitoring')
    parser.add_argument('--db-host', default='127.0.0.1', help='DB host (via SSH tunnel)')
    parser.add_argument('--db-port', type=int, default=5432, help='DB port (via SSH tunnel)')
    parser.add_argument('--db-name', default='batteries', help='DB name')
    parser.add_argument('--db-user', default='troy', help='DB user')
    parser.add_argument('--db-password', default='s3rv3r5mx', help='DB password')
    parser.add_argument('--ssh-host', default='sa@154.119.80.42', help='SSH tunnel host')
    parser.add_argument('--web-port', type=int, default=5000, help='Web dashboard port')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    args = parser.parse_args()

    db_cfg = DatabaseConfig(
        host=args.db_host, 
        port=args.db_port, 
        database=args.db_name, 
        user=args.db_user, 
        password=args.db_password
    )
    
    app_cfg = AppConfig(
        verbose=args.verbose,
        broker=args.broker,
        port=args.port,
        max_batteries=args.max_batteries,
        ssh_tunnel_host=args.ssh_host,
        web_app_port=args.web_port
    )
    
    app = AllToDB(db_cfg, app_cfg, args.known_device)
    return app.run()


if __name__ == '__main__':
    sys.exit(main())


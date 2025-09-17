#!/usr/bin/env python3
"""
Telemetry to DB (non-position)

Subscribes to MQTT topics and stores battery telemetry into PostgreSQL using
the same schema used by the existing system. Only non-position data is written:
- batPropertyRprt → status, temps, cells, network
- batPropertyExtRprt/Rsp → device info

Usage examples:
  python -m db.telemetrytodb --device DEV123
  python -m db.telemetrytodb --broker mqtt-cloud-1.telco.co.zw --db-user troy --db-password s3rv3r5mx
"""

import argparse
import json
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt
import psycopg2
import struct


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
    capture_seconds: int = 0
    request_interval: int = 3
    max_retries: int = 3
    connection_timeout: int = 60


class BatteryDataProcessor:
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
    def sanitize_identifier(identifier: str) -> str:
        return ''.join(c for c in identifier if c.isalnum() or c == '_')


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

    def create_device_tables(self, device_id: str):
        safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
        tables = {
            f"pos_{safe_id}": (
                """
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
            ),
            f"status_{safe_id}": (
                """
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
            ),
            f"temps_{safe_id}": (
                """
                CREATE TABLE IF NOT EXISTS {table} (
                    id SERIAL PRIMARY KEY,
                    time TIMESTAMPTZ NOT NULL,
                    bms_temps_c INTEGER[],
                    cell_temps_c INTEGER[],
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS {table}_time_idx ON {table} (time);
                """
            ),
            f"cells_{safe_id}": (
                """
                CREATE TABLE IF NOT EXISTS {table} (
                    id SERIAL PRIMARY KEY,
                    time TIMESTAMPTZ NOT NULL,
                    cell_voltages_mv INTEGER[],
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS {table}_time_idx ON {table} (time);
                """
            ),
            f"net_{safe_id}": (
                """
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
            ),
            f"info_{safe_id}": (
                """
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
            ),
        }
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                for tname, schema in tables.items():
                    cur.execute(schema.format(table=tname))

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


class InlineDecoder:
    START_CODE = 0x4350
    PROTOCOL_VERSION = 0x11
    HEADER_SIZE = 6

    @staticmethod
    def extract_device_id(topic: str) -> str:
        parts = topic.split('/')
        return parts[2] if len(parts) >= 3 else 'unknown'

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
    def build_header(seq: int, txn: int) -> bytes:
        return struct.pack('!H B H B', InlineDecoder.START_CODE, InlineDecoder.PROTOCOL_VERSION, seq & 0xFFFF, txn & 0xFF)

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


class TelemetryToDB:
    def __init__(self, broker: str, port: int, db_cfg: DatabaseConfig, app_cfg: AppConfig, device: Optional[str]):
        self.broker = broker
        self.port = port
        self.db_cfg = db_cfg
        self.app_cfg = app_cfg
        self.device_filter = device

        self.logger = logging.getLogger(__name__)
        level = logging.DEBUG if app_cfg.verbose else logging.INFO
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('telemetrytodb.log')
            ],
        )

        self.db = DatabaseManager(self.db_cfg)
        self.decoder = InlineDecoder()
        self.proc = BatteryDataProcessor()

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

        self._ext_requested = set()

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            topic = f"/SW_GPS/{self.device_filter}/#" if self.device_filter else "/SW_GPS/#"
            client.subscribe(topic, qos=1)
            self.logger.info(f"Connected. Subscribed to {topic}")
        else:
            self.logger.error(f"MQTT connection failed: {reason_code}")

    def on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        self.logger.warning(f"MQTT disconnected: {reason_code}")

    def on_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            device_id = self.decoder.extract_device_id(topic)
            if self.device_filter and device_id != self.device_filter:
                return
            msg_type = topic.split('/')[-1]
            payload_bytes = self.decoder.decode_payload_bytes(msg.payload)
            try:
                in_hex = payload_bytes.hex()
            except Exception:
                in_hex = "<hex-error>"
            self.logger.info(f"INCOMING | device={device_id} | type={msg_type} | hex={in_hex}")
            if device_id not in self._ext_requested and msg_type not in ("batPropertyExtRprt", "batPropertyExtRsp"):
                try:
                    self._send_header_only(device_id, 'batPropertyExtReq')
                    self._ext_requested.add(device_id)
                except Exception as e:
                    self.logger.warning(f"Auto ext request failed for {device_id}: {e}")
            if msg_type == 'batPropertyRprt':
                self._handle_property_report(device_id, payload_bytes)
            elif msg_type in ('batPropertyExtRprt', 'batPropertyExtRsp'):
                self._handle_property_ext(device_id, payload_bytes)
            else:
                pass
        except Exception as e:
            self.logger.error(f"Message handling error: {e}")

    def _send_header_only(self, device_id: str, request_type: str) -> None:
        if not hasattr(self, '_seq'):
            self._seq = 0
        if not hasattr(self, '_txn'):
            self._txn = 0
        self._seq = (self._seq + 1) & 0xFFFF
        self._txn = (self._txn + 1) & 0xFF
        header = self.decoder.build_header(self._seq, self._txn)
        topic = f"/SW_GPS/{device_id}/user/{request_type}"
        self.client.publish(topic, payload=header, qos=1, retain=False)
        self.logger.info(f"OUTGOING | {topic} | {header.hex()} | seq={self._seq} txn={self._txn}")

    def _handle_property_report(self, device_id: str, payload: bytes) -> None:
        decoded = self.decoder.decode_battery_property_report(payload, device_id)
        if not decoded:
            return
        now = datetime.now(timezone.utc)
        status = decoded.get('Battery Status', {}) or {}
        has_alarms = bool(status.get('has_alarms'))
        status_text = 'No active alarms' if not has_alarms else ', '.join(status.get('active_alarms', []))
        status_row: Dict[str, Any] = {
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
        temps_row: Dict[str, Any] = {
            'timestamp': now,
            'bms_temps': decoded.get('BMS Temperatures', []),
            'cell_temps': decoded.get('Cell Temperatures', []),
        }
        cell_voltages = decoded.get('Cell Voltages', []) or []
        cells_row: Optional[Dict[str, Any]] = None
        if cell_voltages:
            cells_row = {'timestamp': now, 'cell_voltages_mv': cell_voltages}
        network = decoded.get('Network Status', {}) or {}
        network_row: Optional[Dict[str, Any]] = None
        if network:
            network_row = {
                'timestamp': now,
                'rssi': network.get('rssi'),
                'rsrp': network.get('rsrp'),
                'rsrq': network.get('rsrq'),
                'snr': network.get('snr'),
                'network_type': network.get('rat'),
            }
        self.db.create_device_tables(device_id)
        self.db.insert_status(device_id, status_row)
        self.db.insert_temperatures(device_id, temps_row)
        if cells_row:
            self.db.insert_cells(device_id, cells_row)
        if network_row:
            self.db.insert_network(device_id, network_row)
        self.logger.info(f"Stored telemetry for {device_id} (status/temps{', cells' if cells_row else ''}{', net' if network_row else ''})")

    def _handle_property_ext(self, device_id: str, payload: bytes) -> None:
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
        self.db.create_device_tables(device_id)
        self.db.insert_device_info(device_id, info_row)
        self.logger.info(f"Stored device info for {device_id}")

    def run(self) -> int:
        try:
            self.client.connect(self.broker, self.port, 60)
        except Exception as e:
            self.logger.error(f"Failed to connect to MQTT: {e}")
            return 1
        stop_event = threading.Event()
        def request_loop():
            while not stop_event.is_set():
                try:
                    targets = [self.device_filter] if self.device_filter else []
                    for dev in targets:
                        if not dev:
                            continue
                        self._send_header_only(dev, 'batPropertyReq')
                        time.sleep(0.4)
                except Exception as e:
                    self.logger.warning(f"Request loop error: {e}")
                stop_event.wait(self.app_cfg.request_interval)
        t = threading.Thread(target=request_loop, daemon=True)
        t.start()
        self.client.loop_start()
        try:
            end_ts = time.time() + self.app_cfg.capture_seconds if self.app_cfg.capture_seconds else None
            while True:
                time.sleep(0.5)
                if end_ts and time.time() >= end_ts:
                    break
        except KeyboardInterrupt:
            pass
        finally:
            stop_event.set()
            self.client.loop_stop()
            self.client.disconnect()
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description='Ingest MQTT telemetry (non-position) to DB')
    parser.add_argument('--broker', default='mqtt-cloud-1.telco.co.zw', help='MQTT broker host')
    parser.add_argument('--port', type=int, default=1883, help='MQTT broker port')
    parser.add_argument('--device', help='Only process this device ID (optional)')
    parser.add_argument('--db-host', default='127.0.0.1', help='DB host')
    parser.add_argument('--db-port', type=int, default=5432, help='DB port')
    parser.add_argument('--db-name', default='batteries', help='DB name')
    parser.add_argument('--db-user', default='troy', help='DB user')
    parser.add_argument('--db-password', default='s3rv3r5mx', help='DB password')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    parser.add_argument('--capture-time', type=int, default=0, help='Run seconds then exit (0=forever)')
    parser.add_argument('--request-interval', type=int, default=3, help='Seconds between optional pings')
    args = parser.parse_args()
    db_cfg = DatabaseConfig(host=args.db_host, port=args.db_port, database=args.db_name, user=args.db_user, password=args.db_password)
    app_cfg = AppConfig(verbose=args.verbose, capture_seconds=args.capture_time, request_interval=args.request_interval)
    app = TelemetryToDB(args.broker, args.port, db_cfg, app_cfg, args.device)
    return app.run()


if __name__ == '__main__':
    sys.exit(main())



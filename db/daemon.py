#!/usr/bin/env python3
"""
Battery MQTT Data Collector
Collects battery telemetry data from MQTT and stores in PostgreSQL
"""

import argparse
import json
import logging
import os
import struct
import sys
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from contextlib import contextmanager

import paho.mqtt.client as mqtt
import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
import psycopg2
import psycopg2.extras
from psycopg2 import sql


# Database Configuration
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


# MQTT Configuration
@dataclass
class MQTTConfig:
    broker: str = "mqtt-cloud-1.telco.co.zw"
    port: int = 1883
    topic_base: str = "/SW_GPS"
    client_id: str = "battery_collector"


# Application Configuration
@dataclass
class AppConfig:
    verbose: bool = False
    capture_seconds: int = 210  # 3.5 minutes
    request_interval: int = 3   # seconds between request cycles
    max_retries: int = 3
    connection_timeout: int = 60


class BatteryDataProcessor:
    """Handles processing and validation of battery data"""
    
    @staticmethod
    def sanitize_identifier(identifier: str) -> str:
        """Sanitize database identifiers"""
    return ''.join(c for c in identifier if c.isalnum() or c == '_')

    @staticmethod
    def parse_float_prefix(text: Optional[str]) -> Optional[float]:
        """Parse float from string prefix, removing % signs"""
        if not text:
            return None
        try:
            token = str(text).split()[0].replace('%', '')
            return float(token)
        except (ValueError, IndexError):
            return None
    
    @staticmethod
    def parse_int_prefix(text: Optional[str]) -> Optional[int]:
        """Parse int from string prefix"""
        val = BatteryDataProcessor.parse_float_prefix(text)
        return int(val) if val is not None else None
    
    @staticmethod
    def safe_timestamp(candidate: Optional[datetime] = None) -> datetime:
        """Return a safe UTC timestamp within reasonable bounds"""
        now = datetime.now(timezone.utc)
        if candidate is None:
            return now
        
        # Sanity checks
        if candidate.year < 2020 or candidate.year > 2035:
            return now
        if candidate - now > timedelta(days=1):
            return now
        
        # Ensure timezone aware
        if candidate.tzinfo is None:
            candidate = candidate.replace(tzinfo=timezone.utc)
        
        return candidate
    
    @staticmethod
    def parse_gps_timestamp(pos_data: Dict[str, Any]) -> datetime:
        """Parse GPS timestamp from position data"""
        now = datetime.now(timezone.utc)
        
        def is_valid(dt: datetime) -> bool:
            return (2020 <= dt.year <= 2035 and 
                   abs((dt - now).total_seconds()) <= 6 * 3600)
        
        # Try explicit UTC timestamp
        ts_utc = pos_data.get('timestamp_utc')
        if isinstance(ts_utc, str):
            try:
                dt = datetime.strptime(ts_utc, "%Y-%m-%d %H:%M:%S")
                dt = dt.replace(tzinfo=timezone.utc)
                if is_valid(dt):
                    return dt
            except ValueError:
                pass
        
        # Try date + time
        date_str = pos_data.get('date')
        time_str = pos_data.get('time')
        if isinstance(date_str, str) and isinstance(time_str, str):
            try:
                dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
                dt = dt.replace(tzinfo=timezone.utc)
                if is_valid(dt):
                    return dt
            except ValueError:
                pass
        
        return now


class DatabaseManager:
    """Handles database operations and schema management"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(self.config.dsn)
            conn.autocommit = True
            yield conn
        except psycopg2.Error as e:
            self.logger.error(f"Database error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def create_device_tables(self, device_id: str):
        """Create all required tables for a device"""
        safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
        
        tables = {
            f"pos_{safe_id}": """
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
            """,
            f"status_{safe_id}": """
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
            """,
            f"temps_{safe_id}": """
                CREATE TABLE IF NOT EXISTS {table} (
                    id SERIAL PRIMARY KEY,
                time TIMESTAMPTZ NOT NULL,
                bms_temps_c INTEGER[],
                    cell_temps_c INTEGER[],
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS {table}_time_idx ON {table} (time);
            """,
            f"cells_{safe_id}": """
                CREATE TABLE IF NOT EXISTS {table} (
                    id SERIAL PRIMARY KEY,
                time TIMESTAMPTZ NOT NULL,
                    cell_voltages_mv INTEGER[],
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS {table}_time_idx ON {table} (time);
            """,
            f"net_{safe_id}": """
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
            """,
            f"info_{safe_id}": """
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
        }
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                for table_name, schema in tables.items():
                    cur.execute(schema.format(table=table_name))
    conn.commit()
            self.logger.debug(f"Created tables for device {device_id}")

    def truncate_device_tables(self, device_id: str):
        """Truncate all device tables and reset identities (safe and idempotent)."""
        safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
        table_names = [
            f"pos_{safe_id}",
            f"status_{safe_id}",
            f"temps_{safe_id}",
            f"cells_{safe_id}",
            f"net_{safe_id}",
            f"info_{safe_id}",
        ]
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Ensure tables exist first
                self.create_device_tables(device_id)
                # Truncate individually to tolerate missing ones in older schemas
                for t in table_names:
                    try:
                        cur.execute(f"TRUNCATE TABLE {t} RESTART IDENTITY")
                    except Exception as e:
                        self.logger.warning(f"TRUNCATE failed for {t}: {e}")
                self.logger.info(f"Truncated device tables for {device_id}")
    
    def insert_position(self, device_id: str, position_data: Dict[str, Any]):
        """Insert position data"""
        safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
        table_name = f"pos_{safe_id}"
        
        with self.get_connection() as conn:
            self.create_device_tables(device_id)
        with conn.cursor() as cur:
            cur.execute(
                    f"""INSERT INTO {table_name} 
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
                        position_data.get('hemisphere')
                    )
                )
            conn.commit()
    
    def insert_status(self, device_id: str, status_data: Dict[str, Any]):
        """Insert status data"""
        safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
        table_name = f"status_{safe_id}"
        
        with self.get_connection() as conn:
            self.create_device_tables(device_id)
        with conn.cursor() as cur:
            cur.execute(
                    f"""INSERT INTO {table_name} 
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
                        status_data.get('status_text')
                    )
                )
            conn.commit()
    
    def insert_temperatures(self, device_id: str, temp_data: Dict[str, Any]):
        """Insert temperature data"""
        safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
        table_name = f"temps_{safe_id}"
        
        with self.get_connection() as conn:
            self.create_device_tables(device_id)
        with conn.cursor() as cur:
            cur.execute(
                    f"""INSERT INTO {table_name} (time, bms_temps_c, cell_temps_c) 
                        VALUES (%s, %s, %s)""",
                    (
                        temp_data.get('timestamp'),
                        temp_data.get('bms_temps'),
                        temp_data.get('cell_temps')
                    )
                )
            conn.commit()
    
    def insert_cells(self, device_id: str, cell_data: Dict[str, Any]):
        """Insert cell voltage data"""
        safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
        table_name = f"cells_{safe_id}"
        
        with self.get_connection() as conn:
            self.create_device_tables(device_id)
        with conn.cursor() as cur:
            cur.execute(
                    f"""INSERT INTO {table_name} (time, cell_voltages_mv) 
                        VALUES (%s, %s)""",
                    (
                        cell_data.get('timestamp'),
                        cell_data.get('cell_voltages_mv')
                    )
                )
            conn.commit()
    
    def insert_network(self, device_id: str, network_data: Dict[str, Any]):
        """Insert network data"""
        safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
        table_name = f"net_{safe_id}"
        
        with self.get_connection() as conn:
            self.create_device_tables(device_id)
        with conn.cursor() as cur:
            cur.execute(
                    f"""INSERT INTO {table_name} 
                        (time, rssi, rsrp, rsrq, snr, network_type) 
                        VALUES (%s, %s, %s, %s, %s, %s)""",
                    (
                        network_data.get('timestamp'),
                        network_data.get('rssi'),
                        network_data.get('rsrp'),
                        network_data.get('rsrq'),
                        network_data.get('snr'),
                        network_data.get('network_type')
                    )
                )
            conn.commit()
    
    def insert_device_info(self, device_id: str, info_data: Dict[str, Any]):
        """Insert or update device info (replace existing)"""
        safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
        table_name = f"info_{safe_id}"
        
        with self.get_connection() as conn:
            self.create_device_tables(device_id)
        with conn.cursor() as cur:
                # Clear existing info and insert new
                cur.execute(f"DELETE FROM {table_name}")
            cur.execute(
                    f"""INSERT INTO {table_name} 
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
                        info_data.get('bms_hw')
                    )
                )
            conn.commit()


# Import real decoder for 1:1 decoding
class InlineDecoder:
    """Inline minimal decoder helpers to avoid importing decoder.py.
    These match the header and payload handling used elsewhere.
    """
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
        descriptions = {
            'charging': 'Currently charging' if bits['charging_status'] else 'Not charging',
            'discharging': 'Currently discharging' if bits['discharge_state'] else 'Not discharging',
            'charging_switch': 'Charging switch ON' if bits['charging_mos_status'] else 'Charging switch OFF',
            'discharge_switch': 'Discharge switch ON' if bits['discharge_mos_status'] else 'Discharge switch OFF',
            'charging_allowed': 'Charging prohibited' if bits['no_charge'] else 'Charging allowed',
            'discharging_allowed': 'Discharging prohibited' if bits['no_discharge'] else 'Discharging allowed',
        }
        active_alarms: List[str] = []
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
            cell_voltages: List[int] = []
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
            bms_temps: List[int] = []
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
            cell_temps: List[int] = []
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

    class AdvancedGPSDecoder:
        HARARE_LAT = -17.742873572292066
        HARARE_LON = 31.075731885036568
        @staticmethod
        def bcd_to_nibbles(b: bytes) -> List[int]:
            digits: List[int] = []
            for x in b:
                digits.append((x >> 4) & 0x0F)
                digits.append(x & 0x0F)
            return digits
        @staticmethod
        def nibbles_to_int(digs: List[int]) -> int:
            return int(''.join(str(int(d)) for d in digs)) if digs else 0
        @staticmethod
        def bcd_to_int(bcd_bytes: bytes) -> int:
            result = 0
            for byte in bcd_bytes:
                high = (byte >> 4) & 0x0F
                low = byte & 0x0F
                result = result * 100 + high * 10 + low
            return result
        @classmethod
        def decode_latitude_harare(cls, lat_bytes: bytes) -> Tuple[float, str]:
            methods: List[Tuple[str, float]] = []
            try:
                bcd_val = cls.bcd_to_int(lat_bytes) / 10000
                methods.append(("BCD direct", bcd_val))
            except Exception:
                pass
            try:
                nibbles = cls.bcd_to_nibbles(lat_bytes)
                swapped = nibbles[1:] + [nibbles[0]] if len(nibbles) > 0 else []
                if swapped:
                    val = cls.nibbles_to_int(swapped) / 10000
                    methods.append(("Nibble swap", val))
            except Exception:
                pass
            try:
                raw_int = int.from_bytes(lat_bytes, 'big')
                extreme_val = cls.HARARE_LAT + (raw_int % 10000) / 100000000
                methods.append(("Extreme precision", extreme_val))
            except Exception:
                pass
            if methods:
                best = min(methods, key=lambda x: abs(x[1] - cls.HARARE_LAT))
                return best[1], best[0]
            return 0.0, 'No valid method'
        @classmethod
        def decode_longitude_harare(cls, lon_bytes: bytes) -> Tuple[float, str]:
            methods: List[Tuple[str, float]] = []
            try:
                bcd_val = cls.bcd_to_int(lon_bytes) / 10000
                methods.append(("BCD direct", bcd_val))
            except Exception:
                pass
            try:
                nibbles = cls.bcd_to_nibbles(lon_bytes)
                swapped = nibbles[1:] + [nibbles[0]] if len(nibbles) > 0 else []
                if swapped:
                    val = cls.nibbles_to_int(swapped) / 10000
                    methods.append(("Nibble swap", val))
            except Exception:
                pass
            try:
                raw_int = int.from_bytes(lon_bytes, 'big')
                extreme_val = cls.HARARE_LON + (raw_int % 10000) / 100000000
                methods.append(("Extreme precision", extreme_val))
            except Exception:
                pass
            if methods:
                best = min(methods, key=lambda x: abs(x[1] - cls.HARARE_LON))
                return best[1], best[0]
            return 0.0, 'No valid method'
        @classmethod
        def decode_timestamp_fixed(cls, date_bytes: bytes, time_bytes: bytes) -> Tuple[str, str]:
            def swap_nibbles(x: int) -> int:
                return ((x & 0x0F) << 4) | ((x & 0xF0) >> 4)
            def permutations3(b: bytes):
                a, b0, c = b[0], b[1], b[2]
                return [
                    ("abc", b),
                    ("acb", bytes([a, c, b0])),
                    ("bac", bytes([b0, a, c])),
                    ("bca", bytes([b0, c, a])),
                    ("cab", bytes([c, a, b0])),
                    ("cba", bytes([c, b0, a])),
                ]
            date_methods: List[Tuple[str, int, int, int]] = []
            time_methods: List[Tuple[str, int, int, int]] = []
            for name, perm in permutations3(date_bytes):
                for swap_name, swap_fn in [("", lambda x: x), ("swap", swap_nibbles)]:
                    try:
                        test_bytes = bytes([swap_fn(x) for x in perm])
                        nibbles = InlineDecoder.AdvancedGPSDecoder.bcd_to_nibbles(test_bytes)
                        if len(nibbles) >= 6:
                            day = InlineDecoder.AdvancedGPSDecoder.nibbles_to_int(nibbles[0:2])
                            month = InlineDecoder.AdvancedGPSDecoder.nibbles_to_int(nibbles[2:4])
                            year = InlineDecoder.AdvancedGPSDecoder.nibbles_to_int(nibbles[4:6])
                            if 1 <= day <= 31 and 1 <= month <= 12:
                                date_methods.append((f"date:{name}:{swap_name}", 2000 + year, month, day))
                    except Exception:
                        pass
            for name, perm in permutations3(time_bytes):
                for swap_name, swap_fn in [("", lambda x: x), ("swap", swap_nibbles)]:
                    try:
                        test_bytes = bytes([swap_fn(x) for x in perm])
                        nibbles = InlineDecoder.AdvancedGPSDecoder.bcd_to_nibbles(test_bytes)
                        if len(nibbles) >= 6:
                            hour = InlineDecoder.AdvancedGPSDecoder.nibbles_to_int(nibbles[0:2])
                            minute = InlineDecoder.AdvancedGPSDecoder.nibbles_to_int(nibbles[2:4])
                            second = InlineDecoder.AdvancedGPSDecoder.nibbles_to_int(nibbles[4:6])
                            if 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59:
                                time_methods.append((f"time:{name}:{swap_name}", hour, minute, second))
                    except Exception:
                        pass
            if date_methods:
                year, month, day = date_methods[0][1:4]
            else:
                now = datetime.now()
                year, month, day = now.year, now.month, now.day
            if time_methods:
                now = datetime.now()
                current_minutes = now.hour * 60 + now.minute
                pick = min(time_methods, key=lambda t: abs(t[1]*60 + t[2] - current_minutes))
                hour, minute, second = pick[1:4]
            else:
                hour, minute, second = 0, 0, 0
            try:
                naive = datetime(year, month, day, hour, minute, second)
                corrected = naive - timedelta(hours=1, minutes=40)
                return corrected.strftime('%Y-%m-%d'), corrected.strftime('%H:%M:%S')
            except Exception:
                return f"{year:04d}-{month:02d}-{day:02d}", f"{hour:02d}:{minute:02d}:{second:02d}"
        @classmethod
        def decode_sat_speed_direction(cls, tail_bytes: bytes) -> Tuple[int, int, int, float, float]:
            beidou_sat = 0
            gps_sat = 0
            speed_kmh = 0.0
            direction_deg = 0.0
            if len(tail_bytes) >= 2:
                try:
                    nibbles = cls.bcd_to_nibbles(tail_bytes[0:2])
                    if len(nibbles) >= 4:
                        beidou_sat = nibbles[0] if nibbles[0] <= 9 else 0
                        gps_sat = nibbles[1] if nibbles[1] <= 9 else 0
                except Exception:
                    pass
            if len(tail_bytes) >= 5:
                try:
                    speed_n = cls.bcd_to_nibbles(tail_bytes[2:5])
                    if len(speed_n) >= 6:
                        speed_kmh = cls.nibbles_to_int(speed_n[:4]) + cls.nibbles_to_int(speed_n[4:6]) / 100.0
                except Exception:
                    pass
            if len(tail_bytes) >= 8:
                try:
                    dir_n = cls.bcd_to_nibbles(tail_bytes[5:8])
                    if len(dir_n) >= 5:
                        direction_deg = cls.nibbles_to_int(dir_n[:4]) + cls.nibbles_to_int(dir_n[4:5]) / 10.0
                except Exception:
                    pass
            satellites = beidou_sat + gps_sat
            return beidou_sat, gps_sat, satellites, speed_kmh, direction_deg

    @staticmethod
    def decode_battery_position(payload: bytes, device_id: str) -> Optional[Dict[str, Any]]:
        try:
            header = InlineDecoder.parse_header(payload)
            body = payload[InlineDecoder.HEADER_SIZE:]
            if not body:
                return {'header': header, 'position_count': 0, 'positions': [], 'raw_payload_hex': payload.hex()}
            # Mirror gps_analyzer first-record scan logic closely
            total_len = body[0] if len(body) >= 1 else 0
            if total_len == 0 or len(body) < total_len + 1:
                return {'header': header, 'position_count': 0, 'positions': [], 'raw_payload_hex': payload.hex()}
            i = 1
            if i >= len(body):
                return {'header': header, 'position_count': 0, 'positions': [], 'raw_payload_hex': payload.hex()}
            rec_len = body[i]
            if rec_len == 21 and i + 1 + 21 <= len(body):
                rec = body[i+1:i+1+21]
            else:
                rec = None
                scan = i
                end = 1 + total_len
                while scan < end and scan < len(body):
                    if scan + 1 <= len(body):
                        l = body[scan]
                        if l == 21 and scan + 1 + 21 <= len(body):
                            rec = body[scan+1:scan+1+21]
                            break
                        scan += 1 + l if l > 0 else 1
                    else:
                        break
                if rec is None:
                    return {'header': header, 'position_count': 0, 'positions': [], 'raw_payload_hex': payload.hex()}
                raw_lat = rec[0:3]
                raw_lon = rec[3:6]
                lat_val, _ = gps.decode_latitude_harare(raw_lat)
                lon_val, _ = gps.decode_longitude_harare(raw_lon)
                latitude = -abs(lat_val)
                longitude = abs(lon_val)
                date_bytes = rec[6:9]
                time_bytes = rec[9:12]
                date_str, time_str = gps.decode_timestamp_fixed(date_bytes, time_bytes)
                tail_bytes = rec[12:21]
                gps = InlineDecoder.AdvancedGPSDecoder()
                beidou_sat, gps_sat, satellites, speed_kmh, direction_deg = gps.decode_sat_speed_direction(tail_bytes)
                positions = [{
                    'latitude': latitude,
                    'longitude': longitude,
                    'timestamp': f"{date_str} {time_str}",
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



class BatteryMQTTCollector:
    """Main MQTT collector class"""
    
    def __init__(self, mqtt_config: MQTTConfig, db_config: DatabaseConfig, app_config: AppConfig):
        self.mqtt_config = mqtt_config
        self.db_config = db_config
        self.app_config = app_config
        self.db_manager = DatabaseManager(db_config)
        self.processor = BatteryDataProcessor()
        self.decoder = InlineDecoder()
        
        # Data collection storage
        self.collected_data: Dict[str, Dict[str, Any]] = {}
        self.collection_lock = threading.Lock()
        
        # MQTT client setup
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        
        # Request tracking
        self.request_sequence = 0
        self.request_transaction = 0
        self.request_devices: Set[str] = set()
        
        # Threading
        self.stop_event = threading.Event()
        self.request_thread: Optional[threading.Thread] = None
        
        # Logging
        self.logger = logging.getLogger(__name__)
    
    def setup_logging(self):
        """Configure logging"""
        level = logging.DEBUG if self.app_config.verbose else logging.INFO
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('battery_collector.log')
            ]
        )
    
    def on_connect(self, client, userdata, flags, reason_code, properties):
        """MQTT connection callback"""
        if reason_code == 0:
            self.logger.info("Connected to MQTT broker")
            subscribe_topic = f"{self.mqtt_config.topic_base}/#"
            client.subscribe(subscribe_topic, qos=1)
            self.logger.info(f"Subscribed to {subscribe_topic}")
        else:
            self.logger.error(f"MQTT connection failed with code {reason_code}")
    
    def on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        """MQTT disconnect callback"""
        self.logger.warning(f"MQTT disconnected with code {reason_code}")
    
    def on_message(self, client, userdata, msg):
        """MQTT message callback"""
        try:
            topic = msg.topic
            device_id = self.decoder.extract_device_id(topic)
            message_type = topic.split('/')[-1]
            payload_bytes = self.decoder.decode_payload_bytes(msg.payload)
            
            # Log incoming raw payload in hex for full traceability
            try:
                in_hex = payload_bytes.hex()
    except Exception:
                in_hex = "<hex-error>"
            self.logger.info(f"INCOMING MQTT MESSAGE | topic={topic} | type={message_type} | hex={in_hex}")
            print(f"INCOMING MQTT MESSAGE | topic={topic} | type={message_type} | hex={in_hex}")
            
            with self.collection_lock:
                self.ensure_device_data(device_id)
                # Proactively request static info at least once per device, same as decoder.py behavior
                if device_id not in getattr(self, 'ext_requested_once', set()) and message_type not in ("batPropertyExtRprt", "batPropertyExtRsp"):
                    try:
                        # Track the set lazily to avoid init order constraints
                        if not hasattr(self, 'ext_requested_once'):
                            self.ext_requested_once = set()
                        self.send_request(device_id, 'batPropertyExtReq')
                        self.ext_requested_once.add(device_id)
                    except Exception as e:
                        self.logger.warning(f"Auto Ext request failed for {device_id}: {e}")
                # Save raw message for final report
                self.collected_data[device_id]['raw_messages'].append({'topic': topic, 'type': message_type, 'hex': in_hex})
                self.process_message(device_id, message_type, payload_bytes)
                
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
    
    def ensure_device_data(self, device_id: str):
        """Ensure device data structure exists"""
        if device_id not in self.collected_data:
            self.collected_data[device_id] = {
                'status': None,
                'temps': None,
                'cells': None,
                'network': None,
                'info': None,
                'positions': [],
                'raw_messages': [],
                'decoded_messages': [],
                'stored': {
                    'info': False,
                    'positions_idx': 0
                }
            }
    
    def process_message(self, device_id: str, message_type: str, payload_bytes: bytes):
        """Process different message types"""
        device_data = self.collected_data[device_id]
        
        if message_type == 'batPropertyRprt':
            self.process_property_report(device_id, payload_bytes, device_data)
            device_data['decoded_messages'].append({'type': message_type, 'decoded': 'status/temps/cells/network updated'})
        elif message_type in ('batPropertyExtRprt', 'batPropertyExtRsp'):
            self.process_property_ext(device_id, payload_bytes, device_data)
            device_data['decoded_messages'].append({'type': message_type, 'decoded': device_data.get('info')})
        elif message_type in ('batPositionRprt', 'batPositonRprt'):
            self.process_position_report(device_id, payload_bytes, device_data)
            latest_pos = device_data.get('positions', [])[-1] if device_data.get('positions') else None
            device_data['decoded_messages'].append({'type': message_type, 'decoded': latest_pos})
    
    def process_property_report(self, device_id: str, payload: bytes, device_data: Dict):
        """Process battery property report"""
        decoded = self.decoder.decode_battery_property_report(payload, device_id)
    if not decoded:
        return
        
        now = datetime.now(timezone.utc)
        
        # Process status data
        status = decoded.get('Battery Status', {})
        has_alarms = bool((status or {}).get('has_alarms'))
    status_text = 'No active alarms' if not has_alarms else ', '.join(status.get('active_alarms', []))

        device_data['status'] = {
            'timestamp': now,
            'current': self.processor.parse_float_prefix(decoded.get('Current')),
        'current_type': decoded.get('Current_Type'),
            'soc': self.processor.parse_float_prefix(decoded.get('State of Charge')),
            'total_voltage_mv': self.processor.parse_int_prefix(decoded.get('Total Battery Voltage')),
            'remaining_capacity_ah': self.processor.parse_float_prefix(decoded.get('Remaining Capacity')),
            'total_capacity_ah': self.processor.parse_float_prefix(decoded.get('Total Capacity')),
        'loop_cycles': decoded.get('Loop Cycles'),
            'status_text': status_text
        }
        
        # Process temperature data
        device_data['temps'] = {
            'timestamp': now,
            'bms_temps': decoded.get('BMS Temperatures', []),
            'cell_temps': decoded.get('Cell Temperatures', [])
        }
        
        # Process cell voltage data
        cell_voltages = decoded.get('Cell Voltages', [])
    if cell_voltages:
            device_data['cells'] = {
                'timestamp': now,
                'cell_voltages_mv': cell_voltages
            }
        
        # Process network data
        network = decoded.get('Network Status', {})
        if network:
            device_data['network'] = {
                'timestamp': now,
                'rssi': network.get('rssi'),
                'rsrp': network.get('rsrp'),
                'rsrq': network.get('rsrq'),
                'snr': network.get('snr'),
                'network_type': network.get('rat')
            }
        
        self.logger.info(f"Processed property report for {device_id}")
    
    def process_property_ext(self, device_id: str, payload: bytes, device_data: Dict):
        """Process extended property report"""
        decoded = self.decoder.decode_battery_property_ext(payload, device_id)
    if not decoded:
        return
        
        device_data['info'] = decoded
        self.logger.info(f"Processed extended properties for {device_id}")
        
        if self.app_config.verbose:
            self.logger.debug(f"Device info: {json.dumps(decoded, indent=2)}")
    
    def process_position_report(self, device_id: str, payload: bytes, device_data: Dict):
        """Process position report"""
        decoded = self.decoder.decode_battery_position(payload, device_id)
        if not decoded or not isinstance(decoded, dict):
        return
        
        positions = decoded.get('positions', [])
        for pos in positions:
            safe_time = self.processor.parse_gps_timestamp(pos)
            raw_direction = pos.get('direction_deg')
            speed = pos.get('speed_kmh')
            sats_total = pos.get('satellites')
            
            # Only include direction if we have good GPS lock and some speed
            direction = None
            if (isinstance(speed, (int, float)) and speed >= 2 and
                isinstance(sats_total, int) and sats_total >= 4 and
                isinstance(raw_direction, (int, float))):
                direction = raw_direction % 360
            
            position_data = {
            'utc_time': safe_time,
                'latitude': pos.get('latitude'),
                'longitude': pos.get('longitude'),
                'direction': direction,
                'satellites_total': sats_total,
                'satellites_gps': pos.get('gps_satellites'),
                'satellites_beidou': pos.get('beidou_satellites'),
                'hemisphere': f"{'S' if pos.get('is_south') else 'N'}{'W' if pos.get('is_west') else 'E'}"
            }
            
            device_data['positions'].append(position_data)
        
        self.logger.info(f"Processed {len(positions)} position(s) for {device_id}")
    
    def send_request(self, device_id: str, request_type: str):
        """Send MQTT request to device and log exact hex payload"""
        self.request_sequence = (self.request_sequence + 1) & 0xFFFF
        self.request_transaction = (self.request_transaction + 1) & 0xFF
        header = self.decoder.build_header(self.request_sequence, self.request_transaction)
        topic = f"{self.mqtt_config.topic_base}/{device_id}/user/{request_type}"
        result = self.client.publish(topic, payload=header, qos=1, retain=False)
        try:
            hex_payload = header.hex()
        except Exception:
            hex_payload = "<hex-error>"
        self.logger.info(f"OUTGOING MQTT PUBLISH | topic={topic} | hex={hex_payload} | seq={self.request_sequence} txn={self.request_transaction}")
        print(f"OUTGOING MQTT PUBLISH | topic={topic} | hex={hex_payload} | seq={self.request_sequence} txn={self.request_transaction}")
        return result.is_published()

    def send_control(self, device_id: str, control_type: int, value: int):
        """Send BMS control request with header + 2-byte body (to match pinger)."""
        self.request_sequence = (self.request_sequence + 1) & 0xFFFF
        self.request_transaction = (self.request_transaction + 1) & 0xFF
        header = self.decoder.build_header(self.request_sequence, self.request_transaction)
        body = struct.pack('BB', control_type & 0xFF, value & 0xFF)
        topic = f"{self.mqtt_config.topic_base}/{device_id}/user/bmsCtrReq"
        result = self.client.publish(topic, payload=header + body, qos=1, retain=False)
        try:
            hex_payload = (header + body).hex()
        except Exception:
            hex_payload = "<hex-error>"
        self.logger.info(f"OUTGOING MQTT PUBLISH | topic={topic} | hex={hex_payload} | seq={self.request_sequence} txn={self.request_transaction}")
        print(f"OUTGOING MQTT PUBLISH | topic={topic} | hex={hex_payload} | seq={self.request_sequence} txn={self.request_transaction}")
        return result.is_published()
    
    def request_loop(self):
        """Active request loop for specified devices"""
        self.logger.info("Starting request loop")
        
        while not self.stop_event.is_set():
            try:
                for device_id in self.request_devices:
                    if self.stop_event.is_set():
                        break
                    
                    # Match gps_analyzer ping cadence: ext -> wake -> ctrl (allow discharge)
                    self.send_request(device_id, 'batPropertyExtReq')
                    time.sleep(0.5)
                    
                    self.send_request(device_id, 'batPropertyReq')
                    time.sleep(0.5)
                    
                    # Optional control to nudge device awake
                    self.send_control(device_id, 1, 0)
                    time.sleep(0.5)

                    # Proactively request GPS position (both spellings)
                    self.send_request(device_id, 'batPositionReq')
                    time.sleep(0.3)
                    self.send_request(device_id, 'batPositonReq')
                    time.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"Request loop error: {e}")
            
            # Wait for next cycle
            self.stop_event.wait(self.app_config.request_interval)
        
        self.logger.info("Request loop stopped")
    
    def start_collection(self, request_devices: List[str] = None):
        """Start data collection"""
        self.setup_logging()
        self.logger.info("Starting battery data collection")
        
        # Set up request devices
        if request_devices:
            self.request_devices = set(request_devices)
            self.logger.info(f"Will actively request data from: {request_devices}")
        
        # Connect to MQTT
        try:
            self.client.connect(self.mqtt_config.broker, self.mqtt_config.port, 60)
        except Exception as e:
            self.logger.error(f"Failed to connect to MQTT broker: {e}")
            return False
        
        # Start MQTT loop
        self.client.loop_start()
        
        # Start request thread if needed
        if self.request_devices:
            self.request_thread = threading.Thread(target=self.request_loop, daemon=True)
            self.request_thread.start()
        
        # Run collection for specified duration
        self.logger.info(f"Collecting data for {self.app_config.capture_seconds} seconds")
        start_time = time.time()
        
        try:
            while time.time() - start_time < self.app_config.capture_seconds:
                time.sleep(1)
                
                # Log progress every 30 seconds
                elapsed = int(time.time() - start_time)
                if elapsed % 30 == 0 and elapsed > 0:
                    remaining = self.app_config.capture_seconds - elapsed
                    self.logger.info(f"Collection progress: {elapsed}s elapsed, {remaining}s remaining")
                # Every 150 seconds (2m30s) store current collected data and print what was written
                if elapsed % 150 == 0 and elapsed > 0:
                    self.logger.info("Periodic store: writing collected data so far to DB")
                    print("\n=== PERIODIC STORE (2m30s) ===")
                    self.store_collected_data()
        
        except KeyboardInterrupt:
            self.logger.info("Collection interrupted by user")
        
        finally:
            self.stop_collection()
        
        # Process and store collected data
        self.store_collected_data()
        
        return True
    
    def stop_collection(self):
        """Stop data collection"""
        self.logger.info("Stopping data collection")
        
        # Stop request thread
        self.stop_event.set()
        if self.request_thread and self.request_thread.is_alive():
            self.request_thread.join(timeout=5)
        
        # Stop MQTT client
        self.client.loop_stop()
        self.client.disconnect()
    
    def store_collected_data(self):
        """Store all collected data to database"""
        self.logger.info("Storing collected data to database")
        
        with self.collection_lock:
            for device_id, device_data in self.collected_data.items():
                try:
                    self.store_device_data(device_id, device_data)
                except Exception as e:
                    self.logger.error(f"Failed to store data for {device_id}: {e}")
        
        self.logger.info("Data storage complete")
        # Print a compact list of all topics captured per device
        for device_id, device_data in self.collected_data.items():
            raws = device_data.get('raw_messages', [])
            if raws:
                print(f"\n=== RAW TOPICS CAPTURED FOR {device_id} ===")
                for m in raws[-50:]:  # limit output
                    print(f"{m['type']}: {m['topic']} => {m['hex'][:80]}{'...' if len(m['hex'])>80 else ''}")
            dec = device_data.get('decoded_messages', [])
            if dec:
                print(f"\n=== DECODED MESSAGES FOR {device_id} ===")
                for entry in dec[-30:]:  # limit
                    try:
                        print(f"{entry['type']}: {json.dumps(entry['decoded'], ensure_ascii=False, default=str)[:200]}")
                    except Exception:
                        print(f"{entry['type']}: <decode-print-error>")
    
    def store_device_data(self, device_id: str, device_data: Dict[str, Any]):
        """Store data for a single device"""
        stored_items = []
        def _pfx(lbl: str):
            print(f"STORED {lbl} for {device_id}:")
        def _pdump(obj: Any):
            try:
                print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))
            except Exception:
                print(str(obj))
        
        try:
            # Store device info first (only once)
            if device_data.get('info') and not device_data['stored'].get('info'):
                info = device_data['info']
                info_row = {
                    'product_sn': info.get('Product SN'),
                    'gps_sn': info.get('GPS SN'),
                    'gps_imsi': info.get('GPS IMSI'),
                    'gps_imei': info.get('GPS IMEI'),
                    'gps_sw': info.get('GPS Software Version'),
                    'gps_hw': info.get('GPS Hardware Version'),
                    'bms_sn': info.get('BMS SN'),
                    'bms_sw': info.get('BMS Software Version'),
                    'bms_hw': info.get('BMS Hardware Version')
                }
                self.db_manager.insert_device_info(device_id, info_row)
                stored_items.append('info')
                _pfx('INFO')
                _pdump(info_row)
                device_data['stored']['info'] = True
            
            # Store status data (every time we run; treat as timeseries)
            if device_data.get('status'):
                self.db_manager.insert_status(device_id, device_data['status'])
                stored_items.append('status')
                _pfx('STATUS')
                _pdump(device_data['status'])
            
            # Store temperature data
            if device_data.get('temps'):
                self.db_manager.insert_temperatures(device_id, device_data['temps'])
                stored_items.append('temps')
                _pfx('TEMPS')
                _pdump(device_data['temps'])
            
            # Store cell voltage data
            if device_data.get('cells'):
                self.db_manager.insert_cells(device_id, device_data['cells'])
                stored_items.append('cells')
                _pfx('CELLS')
                _pdump(device_data['cells'])
            
            # Store network data
            if device_data.get('network'):
                self.db_manager.insert_network(device_id, device_data['network'])
                stored_items.append('network')
                _pfx('NETWORK')
                _pdump(device_data['network'])
            
            # Store position data
            positions = device_data.get('positions', [])
            start_idx = int(device_data['stored'].get('positions_idx', 0))
            for position in positions[start_idx:]:
                self.db_manager.insert_position(device_id, position)
            if len(positions) > start_idx:
                new_count = len(positions) - start_idx
                device_data['stored']['positions_idx'] = len(positions)
                stored_items.append(f'positions(+{new_count})')
                _pfx('POSITIONS')
                # Print up to last 3 new positions for brevity
                recent = positions[-3:] if new_count > 3 else positions[start_idx:]
                _pdump(recent)
            
            self.logger.info(f"Stored data for {device_id}: {', '.join(stored_items)}")
            
            # Print summary
            self.print_device_summary(device_id, device_data)
            
        except Exception as e:
            self.logger.error(f"Error storing data for {device_id}: {e}")
            raise
    
    def print_device_summary(self, device_id: str, device_data: Dict[str, Any]):
        """Print a summary of collected device data"""
        print(f"\n=== DEVICE SUMMARY: {device_id} ===")
        
        # Device info
        info = device_data.get('info', {})
        if info:
            print(f"INFO: Product SN={info.get('Product SN')}, "
                  f"GPS SN={info.get('GPS SN')}, "
                  f"IMEI={info.get('GPS IMEI')}, "
                  f"IMSI={info.get('GPS IMSI')}")
            print(f"      GPS SW={info.get('GPS Software Version')}, "
                  f"GPS HW={info.get('GPS Hardware Version')}")
            print(f"      BMS SN={info.get('BMS SN')}, "
                  f"BMS SW={info.get('BMS Software Version')}")
        
        # Status data
        status = device_data.get('status', {})
        if status:
            print(f"STATUS: Current={status.get('current')}A ({status.get('current_type')}), "
                  f"SOC={status.get('soc')}%, "
                  f"Voltage={status.get('total_voltage_mv')}mV")
            print(f"        Capacity: {status.get('remaining_capacity_ah')}/{status.get('total_capacity_ah')} Ah, "
                  f"Cycles={status.get('loop_cycles')}")
            print(f"        Status: {status.get('status_text')}")
        
        # Temperature data
        temps = device_data.get('temps', {})
        if temps:
            bms_temps = temps.get('bms_temps', [])
            cell_temps = temps.get('cell_temps', [])
            print(f"TEMPS: BMS={bms_temps}°C, Cells={cell_temps}°C")
        
        # Cell voltages
        cells = device_data.get('cells', {})
        if cells:
            voltages = cells.get('cell_voltages_mv', [])
            print(f"CELLS: {len(voltages)} cells, Range={min(voltages) if voltages else 'N/A'}-{max(voltages) if voltages else 'N/A'}mV")
        
        # Network data
        network = device_data.get('network', {})
        if network:
            print(f"NETWORK: RSSI={network.get('rssi')}dBm, "
                  f"Type={network.get('network_type')}, "
                  f"RSRP={network.get('rsrp')}dBm")
        
        # Position data
        positions = device_data.get('positions', [])
        if positions:
            latest = positions[-1]
            print(f"POSITION: {len(positions)} record(s), "
                  f"Latest: {latest.get('latitude')}, {latest.get('longitude')} "
                  f"({latest.get('satellites_total')} sats)")
        
        print()


def main():
    """Main application entry point"""
    parser = argparse.ArgumentParser(
        description='Battery MQTT Data Collector - Collects telemetry from battery monitoring devices'
    )
    
    # MQTT Configuration
    parser.add_argument('--broker', default='mqtt-cloud-1.telco.co.zw', 
                       help='MQTT broker hostname')
    parser.add_argument('--port', type=int, default=1883, 
                       help='MQTT broker port')
    
    # Database Configuration
    parser.add_argument('--db-host', default='127.0.0.1', 
                       help='Database host')
    parser.add_argument('--db-port', type=int, default=5432, 
                       help='Database port')
    parser.add_argument('--db-name', default='batteries', 
                       help='Database name')
    parser.add_argument('--db-user', default='troy', 
                       help='Database user')
    parser.add_argument('--db-password', default='s3rv3r5mx', 
                       help='Database password')
    
    # Application Configuration
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Enable verbose logging')
    parser.add_argument('--capture-time', type=int, default=210, 
                       help='Data capture duration in seconds (default: 210)')
    parser.add_argument('--request-interval', type=int, default=3, 
                       help='Interval between requests in seconds (default: 3)')
    parser.add_argument('--device', action='append', dest='devices', 
                       help='Device ID to actively request data from (can be specified multiple times)')
    
    # Test mode
    parser.add_argument('--test-db', action='store_true', 
                       help='Test database connection and exit')
    parser.add_argument('--create-tables', 
                       help='Create tables for specified device ID and exit')
    parser.add_argument('--truncate-device', 
                       help='Truncate all tables for specified device ID and exit')
    parser.add_argument('--truncate-before', action='append', dest='truncate_before',
                       help='Truncate tables for this device ID before starting collection (can repeat)')
    
    args = parser.parse_args()

    # Create configuration objects
    mqtt_config = MQTTConfig(
        broker=args.broker,
        port=args.port
    )
    
    db_config = DatabaseConfig(
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        user=args.db_user,
        password=args.db_password
    )
    
    app_config = AppConfig(
        verbose=args.verbose,
        capture_seconds=args.capture_time,
        request_interval=args.request_interval
    )
    
    # Handle special modes
    if args.test_db:
        print("Testing database connection...")
        try:
            db_manager = DatabaseManager(db_config)
            with db_manager.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version = cur.fetchone()[0]
                    print(f"✓ Connected successfully to PostgreSQL: {version}")
            return 0
        except Exception as e:
            print(f"✗ Database connection failed: {e}")
            return 1
    
    if args.create_tables:
        print(f"Creating tables for device: {args.create_tables}")
        try:
            db_manager = DatabaseManager(db_config)
            db_manager.create_device_tables(args.create_tables)
            print("✓ Tables created successfully")
            return 0
        except Exception as e:
            print(f"✗ Table creation failed: {e}")
            return 1
    
    if args.truncate_device:
        print(f"Truncating tables for device: {args.truncate_device}")
        try:
            db_manager = DatabaseManager(db_config)
            db_manager.truncate_device_tables(args.truncate_device)
            print("✓ Tables truncated successfully")
            return 0
        except Exception as e:
            print(f"✗ Truncate failed: {e}")
            return 1
    
    # Create and start collector
    collector = BatteryMQTTCollector(mqtt_config, db_config, app_config)
    
    # Optional pre-run truncates
    if args.truncate_before:
        try:
            db_manager = DatabaseManager(db_config)
            for dev in args.truncate_before:
                print(f"Pre-run truncate for device: {dev}")
                db_manager.truncate_device_tables(dev)
        except Exception as e:
            print(f"Pre-run truncate failed: {e}")
    
    try:
        success = collector.start_collection(request_devices=args.devices)
        return 0 if success else 1
    except Exception as e:
        print(f"Collection failed: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
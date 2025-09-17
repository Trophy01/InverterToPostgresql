#!/usr/bin/env python3
"""
MQTT → PostgreSQL bridge using inlined decoder and DB schema

- Subscribes to /SW_GPS/#
- Decodes messages with inlined logic (based on decoder.py)
- Inserts into per-device tables using the same schema as db/daemon.py
- Defaults to connecting to PostgreSQL at 127.0.0.1:5432 (for SSH tunnel)
- Runs continuously until stopped with Ctrl+C
- Automatically stores data every 2 minutes and 30 seconds

Usage:
  python mqtt_to_postgres.py                    # Run with defaults
  python mqtt_to_postgres.py -v                 # Run with verbose logging
  python mqtt_to_postgres.py --device 123456    # Override default devices
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt

# Ensure project root is on sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
	sys.path.insert(0, BASE_DIR)

# -----------------------------
# Inlined DB config/manager (based on db/daemon.py)
# -----------------------------
from contextlib import contextmanager
import psycopg2


class DatabaseConfig:
	def __init__(self, host: str = '127.0.0.1', port: int = 5432, database: str = 'batteries', user: str = 'troy', password: str = 's3rv3r5mx'):
		self.host = host
		self.port = port
		self.database = database
		self.user = user
		self.password = password

	@property
	def dsn(self) -> str:
		return f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"


class BatteryDataProcessor:
	@staticmethod
	def sanitize_identifier(identifier: str) -> str:
		return ''.join(c for c in identifier if c.isalnum() or c == '_')

	@staticmethod
	def parse_float_prefix(text: Optional[str]) -> Optional[float]:
		if text is None:
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
	def parse_gps_timestamp(pos_data: Dict[str, Any]) -> datetime:
		"""Parse GPS timestamp from position data - ALWAYS use current time to fix broken GPS timestamps"""
		# Always use current time since GPS timestamps are broken (showing 2085, etc.)
		return datetime.now(timezone.utc)


class DatabaseManager:
	def __init__(self, config: DatabaseConfig):
		self.config = config
		self.logger = logging.getLogger(__name__)

	@contextmanager
	def get_connection(self):
		conn = None
		try:
			conn = psycopg2.connect(self.config.dsn)
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
				for table_name, schema in tables.items():
					cur.execute(schema.format(table=table_name))
				conn.commit()

	def insert_position(self, device_id: str, position_data: Dict[str, Any]):
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
						position_data.get('hemisphere'),
					)
				)
				conn.commit()

	def insert_status(self, device_id: str, status_data: Dict[str, Any]):
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
						status_data.get('status_text'),
					)
				)
				conn.commit()

	def insert_temperatures(self, device_id: str, temp_data: Dict[str, Any]):
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
						temp_data.get('cell_temps'),
					)
				)
				conn.commit()

	def insert_cells(self, device_id: str, cell_data: Dict[str, Any]):
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
						cell_data.get('cell_voltages_mv'),
					)
				)
				conn.commit()

	def insert_network(self, device_id: str, network_data: Dict[str, Any]):
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
						network_data.get('network_type'),
					)
				)
				conn.commit()

	def insert_device_info(self, device_id: str, info_data: Dict[str, Any]):
		safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
		table_name = f"info_{safe_id}"
		with self.get_connection() as conn:
			self.create_device_tables(device_id)
			with conn.cursor() as cur:
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
						info_data.get('bms_hw'),
					)
				)
				conn.commit()

# -----------------------------
# Inlined decoder (based on decoder.py)
# -----------------------------
import struct

START_CODE = 0x4350
PROTOCOL_VERSION = 0x11
HEADER_SIZE = 6


def parse_header(payload: bytes) -> Dict[str, Any]:
	"""Parse the 6-byte message header."""
	if len(payload) < HEADER_SIZE:
		raise ValueError('Payload shorter than header size')
	start_code, proto_ver, seq, txn = struct.unpack('!H B H B', payload[:HEADER_SIZE])
	return {
		'start_code': start_code,
		'protocol_version': proto_ver,
		'sequence_number': seq,
		'transaction_id': txn,
		'valid': (start_code == START_CODE and proto_ver == PROTOCOL_VERSION),
	}


def decode_battery_status(status_bytes: bytes) -> Dict[str, Any]:
	"""Decode 4-byte status bitfield into flags and descriptions."""
	status_int = int.from_bytes(status_bytes, byteorder='big')
	bits = {
		'charging_status': (status_int >> 0) & 1,
		'full_state': (status_int >> 1) & 1,
		'charge_overcurrent': (status_int >> 2) & 1,
		'cell_overvoltage': (status_int >> 3) & 1,
		'discharge_state': (status_int >> 4) & 1,
		'short_circuit_alarm': (status_int >> 5) & 1,
		'discharge_overflow': (status_int >> 6) & 1,
		'cell_undervoltage': (status_int >> 7) & 1,
		'cell_open_circuit': (status_int >> 8) & 1,
		'temp_detect_open': (status_int >> 9) & 1,
		'cell_high_temp': (status_int >> 10) & 1,
		'cell_low_temp': (status_int >> 11) & 1,
		'bms_high_temp': (status_int >> 12) & 1,
		'reserved_bit13': (status_int >> 13) & 1,
		'no_charge': (status_int >> 14) & 1,
		'no_discharge': (status_int >> 15) & 1,
		'discharge_mos_failed': (status_int >> 16) & 1,
		'charging_mos_status': (status_int >> 17) & 1,
		'discharge_mos_status': (status_int >> 18) & 1,
		'charge_mos_failure': (status_int >> 19) & 1,
		'high_low_voltage_failure': (status_int >> 20) & 1,
		'ultra_high_temp_failure': (status_int >> 21) & 1,
		'cell_pressure_difference': (status_int >> 22) & 1,
		'battery_temp_difference': (status_int >> 23) & 1,
	}
	active = []
	if bits['charge_mos_failure']:
		active.append('Charge MOS failure')
	if bits['high_low_voltage_failure']:
		active.append('High/low voltage cell failure')
	if bits['ultra_high_temp_failure']:
		active.append('Ultra high temperature failure')
	if bits['cell_pressure_difference']:
		active.append('Large cell pressure difference')
	if bits['battery_temp_difference']:
		active.append('Large battery temperature difference')
	return {
		'raw': status_int,
		'bits': bits,
		'active_alarms': active,
		'has_alarms': len(active) > 0,
	}


def decode_battery_property_report(payload_bytes: bytes, topic_device_id: str = None) -> Optional[Dict[str, Any]]:
	"""Decode property report: current, temps, cells, capacities, network."""
	try:
		_ = parse_header(payload_bytes)
		body = payload_bytes[HEADER_SIZE:]
		offset = 0

		# Product SN (LV)
		if offset >= len(body):
			return None
		sn_len = body[offset]
		offset += 1
		if offset + sn_len > len(body):
			return None
		if all(b == 0xFF for b in body[offset:offset+sn_len]):
			product_sn = 'UNKNOWN_SN'
		else:
			try:
				product_sn = body[offset:offset+sn_len].decode('ascii', errors='replace')
			except Exception:
				product_sn = 'HEX:' + body[offset:offset+sn_len].hex()
		offset += sn_len

		# Battery Status (4 bytes)
		if offset + 4 > len(body):
			return None
		status = decode_battery_status(body[offset:offset+4])
		offset += 4

		# Battery Data length (1 byte)
		if offset >= len(body):
			return None
		_ = body[offset]
		offset += 1

		# Current (2 bytes, signed, deci-amps)
		if offset + 2 > len(body):
			return None
		current = int.from_bytes(body[offset:offset+2], 'big', signed=True) / 10
		offset += 2

		# Cell voltages list (LV: length in bytes, values are 2 bytes each)
		if offset >= len(body):
			return None
		cv_len = body[offset]
		offset += 1
		if offset + cv_len > len(body):
			return None
		cell_voltages = []
		end_cv = offset + cv_len
		while offset + 2 <= end_cv:
			cell_voltages.append(int.from_bytes(body[offset:offset+2], 'big'))
			offset += 2

		# BMS temps (LV, signed bytes)
		if offset >= len(body):
			return None
		bms_len = body[offset]
		offset += 1
		if offset + bms_len > len(body):
			return None
		bms_temps = []
		for _i in range(bms_len):
			val = body[offset]
			if val > 127:
				val -= 256
			bms_temps.append(val)
			offset += 1

		# Cell temps (LV, signed bytes)
		if offset >= len(body):
			return None
		ct_len = body[offset]
		offset += 1
		if offset + ct_len > len(body):
			return None
		cell_temps = []
		for _i in range(ct_len):
			val = body[offset]
			if val > 127:
				val -= 256
			cell_temps.append(val)
			offset += 1

		# Loop cycles (2)
		if offset + 2 > len(body):
			return None
		loop_cycles = int.from_bytes(body[offset:offset+2], 'big')
		offset += 2

		# Remaining capacity (2, deci-Ah) and total capacity (2, deci-Ah)
		if offset + 4 > len(body):
			return None
		remaining_capacity = int.from_bytes(body[offset:offset+2], 'big') / 10
		offset += 2
		total_capacity = int.from_bytes(body[offset:offset+2], 'big') / 10
		offset += 2

		# Network status (best-effort)
		net = {}
		if offset < len(body):
			rssi = body[offset]
			net['rssi'] = None if rssi in (0, 255) else -int(rssi)
			offset += 1
			if offset + 6 <= len(body):
				net['plmn'] = body[offset:offset+6].decode('ascii', errors='replace')
				offset += 6
			if offset + 2 <= len(body):
				net['lac'] = body[offset:offset+2].hex()
				offset += 2
			if offset + 4 <= len(body):
				net['cell_id'] = body[offset:offset+4].hex()
				offset += 4
			if offset < len(body):
				net['rat'] = body[offset]
				offset += 1

		soc_pct = 0.0
		if total_capacity > 0:
			soc_pct = (remaining_capacity / total_capacity) * 100.0
		total_voltage_mv = sum(cell_voltages) if cell_voltages else 0

		return {
			'Device ID': topic_device_id if topic_device_id and product_sn == 'UNKNOWN_SN' else product_sn,
			'Product SN': product_sn,
			'Battery Status': status,
			'Current': f'{current} A',
			'Current_Type': 'Charging' if current > 0 else 'Discharging' if current < 0 else 'Idle',
			'Cell Voltages': cell_voltages,
			'Total Battery Voltage': f'{total_voltage_mv} mV',
			'BMS Temperatures': bms_temps,
			'Cell Temperatures': cell_temps,
			'Loop Cycles': loop_cycles,
			'Remaining Capacity': f'{remaining_capacity} Ah',
			'Total Capacity': f'{total_capacity} Ah',
			'State of Charge': f'{soc_pct:.1f}% ',
			'Network Status': net,
		}
	except Exception:
		return None


def decode_battery_property_ext(payload_bytes: bytes, topic_device_id: str = None) -> Optional[Dict[str, Any]]:
	"""Minimal ext decoder to populate info table. Best-effort parsing."""
	try:
		_ = parse_header(payload_bytes)
		body = payload_bytes[HEADER_SIZE:]
		offset = 0
		def read_lv_ascii() -> str:
			nonlocal offset
			if offset >= len(body):
				return ''
			l = body[offset]
			offset += 1
			if offset + l > len(body):
				return ''
			chunk = body[offset:offset+l]
			offset += l
			if all(b == 0xFF for b in chunk):
				return ''
			try:
				return chunk.decode('ascii', errors='replace')
			except Exception:
				return 'HEX:' + chunk.hex()
		product_sn = read_lv_ascii()
		gps_sn = read_lv_ascii()
		gps_imsi = read_lv_ascii()
		gps_imei = read_lv_ascii()
		gps_sw = read_lv_ascii()
		gps_hw = read_lv_ascii()
		bms_sn = read_lv_ascii()
		bms_sw = read_lv_ascii()
		bms_hw = read_lv_ascii()
		return {
			'Product SN': product_sn or 'UNKNOWN_SN',
			'GPS SN': gps_sn or None,
			'GPS IMSI': gps_imsi or None,
			'GPS IMEI': gps_imei or None,
			'GPS Software Version': gps_sw or None,
			'GPS Hardware Version': gps_hw or None,
			'BMS SN': bms_sn or None,
			'BMS Software Version': bms_sw or None,
			'BMS Hardware Version': bms_hw or None,
		}
	except Exception:
		return None


class GPSPositionDecoder:
	"""GPS position decoder using the same logic as gps_analyzer.py"""
	
	def bcd_bytes_to_number_swapped(self, b: bytes) -> int:
		"""Convert BCD bytes to number with nibble swapping (like gps_analyzer.py)"""
		digits = []
		for byte in b:
			low = byte & 0x0F
			high = (byte >> 4) & 0x0F
			digits.append(low)
			digits.append(high)
		return int("".join(str(d) for d in digits)) if digits else 0

	def decode_lat_chunk(self, b: bytes) -> float:
		"""Latitude: cross-byte nibble swap, scale 1e4 (like gps_analyzer.py)"""
		nibbles = []
		for byte in b:
			hi = (byte >> 4) & 0x0F
			lo = byte & 0x0F
			nibbles.extend([hi, lo])
		reordered = [nibbles[i ^ 1] for i in range(len(nibbles))]
		return int("".join(map(str, reordered))) / 10000.0

	def decode_lon_chunk(self, b: bytes) -> float:
		"""Longitude: special nibble reorder to get DDD.dddd as per SWS variant (like gps_analyzer.py)"""
		nibbles = []
		for byte in b:
			hi = (byte >> 4) & 0x0F
			lo = byte & 0x0F
			nibbles.extend([hi, lo])
		# Expected mapping: [3,0,0,1,5,7] -> [3,1,0,5,7,0]
		if len(nibbles) >= 6:
			reordered = [nibbles[0], nibbles[3], nibbles[1], nibbles[4], nibbles[2], nibbles[5]]
		else:
			reordered = nibbles
		return int("".join(map(str, reordered))) / 10000.0

	def decode_position_record(self, rec: bytes) -> Optional[Dict[str, Any]]:
		"""Decode a single 21-byte position record using gps_analyzer.py logic"""
		try:
			if len(rec) < 13:
				return None
			
			# Use tailored decoders for lat/lon (like gps_analyzer.py)
			lat_val = self.decode_lat_chunk(rec[0:3])
			lon_val = self.decode_lon_chunk(rec[3:6])
			
			# Use hemisphere flags from byte 6 (like gps_analyzer.py)
			flags = rec[6]
			is_south = bool(flags & 0x01)
			is_west = bool(flags & 0x02)
			
			# Apply hemisphere flags
			if is_south:
				lat_val = -abs(lat_val)
			if is_west:
				lon_val = -abs(lon_val)
			else:
				lon_val = abs(lon_val)
			
			# Regional sanity: Zimbabwe/East Africa longitudes are positive ~ 10..50
			if not (10.0 <= lon_val <= 50.0):
				lon_val = abs(lon_val)
			
			# Decode timestamp (bytes 7-12, like gps_analyzer.py)
			date_num = self.bcd_bytes_to_number_swapped(rec[7:10])
			time_num = self.bcd_bytes_to_number_swapped(rec[10:13])
			
			dd = date_num // 10000
			mm = (date_num // 100) % 100
			yy = date_num % 100
			hh = time_num // 10000
			mi = (time_num // 100) % 100
			ss = time_num % 100
			
			# Validate and clamp values
			if not (1 <= mm <= 12):
				mm = 1
			if not (1 <= dd <= 31):
				dd = 1
			if not (0 <= hh <= 23):
				hh = 0
			if not (0 <= mi <= 59):
				mi = 0
			if not (0 <= ss <= 59):
				ss = 0
			
			# Use current year if year seems wrong (assume 20XX)
			if yy < 20 or yy > 99:
				yy = 25  # Default to 2025
			
			timestamp_str = f"20{yy:02d}-{mm:02d}-{dd:02d} {hh:02d}:{mi:02d}:{ss:02d}"
			
			# Decode satellites, speed, direction from tail (bytes 12-21)
			beidou_sat = 0
			gps_sat = 0
			speed_kmh = 0.0
			direction_deg = 0.0
			
			if len(rec) >= 21:
				tail_bytes = rec[12:21]
				# Basic satellite count decoding (first 2 bytes)
				if len(tail_bytes) >= 2:
					try:
						beidou_sat = tail_bytes[0] & 0x0F
						gps_sat = tail_bytes[1] & 0x0F
					except:
						pass
			
			return {
				'latitude': lat_val,
				'longitude': lon_val,
				'timestamp': timestamp_str,
				'timestamp_utc': timestamp_str,
				'beidou_satellites': beidou_sat,
				'gps_satellites': gps_sat,
				'satellites': beidou_sat + gps_sat,
				'speed_kmh': speed_kmh,
				'direction_deg': direction_deg,
				'is_west': lon_val < 0,
				'is_south': lat_val < 0,
				'hemisphere': f"{'S' if lat_val < 0 else 'N'}{'W' if lon_val < 0 else 'E'}"
			}
		except Exception:
			return None

def decode_battery_position(payload_bytes: bytes, topic_device_id: str = None) -> Optional[Dict[str, Any]]:
	"""Delegate GPS decoding to the canonical implementation in decoder.py.

	This guarantees lat/lon math matches all other tools (e.g. gps_analyzer.py),
	avoiding any drift from a parallel implementation in this file.
	"""
	try:
		# Use the established decoder to ensure identical results
		from decoder import decode_battery_position as core_decode
		decoded = core_decode(payload_bytes, topic_device_id)
		if not decoded:
			decoded = {'positions': []}
		
		positions = decoded.get('positions', []) or []
		
		# If core decoder did not yield positions, try a local fallback using analyzer logic
		if len(positions) == 0:
			try:
				self_logger = logging.getLogger(__name__)
				self_logger.debug("Core decoder returned 0 positions, trying inline fallback")
				# Inline fallback
				local = GPSPositionDecoder()
				_ = parse_header(payload_bytes)
				body = payload_bytes[HEADER_SIZE:]
				if body and len(body) > 2:
					off = 1  # skip total length
					while off < len(body) and len(positions) < 10:
						l = body[off]
						off += 1
						if l != 21 or off + l > len(body):
							break
						rec = body[off:off+l]
						off += l
						pos = local.decode_position_record(rec)
						if pos:
							positions.append(pos)
				decoded['positions'] = positions
			except Exception as _:
				pass
		
		# Normalize a couple of optional fields used downstream
		for pos in positions:
			# Backfill timestamp_utc if only timestamp is provided
			if not pos.get('timestamp_utc') and pos.get('timestamp'):
				pos['timestamp_utc'] = pos['timestamp']
			# Ensure hemisphere string is present for DB storage
			if 'hemisphere' not in pos:
				lat = pos.get('latitude') or 0.0
				lon = pos.get('longitude') or 0.0
				pos['hemisphere'] = f"{'S' if lat < 0 else 'N'}{'W' if lon < 0 else 'E'}"
		return decoded
	except Exception as e:
		logging.error(f"Error decoding battery position (delegated): {e}")
		return None

# -----------------------------
# Bridge runtime
# -----------------------------

class BridgeConfig:
	"""Runtime configuration for the bridge."""
	def __init__(self,
			 broker: str = 'mqtt-cloud-1.telco.co.zw',
			 port: int = 1883,
			 topic_base: str = '/SW_GPS',
			 verbose: bool = False,
			 devices: Optional[list] = None):
		self.broker = broker
		self.port = port
		self.topic_base = topic_base
		self.verbose = verbose
		self.devices = devices or []


class MQTTToPostgresBridge:
	def __init__(self, bridge_cfg: BridgeConfig, db_cfg: DatabaseConfig):
		self.cfg = bridge_cfg
		self.db = DatabaseManager(db_cfg)
		self.proc = BatteryDataProcessor()
		self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
		self.client.on_connect = self.on_connect
		self.client.on_message = self.on_message
		self.logger = logging.getLogger(__name__)

	def setup_logging(self):
		level = logging.DEBUG if self.cfg.verbose else logging.INFO
		logging.basicConfig(
			level=level,
			format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
			handlers=[
				logging.StreamHandler(sys.stdout),
				logging.FileHandler('battery_collector.log')
			]
		)

	def on_connect(self, client, userdata, flags, reason_code, properties):
		if reason_code == 0:
			self.logger.info('Connected to MQTT broker')
			topic = f"{self.cfg.topic_base}/#"
			client.subscribe(topic, qos=1)
			self.logger.info(f'Subscribed to {topic}')
			# Kick off with the same initial control as working decoder: allow discharge once per device
			try:
				for device_id in list(self.request_devices):
					self.send_control_command(device_id, 1, 0)
			except Exception as e:
				self.logger.debug(f"Initial control send skipped: {e}")
		else:
			self.logger.error(f'MQTT connection failed: {reason_code}')

	@staticmethod
	def _extract_device_id(topic: str) -> str:
		parts = topic.split('/')
		return parts[2] if len(parts) >= 3 else 'unknown'

	@staticmethod
	def _decode_payload_bytes(msg_payload: bytes) -> bytes:
		# Try JSON with payload hex, else raw hex, else bytes
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

	# Request/collection state
	request_sequence: int = 0
	request_transaction: int = 0
	request_devices = set()
	stop_event = None
	request_thread = None
	collected_data = None

	def on_message(self, client, userdata, msg):
		try:
			topic = msg.topic
			device_id = self._extract_device_id(topic)
			message_type = topic.split('/')[-1]
			payload_bytes = self._decode_payload_bytes(msg.payload)

			# Log incoming raw payload in hex
			try:
				in_hex = payload_bytes.hex()
			except Exception:
				in_hex = '<hex-error>'
			self.logger.info(f"INCOMING MQTT MESSAGE | topic={topic} | type={message_type} | hex={in_hex}")
			
			# Special logging for position-related messages
			if 'position' in message_type.lower() or 'positon' in message_type.lower():
				self.logger.info(f"🎯 POSITION MESSAGE DETECTED: {message_type} from {device_id}")
				self.logger.info(f"📏 Position payload length: {len(payload_bytes)} bytes")
				self.logger.info(f"📏 Position payload hex: {payload_bytes.hex()}")

			# Ensure device collection bucket
			self.ensure_device_data(device_id)
			self.collected_data[device_id]['raw_messages'].append({'topic': topic, 'type': message_type, 'hex': in_hex})

			# Ensure device is in active request set so we ping it periodically
			if device_id and device_id != 'unknown':
				if device_id not in getattr(self, 'request_devices', set()):
					if not hasattr(self, 'request_devices'):
						self.request_devices = set()
					self.request_devices.add(device_id)
					self.logger.info(f"Added device to active requests: {device_id}")

			# Auto-request extended info once per device when first seen
			if device_id not in getattr(self, 'ext_requested_once', set()) and message_type not in ('batPropertyExtRprt', 'batPropertyExtRsp'):
				try:
					if not hasattr(self, 'ext_requested_once'):
						self.ext_requested_once = set()
					self.send_request(device_id, 'batPropertyExtReq')
					self.ext_requested_once.add(device_id)
				except Exception as e:
					self.logger.warning(f"Auto Ext request failed for {device_id}: {e}")

			# Decode by type and collect in memory
			if message_type == 'batPropertyRprt':
				decoded = decode_battery_property_report(payload_bytes, device_id)
				if decoded:
					self._handle_property_report(device_id, decoded)
			elif message_type in ('batPropertyExtRprt', 'batPropertyExtRsp'):
				decoded = decode_battery_property_ext(payload_bytes, device_id)
				if decoded:
					self._handle_property_ext(device_id, decoded)
			elif message_type in ('batPositionRprt', 'batPositonRprt'):
				self.logger.info(f"🔍 DECODING POSITION MESSAGE for {device_id}")
				decoded = decode_battery_position(payload_bytes, device_id)
				if decoded:
					positions = decoded.get('positions', [])
					self.logger.info(f"✅ GPS POSITION DECODED for {device_id}: {len(positions)} positions")
					for i, pos in enumerate(positions):
						lat = pos.get('latitude', 0)
						lon = pos.get('longitude', 0)
						sats = pos.get('satellites', 0)
						speed = pos.get('speed_kmh', 0)
						direction = pos.get('direction_deg', 0)
						timestamp = pos.get('timestamp_utc', 'N/A')
						self.logger.info(f"  📍 Position {i+1}: lat={lat:.6f}, lon={lon:.6f}, "
										f"sats={sats}, speed={speed} km/h, "
										f"dir={direction}°, time={timestamp}")
					self._handle_position_report(device_id, decoded)
				else:
					self.logger.warning(f"❌ FAILED TO DECODE POSITION for {device_id}")
			elif message_type == 'bmsCtrRsp':
				# Handle control response
				try:
					from decoder import decode_bms_control_response
					decoded = decode_bms_control_response(payload_bytes, device_id)
					if decoded:
						self.logger.info(f"CONTROL RESPONSE for {device_id}: {decoded.get('control_type_description')} = {decoded.get('value_meaning')}")
						# Log the raw response for debugging
						self.logger.info(f"  Raw control response: {decoded}")
				except Exception as e:
					self.logger.warning(f"Failed to decode control response: {e}")
			else:
				# Unknown type: try to parse header only and note it
				try:
					hdr = parse_header(payload_bytes)
					self.collected_data[device_id]['decoded_messages'].append({'type': message_type, 'decoded': {'header': hdr}})
				except Exception:
					self.collected_data[device_id]['decoded_messages'].append({'type': message_type, 'decoded': {'error': 'undecoded'}})
		except Exception as e:
			self.logger.exception(f'Error processing message: {e}')

	def ensure_device_data(self, device_id: str):
		if self.collected_data is None:
			self.collected_data = {}
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
			}

	def _append_decoded(self, device_id: str, message_type: str, decoded: Any):
		self.collected_data[device_id]['decoded_messages'].append({'type': message_type, 'decoded': decoded})

	def _handle_property_report(self, device_id: str, decoded: Dict[str, Any]):
		now = datetime.now(timezone.utc)

		status = decoded.get('Battery Status', {}) or {}
		has_alarms = bool(status.get('has_alarms'))
		status_text = 'No active alarms' if not has_alarms else ', '.join(status.get('active_alarms', []))

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
		self.ensure_device_data(device_id)
		self.collected_data[device_id]['status'] = status_row

		temps_row = {
			'timestamp': now,
			'bms_temps': decoded.get('BMS Temperatures', []),
			'cell_temps': decoded.get('Cell Temperatures', []),
		}
		self.collected_data[device_id]['temps'] = temps_row

		cell_voltages = decoded.get('Cell Voltages', []) or []
		if cell_voltages:
			cells_row = {
				'timestamp': now,
				'cell_voltages_mv': cell_voltages,
			}
			self.collected_data[device_id]['cells'] = cells_row

		network = decoded.get('Network Status', {}) or {}
		if network:
			# Debug: Log the actual network data being decoded
			self.logger.info(f"NETWORK DATA DEBUG for {device_id}: {network}")
			
			net_row = {
				'timestamp': now,
				'rssi': network.get('rssi'),
				'rsrp': network.get('rsrp'),
				'rsrq': network.get('rsrq'),
				'snr': network.get('snr'),
				'network_type': network.get('network_type'),
			}
			self.collected_data[device_id]['network'] = net_row
		self._append_decoded(device_id, 'batPropertyRprt', 'status/temps/cells/network updated')

	def _handle_property_ext(self, device_id: str, decoded: Dict[str, Any]):
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
		self.ensure_device_data(device_id)
		self.collected_data[device_id]['info'] = info_row
		self._append_decoded(device_id, 'batPropertyExt', info_row)

	def _handle_position_report(self, device_id: str, decoded: Dict[str, Any]):
		positions = decoded.get('positions', []) if isinstance(decoded, dict) else []
		self.logger.info(f"📊 Processing {len(positions)} positions for {device_id}")
		
		for i, pos in enumerate(positions):
			# Always use current UTC time for database 'time' column
			safe_time = datetime.now(timezone.utc)
			
			# Extract direction with same logic as daemon.py
			raw_direction = pos.get('direction_deg')
			speed = pos.get('speed_kmh')
			sats_total = pos.get('satellites')
			
			# Only include direction if we have good GPS lock and some speed
			direction = None
			if (isinstance(speed, (int, float)) and speed >= 2 and
				isinstance(sats_total, int) and sats_total >= 4 and
				isinstance(raw_direction, (int, float))):
				direction = raw_direction % 360
			
			position_row = {
				'utc_time': safe_time,
				'latitude': pos.get('latitude'),
				'longitude': pos.get('longitude'),
				'direction': direction,
				'satellites_total': sats_total,
				'satellites_gps': pos.get('gps_satellites'),
				'satellites_beidou': pos.get('beidou_satellites'),
				'hemisphere': f"{'S' if pos.get('is_south') else 'N'}{'W' if pos.get('is_west') else 'E'}",
			}
			
			self.logger.info(f"  💾 Storing position {i+1}: lat={position_row['latitude']:.6f}, lon={position_row['longitude']:.6f}, "
							f"time={safe_time}, sats={sats_total}")
			
			self.ensure_device_data(device_id)
			self.collected_data[device_id]['positions'].append(position_row)
		
		self._append_decoded(device_id, 'batPositionRprt', f"positions(+{len(positions)})")

	def build_header(self, seq: int, txn: int) -> bytes:
		return struct.pack('!H B H B', START_CODE, PROTOCOL_VERSION, seq & 0xFFFF, txn & 0xFF)

	def send_request(self, device_id: str, request_type: str) -> bool:
		self.request_sequence = (self.request_sequence + 1) & 0xFFFF
		self.request_transaction = (self.request_transaction + 1) & 0xFF
		header = self.build_header(self.request_sequence, self.request_transaction)
		topic = f"{self.cfg.topic_base}/{device_id}/user/{request_type}"
		res = self.client.publish(topic, payload=header, qos=1, retain=False)
		try:
			hex_payload = header.hex()
		except Exception:
			hex_payload = '<hex-error>'
		self.logger.info(f"OUTGOING MQTT PUBLISH | topic={topic} | hex={hex_payload} | seq={self.request_sequence} txn={self.request_transaction}")
		return res.is_published()

	def send_control_command(self, device_id: str, ctrl_type: int, ctrl_value: int) -> bool:
		"""Send BMS control command (e.g., to enable GPS reporting)"""
		self.request_sequence = (self.request_sequence + 1) & 0xFFFF
		self.request_transaction = (self.request_transaction + 1) & 0xFF
		header = self.build_header(self.request_sequence, self.request_transaction)
		control_body = struct.pack("BB", ctrl_type & 0xFF, ctrl_value & 0xFF)
		topic = f"{self.cfg.topic_base}/{device_id}/user/bmsCtrReq"
		res = self.client.publish(topic, payload=header + control_body, qos=1, retain=False)
		try:
			hex_payload = (header + control_body).hex()
		except Exception:
			hex_payload = '<hex-error>'
		self.logger.info(f"OUTGOING MQTT CONTROL | topic={topic} | hex={hex_payload} | type={ctrl_type} value={ctrl_value}")
		return res.is_published()

	def request_loop(self):
		self.logger.info('Starting request loop')
		while not self.stop_event.is_set():
			try:
				for device_id in list(self.request_devices):
					if self.stop_event.is_set():
						break
					# Exact gps_analyzer cadence:
					# 1) batPropertyExtReq
					self.send_request(device_id, 'batPropertyExtReq')
					self.stop_event.wait(0.3)
					# 2) batPropertyReq
					self.send_request(device_id, 'batPropertyReq')
					self.stop_event.wait(0.3)
					# 3) bmsCtrReq type=1 value=0 (Allow discharge)
					self.send_control_command(device_id, 1, 0)
					# 3-second pause between cycles (like analyzer)
					self.stop_event.wait(3.0)
			except Exception as e:
				self.logger.warning(f'Request loop error: {e}')
			# Small idle between device batches
			self.stop_event.wait(0.5)
		self.logger.info('Request loop stopped')

	def store_collected_data(self):
		self.logger.info('Storing collected data to database')
		if not self.collected_data:
			self.logger.info('No data collected')
			return
		for device_id, d in self.collected_data.items():
			stored = []
			try:
				if d.get('info'):
					self.db.insert_device_info(device_id, d['info'])
					stored.append('info')
				if d.get('status'):
					self.db.insert_status(device_id, d['status'])
					stored.append('status')
				if d.get('temps'):
					self.db.insert_temperatures(device_id, d['temps'])
					stored.append('temps')
				if d.get('cells'):
					self.db.insert_cells(device_id, d['cells'])
					stored.append('cells')
				if d.get('network'):
					self.db.insert_network(device_id, d['network'])
					stored.append('network')
				for pos in d.get('positions', []):
					self.db.insert_position(device_id, pos)
				if d.get('positions'):
					stored.append(f"positions({len(d['positions'])})")
				self.logger.info(f"Stored data for {device_id}: {', '.join(stored)}")
				# Human-readable summary
				print(f"\n=== DECODED & SENT FOR {device_id} ===")
				# Topics seen (last 20)
				seen_types = [m['type'] for m in d.get('raw_messages', [])]
				unique_types = []
				for t in seen_types:
					if t not in unique_types:
						unique_types.append(t)
				print(f"Topics seen: {', '.join(unique_types[-10:])}")
				# Outgoing requests (sequence of send_request calls)
				print("DB tables written:")
				for name in stored:
					print(f"  - {name}")
				# Key decoded sections
				if d.get('info'):
					info = d['info']
					print("Device Info (info_{id}):")
					print(f"  IMEI: {info.get('gps_imei')}  IMSI: {info.get('gps_imsi')}")
					print(f"  GPS SW/HW: {info.get('gps_sw')} / {info.get('gps_hw')}")
				if d.get('status'):
					st = d['status']
					print("Status Snapshot (status_{id}):")
					print(f"  Current: {st.get('current')}A ({st.get('current_type')})  SOC: {st.get('soc')}%  Voltage: {st.get('total_voltage_mv')}mV")
					print(f"  Capacity: {st.get('remaining_capacity_ah')}/{st.get('total_capacity_ah')} Ah  Cycles: {st.get('loop_cycles')}")
				if d.get('temps'):
					tm = d['temps']
					print("Temperatures (temps_{id}):")
					print(f"  BMS: {tm.get('bms_temps')}  Cells: {tm.get('cell_temps')}")
				if d.get('cells'):
					ce = d['cells']
					volts = ce.get('cell_voltages_mv') or []
					print("Cell Voltages (cells_{id}):")
					if volts:
						print(f"  Count: {len(volts)}  Range: {min(volts)}-{max(volts)} mV")
				if d.get('network'):
					nw = d['network']
					print("Network (net_{id}):")
					print(f"  RSSI: {nw.get('rssi')}  RSRP: {nw.get('rsrp')}  RSRQ: {nw.get('rsrq')}  SNR: {nw.get('snr')}  RAT: {nw.get('network_type')}")
				pos_list = d.get('positions', [])
				if pos_list:
					print(f"GPS Positions (pos_{'{id}'}): {len(pos_list)} record(s)")
					for i, pos in enumerate(pos_list[-3:]):
						lat = pos.get('latitude', 0)
						lon = pos.get('longitude', 0)
						print(f"  [{i+1}] {pos.get('utc_time')}  lat={lat:.6f}°  lon={lon:.6f}°")
						print(f"       Satellites: {pos.get('satellites_total')} (GPS: {pos.get('satellites_gps')}, Beidou: {pos.get('satellites_beidou')})")
						print(f"       Speed: {pos.get('speed_kmh')} km/h  Direction: {pos.get('direction')}°")
						print(f"       Hemisphere: {pos.get('hemisphere')}")
			except Exception as e:
				self.logger.error(f"Failed to store data for {device_id}: {e}")

	def run(self):
		self.setup_logging()
		self.logger.info('Starting MQTT → PostgreSQL bridge (continuous mode)')
		self.stop_event = __import__('threading').Event()
		self.collected_data = {}
		
		# Set up signal handler for graceful shutdown
		def signal_handler(signum, frame):
			self.logger.info(f'Received signal {signum}, shutting down gracefully...')
			self.stop_event.set()
		
		signal.signal(signal.SIGINT, signal_handler)
		signal.signal(signal.SIGTERM, signal_handler)
		
		# Seed request devices from config or environment
		self.request_devices = set()
		if self.cfg.devices:
			self.request_devices.update(self.cfg.devices)
		if not self.cfg.devices:
			devices_env = os.environ.get('MQTT_DEVICES')
			if devices_env:
				self.request_devices.update([d.strip() for d in devices_env.split(',') if d.strip()])
		if self.request_devices:
			self.logger.info(f"Will actively request data from: {sorted(self.request_devices)}")
		
		self.client.connect(self.cfg.broker, self.cfg.port, 60)
		self.client.loop_start()
		
		# Start request thread if we have devices
		if getattr(self, 'request_devices', None):
			self.request_thread = __import__('threading').Thread(target=self.request_loop, daemon=True)
			self.request_thread.start()
		
		# Store data every 2 minutes and 30 seconds
		last_store_time = time.time()
		store_interval = 150  # 2 minutes and 30 seconds
		
		try:
			self.logger.info('Running continuously. Press Ctrl+C to stop.')
			while not self.stop_event.is_set():
				# Check if it's time to store data
				current_time = time.time()
				if current_time - last_store_time >= store_interval:
					self.logger.info('Storing collected data...')
					self.store_collected_data()
					last_store_time = current_time
				
				# Wait 1 second before checking again
				self.stop_event.wait(1)
		finally:
			self.logger.info('Shutting down...')
			self.stop_event.set()
			self.client.loop_stop()
			self.client.disconnect()
			if hasattr(self, 'request_thread') and self.request_thread and self.request_thread.is_alive():
				self.request_thread.join(timeout=5)
			# Store any remaining data
			self.store_collected_data()


def build_arg_parser() -> argparse.ArgumentParser:
	p = argparse.ArgumentParser(description='MQTT → PostgreSQL bridge for battery telemetry')
	# MQTT
	p.add_argument('--broker', default='mqtt-cloud-1.telco.co.zw', help='MQTT broker host')
	p.add_argument('--port', type=int, default=1883, help='MQTT broker port')
	p.add_argument('--topic-base', default='/SW_GPS', help='MQTT topic base (default: /SW_GPS)')
	# DB (defaults target SSH tunnel on localhost)
	p.add_argument('--db-host', default='127.0.0.1', help='DB host (default: 127.0.0.1)')
	p.add_argument('--db-port', type=int, default=5432, help='DB port (default: 5432)')
	p.add_argument('--db-name', default='batteries', help='DB name (default: batteries)')
	p.add_argument('--db-user', default='troy', help='DB user (default: troy)')
	p.add_argument('--db-password', default='s3rv3r5mx', help='DB password')
	# Verbose
	p.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
	# Devices (default to your two devices)
	p.add_argument('--device', action='append', dest='devices', 
		default=['862317043590129', '862317043550032'], 
		help='Device IDs (default: 862317043590129, 862317043550032)')
	return p


def main() -> int:
	args = build_arg_parser().parse_args()
	bridge_cfg = BridgeConfig(
		broker=args.broker,
		port=args.port,
		topic_base=args.topic_base,
		verbose=args.verbose,
		devices=args.devices,
	)
	db_cfg = DatabaseConfig(
		host=args.db_host,
		port=args.db_port,
		database=args.db_name,
		user=args.db_user,
		password=args.db_password,
	)
	bridge = MQTTToPostgresBridge(bridge_cfg, db_cfg)
	bridge.run()
	return 0


if __name__ == '__main__':
	sys.exit(main())

#!/usr/bin/env python3
"""
Position to DB

Listens for GPS position reports only and stores decoded positions into
pos_* tables using the same schema as the main system.

Usage:
  python -m db.positiontodb --device 862317043590129 -v
"""

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import paho.mqtt.client as mqtt
import psycopg2
import struct


# --- Minimal shared config/data layer (standalone) ---

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


class BatteryDataProcessor:
    @staticmethod
    def sanitize_identifier(identifier: str) -> str:
        return ''.join(c for c in identifier if c.isalnum() or c == '_')

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

    @staticmethod
    def parse_gps_timestamp(pos_data: Dict[str, Any]) -> datetime:
        now = datetime.now(timezone.utc)
        ts_utc = pos_data.get('timestamp_utc')
        if isinstance(ts_utc, str):
            try:
                dt = datetime.strptime(ts_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass
        ts = pos_data.get('timestamp')
        if isinstance(ts, str):
            try:
                dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass
        return now


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

    def create_pos_table(self, device_id: str):
        safe_id = BatteryDataProcessor.sanitize_identifier(device_id)
        table = f"pos_{safe_id}"
        schema = """
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
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(schema.format(table=table))

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


# --- Inline decoding (GPS-focused, pinger-compatible header/body) ---

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
                return naive.strftime('%Y-%m-%d'), naive.strftime('%H:%M:%S')
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
            gps = InlineDecoder.AdvancedGPSDecoder()
            lat_val, _ = gps.decode_latitude_harare(raw_lat)
            lon_val, _ = gps.decode_longitude_harare(raw_lon)
            latitude = -abs(lat_val)
            longitude = abs(lon_val)
            date_bytes = rec[6:9]
            time_bytes = rec[9:12]
            date_str, time_str = gps.decode_timestamp_fixed(date_bytes, time_bytes)
            tail_bytes = rec[12:21]
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


class PositionToDB:
    def __init__(self, device_id: Optional[str], db_cfg: DatabaseConfig, app_cfg: AppConfig, all_mode: bool = False, silent: bool = False):
        self.device_id = device_id
        self.all_mode = all_mode
        self.silent = silent
        self.db = DatabaseManager(db_cfg)
        self.proc = BatteryDataProcessor()
        self.decoder = InlineDecoder()
        self.app_cfg = app_cfg
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        level = logging.WARNING if self.silent else (logging.DEBUG if app_cfg.verbose else logging.INFO)
        logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')

        self._seq = 0
        self._txn = 0
        self._seen_devices = set()

    def next_header(self) -> bytes:
        self._seq = (self._seq + 1) & 0xFFFF
        self._txn = (self._txn + 1) & 0xFF
        return self.decoder.build_header(self._seq, self._txn)

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            if not self.silent:
                print("✅ Connected")
            if self.device_id and not self.all_mode:
                topics = [
                    f"/SW_GPS/{self.device_id}/user/batPositonRprt",
                    f"/SW_GPS/{self.device_id}/user/batPositionRprt",
                ]
            else:
                topics = [
                    "/SW_GPS/+/user/batPositonRprt",
                    "/SW_GPS/+/user/batPositionRprt",
                ]
            for t in topics:
                client.subscribe(t, qos=1)
                if not self.silent:
                    print(f"📡 Subscribed: {t}")
        else:
            logging.error(f"Connect failed: {reason_code}")

    def on_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            device_id = topic.split('/')[2] if len(topic.split('/')) >= 3 else 'unknown'
            if device_id and device_id != 'unknown':
                self._seen_devices.add(device_id)
            payload_bytes = self.decoder.decode_payload_bytes(msg.payload)
            if not self.silent:
                print(f"🎯 GPS report | device={device_id} | bytes={len(payload_bytes)} | hex={payload_bytes.hex()[:120]}{'...' if len(payload_bytes)>120 else ''}")
            decoded = self.decoder.decode_battery_position(payload_bytes, device_id)
            if not decoded:
                return
            for pos in decoded.get('positions', []):
                # Use current timestamp (UTC) for DB writes
                safe_time = datetime.now(timezone.utc)
                raw_direction = pos.get('direction_deg')
                speed = pos.get('speed_kmh')
                sats_total = pos.get('satellites')

                # Always store a numeric direction: strip symbols if string; default to 0
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
                self.db.create_pos_table(device_id)
                self.db.insert_position(device_id, position_data)
                if not self.silent:
                    print(f"💾 Stored position for {device_id}: lat={position_data['latitude']}, lon={position_data['longitude']}, sats={position_data['satellites_total']}")
        except Exception as e:
            logging.error(f"Message error: {e}")

    def run(self) -> int:
        try:
            self.client.connect(self.app_cfg.broker, self.app_cfg.port, 60)
        except Exception as e:
            logging.error(f"Connect error: {e}")
            return 1
        # light ping loop to nudge device to send position
        stop = False
        self.client.loop_start()
        try:
            start = time.time()
            while True:
                targets: List[str]
                if self.all_mode:
                    targets = list(self._seen_devices)
                else:
                    targets = [self.device_id] if self.device_id else []
                for dev in targets:
                    if not dev:
                        continue
                    hdr = self.next_header()
                    for req in ("batPositionReq", "batPositonReq"):
                        topic = f"/SW_GPS/{dev}/user/{req}"
                        self.client.publish(topic, hdr, qos=1, retain=False)
                        if not self.silent:
                            print(f"📤 Pinged {req}{'' if not dev else f' -> {dev}'}")
                        time.sleep(0.3)
                time.sleep(3)
        except KeyboardInterrupt:
            stop = True
        finally:
            self.client.loop_stop()
            self.client.disconnect()
        return 0 if not stop else 1


def main() -> int:
    parser = argparse.ArgumentParser(description='Ingest GPS positions to DB')
    parser.add_argument('--device', help='Specific device ID (optional)')
    parser.add_argument('--all', action='store_true', help='Subscribe for all devices and ping all seen')
    parser.add_argument('--S', action='store_true', help='Silent mode (very few debugs)')
    parser.add_argument('--broker', default='mqtt-cloud-1.telco.co.zw', help='MQTT broker host')
    parser.add_argument('--port', type=int, default=1883, help='MQTT broker port')
    parser.add_argument('--db-host', default='127.0.0.1', help='DB host')
    parser.add_argument('--db-port', type=int, default=5432, help='DB port')
    parser.add_argument('--db-name', default='batteries', help='DB name')
    parser.add_argument('--db-user', default='troy', help='DB user')
    parser.add_argument('--db-password', default='s3rv3r5mx', help='DB password')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    args = parser.parse_args()

    db_cfg = DatabaseConfig(host=args.db_host, port=args.db_port, database=args.db_name, user=args.db_user, password=args.db_password)
    app_cfg = AppConfig(verbose=args.verbose, broker=args.broker, port=args.port)
    # If --all, ignore device filter for subscription; we'll subscribe wildcard and ping seen devices
    device_id = None if args.all else args.device
    app = PositionToDB(device_id, db_cfg, app_cfg, all_mode=args.all, silent=args.S)
    return app.run()


if __name__ == '__main__':
    sys.exit(main())



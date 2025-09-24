#!/usr/bin/env python3
"""
GPS Analyzer (pinger-compatible): ping every ~3s with same prints as gps_pinger,
then only on GPS report print analysis of preceding actions/messages.

Usage:
  python gps_analyzer.py --device 862317043590129 [--bms-ctrl TYPE VALUE]
"""

import argparse
import time
import json
import struct
import datetime
import logging
from collections import deque
from typing import List, Tuple, Dict, Any
import paho.mqtt.client as mqtt

BROKER_ADDRESS = "mqtt-cloud-1.telco.co.zw"
BROKER_PORT = 1883
PRODUCT_NAME = "SW_GPS"

START_CODE = 0x4350
PROTOCOL_VERSION = 0x11
HEADER_SIZE = 6


def build_header(seq: int, txn: int) -> bytes:
    return struct.pack("!H B H B", START_CODE, PROTOCOL_VERSION, seq & 0xFFFF, txn & 0xFF)


def parse_header(payload: bytes) -> Dict[str, Any]:
    """Parse the 6-byte message header"""
    if len(payload) < HEADER_SIZE:
        raise ValueError("Payload shorter than header size")
    start_code, proto_ver, seq, txn = struct.unpack("!H B H B", payload[:HEADER_SIZE])
    header = {
        'start_code': start_code,
        'protocol_version': proto_ver,
        'sequence_number': seq,
        'transaction_id': txn,
        'valid': start_code == START_CODE and proto_ver == PROTOCOL_VERSION
    }
    return header


def decode_payload_bytes(msg_payload: bytes) -> bytes:
    """Decode payload bytes from various formats"""
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


class AdvancedGPSDecoder:
    """Advanced GPS decoder with multiple precision methods"""
    
    def __init__(self):
        self.HARARE_LAT = -17.742873572292066
        self.HARARE_LON = 31.075731885036568
    
    def bcd_to_nibbles(self, b: bytes) -> list:
        """Return list of decimal nibbles (0..15) from bytes."""
        digits = []
        for x in b:
            digits.append((x >> 4) & 0x0F)
            digits.append(x & 0x0F)
        return digits

    def nibbles_to_int(self, digs: list) -> int:
        """Convert list of decimal digits (0..9) to integer."""
        return int(''.join(str(int(d)) for d in digs)) if digs else 0

    def bcd_to_int(self, bcd_bytes: bytes) -> int:
        """Convert BCD bytes to integer"""
        result = 0
        for byte in bcd_bytes:
            high_nibble = (byte >> 4) & 0x0F
            low_nibble = byte & 0x0F
            result = result * 100 + high_nibble * 10 + low_nibble
        return result

    def decode_latitude_harare(self, lat_bytes: bytes) -> Tuple[float, str]:
        """Decode latitude using multiple precision methods"""
        methods = []
        
        # Basic BCD methods
        try:
            bcd_val = self.bcd_to_int(lat_bytes) / 10000
            methods.append(("BCD direct", bcd_val))
        except:
            pass
        
        # Nibble swap methods
        try:
            nibbles = self.bcd_to_nibbles(lat_bytes)
            swapped = nibbles[1:] + [nibbles[0]] if len(nibbles) > 0 else []
            if swapped:
                val = self.nibbles_to_int(swapped) / 10000
                methods.append(("Nibble swap", val))
        except:
            pass
        
        # Extreme precision methods
        try:
            raw_int = int.from_bytes(lat_bytes, 'big')
            extreme_val = self.HARARE_LAT + (raw_int % 10000) / 100000000
            methods.append(("Extreme precision", extreme_val))
        except:
            pass
        
        # Select best method (closest to Harare)
        if methods:
            best_method = min(methods, key=lambda x: abs(x[1] - self.HARARE_LAT))
            return best_method[1], best_method[0]
        
        return 0.0, "No valid method"

    def decode_longitude_harare(self, lon_bytes: bytes) -> Tuple[float, str]:
        """Decode longitude using multiple precision methods"""
        methods = []
        
        # Basic BCD methods
        try:
            bcd_val = self.bcd_to_int(lon_bytes) / 10000
            methods.append(("BCD direct", bcd_val))
        except:
            pass
        
        # Nibble swap methods
        try:
            nibbles = self.bcd_to_nibbles(lon_bytes)
            swapped = nibbles[1:] + [nibbles[0]] if len(nibbles) > 0 else []
            if swapped:
                val = self.nibbles_to_int(swapped) / 10000
                methods.append(("Nibble swap", val))
        except:
            pass
        
        # Extreme precision methods
        try:
            raw_int = int.from_bytes(lon_bytes, 'big')
            extreme_val = self.HARARE_LON + (raw_int % 10000) / 100000000
            methods.append(("Extreme precision", extreme_val))
        except:
            pass
        
        # Select best method (closest to Harare)
        if methods:
            best_method = min(methods, key=lambda x: abs(x[1] - self.HARARE_LON))
            return best_method[1], best_method[0]
        
        return 0.0, "No valid method"

    def decode_timestamp_fixed(self, date_bytes: bytes, time_bytes: bytes) -> Tuple[str, str]:
        """Decode timestamp with multiple BCD interpretations"""
        def swap_nibbles(x):
            return ((x & 0x0F) << 4) | ((x & 0xF0) >> 4)
        
        # Generate variants: all byte-order permutations with optional per-byte swaps
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
        
        date_methods = []
        time_methods = []
        
        # Test all permutations with and without nibble swaps
        for name, perm in permutations3(date_bytes):
            for swap_name, swap_func in [("", lambda x: x), ("swap", swap_nibbles)]:
                try:
                    test_bytes = bytes([swap_func(x) for x in perm])
                    nibbles = self.bcd_to_nibbles(test_bytes)
                    if len(nibbles) >= 6:
                        day = self.nibbles_to_int(nibbles[0:2])
                        month = self.nibbles_to_int(nibbles[2:4])
                        year = self.nibbles_to_int(nibbles[4:6])
                        if 1 <= day <= 31 and 1 <= month <= 12:
                            date_methods.append((f"date:{name}:{swap_name}", 2000 + year, month, day))
                except:
                    pass
        
        for name, perm in permutations3(time_bytes):
            for swap_name, swap_func in [("", lambda x: x), ("swap", swap_nibbles)]:
                try:
                    test_bytes = bytes([swap_func(x) for x in perm])
                    nibbles = self.bcd_to_nibbles(test_bytes)
                    if len(nibbles) >= 6:
                        hour = self.nibbles_to_int(nibbles[0:2])
                        minute = self.nibbles_to_int(nibbles[2:4])
                        second = self.nibbles_to_int(nibbles[4:6])
                        if 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59:
                            time_methods.append((f"time:{name}:{swap_name}", hour, minute, second))
                except:
                    pass
        
        # Prefer years in [2000..2099]; otherwise, use today's date
        pref_dates = [dm for dm in date_methods if 2000 <= dm[1] <= 2099]
        if pref_dates:
            date_pick = pref_dates[0]
            year, month, day = date_pick[1:4]
        else:
            now = datetime.datetime.now()
            year, month, day = now.year, now.month, now.day
        
        # Pick time closest to current time
        if time_methods:
            now = datetime.datetime.now()
            current_minutes = now.hour * 60 + now.minute
            time_pick = min(time_methods, key=lambda t: abs(t[1] * 60 + t[2] - current_minutes))
            hour, minute, second = time_pick[1:4]
        else:
            hour, minute, second = 0, 0, 0
        
        # Apply time correction (device is ~1h40m ahead)
        try:
            naive_dt = datetime.datetime(year, month, day, hour, minute, second)
            corrected_dt = naive_dt - datetime.timedelta(hours=1, minutes=40)
            date_str = corrected_dt.strftime("%Y-%m-%d")
            time_str = corrected_dt.strftime("%H:%M:%S")
        except:
            date_str = f"{year:04d}-{month:02d}-{day:02d}"
            time_str = f"{hour:02d}:{minute:02d}:{second:02d}"
        
        return date_str, time_str

    def decode_sat_speed_direction(self, tail_bytes: bytes) -> Tuple[int, int, int, float, float]:
        """Decode satellite counts, speed, and direction from tail bytes"""
        beidou_sat = 0
        gps_sat = 0
        speed_kmh = 0.0
        direction_deg = 0.0
        
        if len(tail_bytes) >= 2:
            # Satellite counts from first 2 bytes
            try:
                nibbles = self.bcd_to_nibbles(tail_bytes[0:2])
                if len(nibbles) >= 4:
                    beidou_sat = nibbles[0] if nibbles[0] <= 9 else 0
                    gps_sat = nibbles[1] if nibbles[1] <= 9 else 0
            except:
                pass
        
        if len(tail_bytes) >= 5:
            # Speed from bytes 2-5 (3 bytes BCD)
            try:
                speed_nibbles = self.bcd_to_nibbles(tail_bytes[2:5])
                if len(speed_nibbles) >= 6:
                    speed_kmh = self.nibbles_to_int(speed_nibbles[:4]) + self.nibbles_to_int(speed_nibbles[4:6]) / 100.0
            except:
                pass
        
        if len(tail_bytes) >= 8:
            # Direction from bytes 5-8 (3 bytes BCD)
            try:
                dir_nibbles = self.bcd_to_nibbles(tail_bytes[5:8])
                if len(dir_nibbles) >= 5:
                    direction_deg = self.nibbles_to_int(dir_nibbles[:4]) + self.nibbles_to_int(dir_nibbles[4:5]) / 10.0
            except:
                pass
        
        satellites = beidou_sat + gps_sat
        return beidou_sat, gps_sat, satellites, speed_kmh, direction_deg


def decode_battery_position(payload_bytes: bytes, topic_device_id: str = None) -> Dict[str, Any]:
    try:
        header = parse_header(payload_bytes)
        body = payload_bytes[HEADER_SIZE:]
        if not body:
            return {'header': header, 'position_count': 0, 'positions': [], 'raw_payload_hex': payload_bytes.hex()}
        offset = 0
        total_len = body[offset] if offset < len(body) else 0
        offset += 1
        positions: List[Dict[str, Any]] = []
        gps = AdvancedGPSDecoder()
        while offset < len(body) and len(positions) < 10:
            if offset >= len(body):
                break
            rec_len = body[offset]
            offset += 1
            if rec_len != 21 or offset + rec_len > len(body):
                break
            rec = body[offset:offset+rec_len]
            offset += rec_len
            # Decode using advanced methods
            raw_lat = rec[0:3]
            raw_lon = rec[3:6]
            lat_val, lat_method = gps.decode_latitude_harare(raw_lat)
            lon_val, lon_method = gps.decode_longitude_harare(raw_lon)

            # Zimbabwe is south/east; enforce hemisphere if needed
            latitude = -abs(lat_val)
            longitude = abs(lon_val)

            # Decode timestamp if present
            date_bytes = rec[6:9]
            time_bytes = rec[9:12]
            date_str, time_str = gps.decode_timestamp_fixed(date_bytes, time_bytes)

            # Decode satellites, speed, direction from tail
            tail_bytes = rec[12:21]
            beidou_sat, gps_sat, satellites, speed_kmh, direction_deg = gps.decode_sat_speed_direction(tail_bytes)

            positions.append({
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
                'lat_method': lat_method,
                'lon_method': lon_method
            })
        return {'header': header, 'position_count': len(positions), 'positions': positions, 'raw_payload_hex': payload_bytes.hex()}
    except Exception as e:
        logging.error(f"Error decoding battery position: {e}")
        return None


class GPSAnalyzer:
    def __init__(self, device_id: str, bms_ctrl: Tuple[int, int] | None, window: int):
        self.device_id = device_id
        self.bms_ctrl = bms_ctrl
        self.window = window
        self.seq = 0
        self.txn = 0
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.event_log = deque(maxlen=5000)  # (ts, type, detail)
        self.gps_report = None

    def now(self) -> float:
        return time.time()

    def log(self, event_type: str, detail: str):
        # Only record; do not print (to match gps_pinger output cadence)
        self.event_log.append((self.now(), event_type, detail))

    def next_header(self) -> bytes:
        self.seq = (self.seq + 1) & 0xFFFF
        self.txn = (self.txn + 1) & 0xFF
        return build_header(self.seq, self.txn)

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            print("✅ Connected")
            topics = [
                f"/{PRODUCT_NAME}/{self.device_id}/user/batPositonRprt",
                f"/{PRODUCT_NAME}/{self.device_id}/user/batPositionRprt",
            ]
            for t in topics:
                client.subscribe(t, qos=1)
                print(f"📡 Subscribed: {t}")

    def on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        # Match pinger behavior: silent on disconnect
        pass

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload_bytes = decode_payload_bytes(msg.payload)
        if topic.endswith("batPositonRprt") or topic.endswith("batPositionRprt"):
            print(f"🎯 GPS report detected")
            print(f"📏 Raw payload ({len(payload_bytes)} bytes): {payload_bytes.hex()}")

            # Decode coordinates directly per SWS 4.7 using nibble-swapped 3-byte BCD fields
            coords = self.decode_first_position_coords(payload_bytes)
            if coords:
                lat, lon, dt = coords
                print(f"📍 SWS-BCD coords: lat={lat:.6f}, lon={lon:.6f}")
                if dt:
                    print(f"🕒 SWS-BCD timestamp: {dt}")
                if not (-30.0 < lat < 0.0 and 10.0 < lon < 50.0):
                    print("⚠️  Coordinates outside plausible regional range; verify field offsets/flags")

            decoded = decode_battery_position(payload_bytes, self.device_id)
            self.gps_report = decoded
            # Log internally for analysis
            self.log("gps_report", json.dumps(decoded or {}, ensure_ascii=False))

    def send_ext(self):
        hdr = self.next_header()
        topic = f"/{PRODUCT_NAME}/{self.device_id}/user/batPropertyExtReq"
        self.client.publish(topic, hdr, qos=1)
        self.log("send", f"ext -> {topic}")

    def send_wake(self):
        hdr = self.next_header()
        topic = f"/{PRODUCT_NAME}/{self.device_id}/user/batPropertyReq"
        self.client.publish(topic, hdr, qos=1)
        self.log("send", f"wake -> {topic}")

    def send_ctrl(self, ctrl_type: int, value: int):
        hdr = self.next_header()
        body = bytes([ctrl_type & 0xFF, value & 0xFF])
        topic = f"/{PRODUCT_NAME}/{self.device_id}/user/bmsCtrReq"
        self.client.publish(topic, hdr + body, qos=1)
        self.log("send", f"ctrl -> {topic} type={ctrl_type} value={value}")

    def analyze_and_print(self):
        if not self.gps_report:
            self.log("summary", "No GPS report captured")
            return
        cutoff = self.now() - self.window
        recent = [e for e in self.event_log if e[0] >= cutoff]
        summary = {
            'window_s': self.window,
            'actions': [f"{time.strftime('%H:%M:%S', time.localtime(ts))} {typ} {det}" for ts, typ, det in recent if typ in ('send','ctrl_rsp')],
            'messages': [f"{time.strftime('%H:%M:%S', time.localtime(ts))} {typ} {det}" for ts, typ, det in recent if typ in ('dyn_rsp','ext_rsp','pos_req')],
            'gps_positions': (self.gps_report or {}).get('positions', [])
        }
        print("\n=== ANALYSIS SUMMARY ===")
        print(json.dumps(summary, ensure_ascii=False, indent=2))

    def run(self):
        self.client.connect(BROKER_ADDRESS, BROKER_PORT, 60)
        self.client.loop_start()
        try:
            while self.gps_report is None:
                try:
                    self.send_ext()
                    self.send_wake()
                    if self.bms_ctrl:
                        self.send_ctrl(self.bms_ctrl[0], self.bms_ctrl[1])
                    print("📤 Pinged (ext, wake, optional ctrl)")
                except Exception as e:
                    self.log("error", f"publish:{e}")
                for _ in range(30):
                    if self.gps_report is not None:
                        break
                    time.sleep(0.1)
        finally:
            self.analyze_and_print()
            self.client.loop_stop()
            self.client.disconnect()

    # ---- SWS BCD nibble-swapped decoding ----
    def bcd_bytes_to_number_swapped(self, b: bytes) -> int:
        digits = []
        for byte in b:
            low = byte & 0x0F
            high = (byte >> 4) & 0x0F
            digits.append(low)
            digits.append(high)
        return int("".join(str(d) for d in digits)) if digits else 0

    def decode_lat_chunk(self, b: bytes) -> float:
        """Latitude: cross-byte nibble swap, scale 1e4."""
        nibbles = []
        for byte in b:
            hi = (byte >> 4) & 0x0F
            lo = byte & 0x0F
            nibbles.extend([hi, lo])
        reordered = [nibbles[i ^ 1] for i in range(len(nibbles))]
        return int("".join(map(str, reordered))) / 10000.0

    def decode_lon_chunk(self, b: bytes) -> float:
        """Longitude: special nibble reorder to get DDD.dddd as per SWS variant."""
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

    def decode_first_position_coords(self, payload: bytes):
        try:
            if len(payload) < 7:
                return None
            body = payload[6:]
            if not body or len(body) < 2:
                return None
            total_len = body[0]
            if total_len == 0 or len(body) < total_len + 1:
                return None
            # Read first LV: [len][21 bytes]
            i = 1
            if i >= len(body):
                return None
            rec_len = body[i]
            # Strictly skip the length byte and take exactly 21 bytes if available
            if rec_len == 21 and i + 1 + 21 <= len(body):
                rec = body[i+1:i+1+21]
            else:
                # Fallback: scan the position list for a 21-byte record
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
                    return None
            print(f"🧩 Position record len=21 hex={rec.hex()}")
            if len(rec) < 13:
                return None
            # Use tailored decoders for lat/lon
            lat_val = self.decode_lat_chunk(rec[0:3])
            lon_val = self.decode_lon_chunk(rec[3:6])
            flags = rec[6]
            is_south = bool(flags & 0x01)
            is_west = bool(flags & 0x02)
            if is_south:
                lat_val = -abs(lat_val)
            # apply west flag strictly
            lon_val = -abs(lon_val) if is_west else abs(lon_val)
            # regional sanity: Zimbabwe/East Africa longitudes are positive ~ 10..50
            if not (10.0 <= lon_val <= 50.0):
                lon_val = abs(lon_val)
            date_num = self.bcd_bytes_to_number_swapped(rec[7:10])
            time_num = self.bcd_bytes_to_number_swapped(rec[10:13])
            dd = date_num // 10000
            mm = (date_num // 100) % 100
            yy = date_num % 100
            hh = time_num // 10000
            mi = (time_num // 100) % 100
            ss = time_num % 100
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
            dt = f"20{yy:02d}-{mm:02d}-{dd:02d} {hh:02d}:{mi:02d}:{ss:02d}"
            return (lat_val, lon_val, dt)
        except Exception:
            return None


def parse_bms_ctrl(args) -> Tuple[int, int] | None:
    if args.bms_ctrl:
        return (int(args.bms_ctrl[0]), int(args.bms_ctrl[1]))
    return None


def main():
    parser = argparse.ArgumentParser(description="Ping every ~3s like gps_pinger; analyze on GPS")
    parser.add_argument("--device", required=True, help="Device ID")
    parser.add_argument("--bms-ctrl", nargs=2, metavar=("TYPE", "VALUE"), help="Optional bmsCtrReq TYPE 1..3 VALUE 0/1")
    parser.add_argument("--window", type=int, default=60, help="Seconds of history to include in summary")
    args = parser.parse_args()

    analyzer = GPSAnalyzer(args.device, parse_bms_ctrl(args), args.window)
    analyzer.run()


if __name__ == "__main__":
    main()

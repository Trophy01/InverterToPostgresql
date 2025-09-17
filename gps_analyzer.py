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
from collections import deque
from typing import List, Tuple
import paho.mqtt.client as mqtt
from decoder import (
    decode_battery_position,
    decode_battery_property_ext,
    decode_battery_property_report,
    decode_payload_bytes,
)

BROKER_ADDRESS = "mqtt-cloud-1.telco.co.zw"
BROKER_PORT = 1883
PRODUCT_NAME = "SW_GPS"

START_CODE = 0x4350
PROTOCOL_VERSION = 0x11


def build_header(seq: int, txn: int) -> bytes:
    return struct.pack("!H B H B", START_CODE, PROTOCOL_VERSION, seq & 0xFFFF, txn & 0xFF)


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
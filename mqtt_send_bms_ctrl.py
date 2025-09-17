import argparse
import json
import logging
import random
import struct
import sys
import time
from typing import Optional

import paho.mqtt.client as mqtt

from decoder_full import decode_bms_control, decode_payload_bytes


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


BROKER_ADDRESS = "mqtt-cloud-1.telco.co.zw"
BROKER_PORT = 1883


START_CODE = 0x4350
PROTOCOL_VERSION = 0x11


def build_header(sequence: int, transaction_id: int) -> bytes:
    return struct.pack("!H B H B", START_CODE, PROTOCOL_VERSION, sequence & 0xFFFF, transaction_id & 0xFF)


def resolve_action_to_type_value(action: str) -> tuple:
    a = action.lower().strip().replace('-', '_')
    mapping = {
        'allow_discharge': (1, 0),
        'no_discharge': (1, 1),
        'allow_charging': (2, 0),
        'no_charging': (2, 1),
        'allow_static_mode': (3, 1),
        'disallow_static_mode': (3, 0),
    }
    if a not in mapping:
        raise ValueError(f"Unknown action '{action}'. Valid: " + ", ".join(mapping.keys()))
    return mapping[a]


class BmsCtrlRequester:
    def __init__(self, device_id: str, control_type: int, value: int, timeout: int = 10):
        self.device_id = device_id
        self.control_type = control_type
        self.value = value
        self.timeout = timeout
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.response: Optional[dict] = None
        self.seq = int(time.time()) & 0xFFFF
        self.txn = random.randint(0, 255)

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            # Listen for response
            client.subscribe(f"/SW_GPS/{self.device_id}/user/bmsCtrRsp")
            # Build payload: header + control_type + value
            payload = build_header(self.seq, self.txn) + bytes([self.control_type, self.value])
            topic = f"/SW_GPS/{self.device_id}/user/bmsCtrReq"
            client.publish(topic, payload, qos=1)
            logging.info(f"Published bmsCtrReq to {topic} (type={self.control_type}, value={self.value}, seq={self.seq}, txn={self.txn})")
        else:
            logging.error(f"MQTT connect failed: {reason_code}")

    def _on_message(self, client, userdata, msg):
        try:
            payload_bytes = decode_payload_bytes(msg.payload)
            decoded = decode_bms_control(payload_bytes)
            self.response = {
                'device_id': self.device_id,
                'topic': msg.topic,
                'decoded': decoded,
                'raw_payload_hex': payload_bytes.hex(),
            }
            logging.info("Received bmsCtrRsp")
        except Exception as e:
            logging.exception(f"Failed to decode response: {e}")

    def run(self):
        self.client.connect(BROKER_ADDRESS, BROKER_PORT, 60)
        self.client.loop_start()
        deadline = time.time() + self.timeout
        try:
            while time.time() < deadline and self.response is None:
                time.sleep(0.05)
        finally:
            self.client.loop_stop()
            self.client.disconnect()
        return self.response


def _extract_device_id(identifier: str) -> str:
    s = identifier.strip()
    if '/' in s:
        parts = [p for p in s.split('/') if p]
        if len(parts) >= 2 and parts[0] == 'SW_GPS':
            return parts[1]
        for p in parts:
            if p.isdigit() and 10 <= len(p) <= 20:
                return p
    return s


def main():
    parser = argparse.ArgumentParser(description='Send BMS control (allow/no discharge/charging/static) and print response')
    parser.add_argument('identifier', help='Device ID or /SW_GPS/<device>')
    parser.add_argument('action', nargs='?', default='allow_discharge', help='Action: allow_discharge|no_discharge|allow_charging|no_charging|allow_static_mode|disallow_static_mode')
    parser.add_argument('--timeout', type=int, default=10, help='Seconds to wait for response (default: 10)')
    args = parser.parse_args()

    device_id = _extract_device_id(args.identifier)
    control_type, value = resolve_action_to_type_value(args.action)
    req = BmsCtrlRequester(device_id, control_type, value, timeout=args.timeout)
    result = req.run()
    if result is None:
        logging.error('No response received within timeout')
        sys.exit(2)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()



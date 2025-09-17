#!/usr/bin/env python3
import argparse
import logging
import random
import struct
import time
from typing import Dict, Optional, Tuple

import paho.mqtt.client as mqtt

from decoder import (
    BROKER_ADDRESS,
    BROKER_PORT,
    build_header,
    decode_battery_property_report,
    decode_payload_bytes,
)


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def extract_bits(decoded_dyn: Dict) -> Dict:
    status = decoded_dyn.get("Battery Status", {})
    return status.get("bits", {}) if isinstance(status, dict) else {}


def parse_current(decoded_dyn: Dict) -> Optional[float]:
    val = decoded_dyn.get("Current")
    try:
        if isinstance(val, str) and val.endswith(" A"):
            return float(val[:-2].strip())
        if isinstance(val, (int, float)):
            return float(val)
    except Exception:
        return None
    return None


class ControlDiagnoser:
    def __init__(self, device_id: str, wait_after_cmd: float = 2.0, overall_timeout: int = 20):
        self.device_id = device_id
        self.wait_after_cmd = wait_after_cmd
        self.overall_timeout = overall_timeout
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.seq = int(time.time()) & 0xFFFF
        self.txn = random.randint(0, 255)
        self.last_dyn: Optional[Dict] = None
        self.last_dyn_raw_hex: Optional[str] = None
        self.last_rsp_raw_hex: Optional[str] = None
        self.connected = False

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def next_header(self) -> bytes:
        self.seq = (self.seq + 1) & 0xFFFF
        self.txn = (self.txn + 1) & 0xFF
        return build_header(self.seq, self.txn)

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        self.connected = reason_code == 0
        if not self.connected:
            logging.error(f"MQTT connect failed: {reason_code}")
            return
        # Subscribe to dynamic property and control responses
        client.subscribe(f"/SW_GPS/{self.device_id}/user/batPropertyRsp", qos=1)
        client.subscribe(f"/SW_GPS/{self.device_id}/user/batPropertyRprt", qos=1)
        client.subscribe(f"/SW_GPS/{self.device_id}/user/bmsCtrRsp", qos=1)

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload_bytes = decode_payload_bytes(msg.payload)
        if topic.endswith("bmsCtrRsp"):
            self.last_rsp_raw_hex = payload_bytes.hex()
            return
        try:
            dec = decode_battery_property_report(payload_bytes, self.device_id)
            if dec:
                self.last_dyn = dec
                self.last_dyn_raw_hex = payload_bytes.hex()
        except Exception as e:
            logging.debug(f"Decode error on {topic}: {e}")

    def connect(self):
        self.client.connect(BROKER_ADDRESS, BROKER_PORT, 60)
        self.client.loop_start()
        # small wait for connect
        t0 = time.time()
        while not self.connected and time.time() - t0 < 5:
            time.sleep(0.05)

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

    def request_dyn(self):
        hdr = self.next_header()
        self.client.publish(f"/SW_GPS/{self.device_id}/user/batPropertyReq", hdr, qos=1)

    def send_control(self, ctrl_type: int, ctrl_value: int):
        hdr = self.next_header()
        self.last_rsp_raw_hex = None
        self.client.publish(
            f"/SW_GPS/{self.device_id}/user/bmsCtrReq",
            hdr + bytes([ctrl_type & 0xFF, ctrl_value & 0xFF]),
            qos=1,
        )

    def wait_for_dyn_update(self, timeout: float) -> bool:
        t0 = time.time()
        last_seen_hex = self.last_dyn_raw_hex
        while time.time() - t0 < timeout:
            if self.last_dyn_raw_hex and self.last_dyn_raw_hex != last_seen_hex:
                return True
            time.sleep(0.05)
        return False

    def verify_bits_and_current(
        self,
        expect_no_discharge: Optional[bool] = None,
        expect_no_charge: Optional[bool] = None,
        expect_current_sign: Optional[str] = None,  # 'pos'|'neg'|'near0'|None
    ) -> Tuple[bool, Dict]:
        dec = self.last_dyn or {}
        bits = extract_bits(dec)
        cur = parse_current(dec)
        result = {
            'bits': bits,
            'current_A': cur,
            'raw_dyn_hex': self.last_dyn_raw_hex,
            'raw_rsp_hex': self.last_rsp_raw_hex,
        }

        ok = True
        if expect_no_discharge is not None:
            ok &= (bits.get('no_discharge', 0) == (1 if expect_no_discharge else 0))
        if expect_no_charge is not None:
            ok &= (bits.get('no_charge', 0) == (1 if expect_no_charge else 0))
        if expect_current_sign and cur is not None:
            if expect_current_sign == 'pos':
                ok &= cur > 0.05
            elif expect_current_sign == 'neg':
                ok &= cur < -0.05
            elif expect_current_sign == 'near0':
                ok &= abs(cur) <= 0.1

        return ok, result

    def run_test_sequence(self) -> Dict:
        summary: Dict = {'device_id': self.device_id, 'steps': []}
        self.connect()
        try:
            # Baseline read
            self.request_dyn()
            self.wait_for_dyn_update(5)
            ok, res = self.verify_bits_and_current()
            summary['baseline'] = res

            # 1) Prohibit discharge
            self.send_control(1, 1)
            time.sleep(self.wait_after_cmd)
            self.request_dyn()
            self.wait_for_dyn_update(self.overall_timeout)
            ok1, res1 = self.verify_bits_and_current(expect_no_discharge=True)
            summary['steps'].append({'action': 'Prohibit discharge (1,1)', 'ok': ok1, 'result': res1})

            # 2) Allow discharge
            self.send_control(1, 0)
            time.sleep(self.wait_after_cmd)
            self.request_dyn()
            self.wait_for_dyn_update(self.overall_timeout)
            ok2, res2 = self.verify_bits_and_current(expect_no_discharge=False)
            summary['steps'].append({'action': 'Allow discharge (1,0)', 'ok': ok2, 'result': res2})

            # 3) Prohibit charging
            self.send_control(2, 1)
            time.sleep(self.wait_after_cmd)
            self.request_dyn()
            self.wait_for_dyn_update(self.overall_timeout)
            ok3, res3 = self.verify_bits_and_current(expect_no_charge=True)
            summary['steps'].append({'action': 'Prohibit charging (2,1)', 'ok': ok3, 'result': res3})

            # 4) Allow charging
            self.send_control(2, 0)
            time.sleep(self.wait_after_cmd)
            self.request_dyn()
            self.wait_for_dyn_update(self.overall_timeout)
            ok4, res4 = self.verify_bits_and_current(expect_no_charge=False)
            summary['steps'].append({'action': 'Allow charging (2,0)', 'ok': ok4, 'result': res4})

            # 5) Enable static mode
            self.send_control(3, 1)
            time.sleep(self.wait_after_cmd)
            self.request_dyn()
            self.wait_for_dyn_update(self.overall_timeout)
            ok5, res5 = self.verify_bits_and_current()
            summary['steps'].append({'action': 'Allow static mode (3,1)', 'ok': ok5, 'result': res5})

            # 6) Disable static mode
            self.send_control(3, 0)
            time.sleep(self.wait_after_cmd)
            self.request_dyn()
            self.wait_for_dyn_update(self.overall_timeout)
            ok6, res6 = self.verify_bits_and_current()
            summary['steps'].append({'action': 'Static mode not allowed (3,0)', 'ok': ok6, 'result': res6})

        finally:
            self.disconnect()

        # Overall
        summary['all_ok'] = all(step['ok'] for step in summary['steps']) if summary.get('steps') else False
        return summary


def main():
    parser = argparse.ArgumentParser(description='Diagnose battery control commands by verifying status bits and current')
    parser.add_argument('--device', required=True, help='Device ID to test')
    parser.add_argument('--wait', type=float, default=2.0, help='Seconds to wait after sending each control (default 2.0)')
    parser.add_argument('--timeout', type=int, default=20, help='Max seconds to wait for a property update (default 20)')
    args = parser.parse_args()

    diag = ControlDiagnoser(args.device, wait_after_cmd=args.wait, overall_timeout=args.timeout)
    summary = diag.run_test_sequence()

    import json
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()












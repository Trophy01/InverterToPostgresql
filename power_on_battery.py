import argparse
import json
import logging
import random
import struct
import sys
import time
from typing import Optional, Tuple

import paho.mqtt.client as mqtt

from decoder import decode_battery_property_report, decode_payload_bytes


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


BROKER_ADDRESS = "mqtt-cloud-1.telco.co.zw"
BROKER_PORT = 1883


START_CODE = 0x4350
PROTOCOL_VERSION = 0x11


def build_header(sequence: int, transaction_id: int) -> bytes:
    return struct.pack("!H B H B", START_CODE, PROTOCOL_VERSION, sequence & 0xFFFF, transaction_id & 0xFF)


def print_payload_hex(payload: bytes, description: str = "Payload"):
    """Print payload as hex string for debugging"""
    hex_str = payload.hex().upper()
    # Format as pairs for readability
    formatted_hex = ' '.join([hex_str[i:i+2] for i in range(0, len(hex_str), 2)])
    print(f"📤 {description}: {formatted_hex} ({len(payload)} bytes)")


def extract_device_id(identifier: str) -> str:
    s = identifier.strip()
    if '/' in s:
        parts = [p for p in s.split('/') if p]
        if len(parts) >= 2 and parts[0] == 'SW_GPS':
            return parts[1]
        for p in parts:
            if p.isdigit() and 10 <= len(p) <= 20:
                return p
    return s


def parse_current_from_dyn(decoded_dyn: dict) -> Optional[float]:
    try:
        current_field = decoded_dyn.get("Current")  # e.g., "12.3 A"
        if isinstance(current_field, str) and current_field.endswith(" A"):
            return float(current_field[:-2].strip())
        # Some variants may store numeric already
        if isinstance(current_field, (int, float)):
            return float(current_field)
    except Exception:
        return None
    return None


def run_pinger_until_current_changes(device_id: str, repeat_seconds: int = 3, timeout_seconds: int = 600,
                                     initial_current: Optional[float] = None,
                                     controls: Tuple[Tuple[int, int], ...] = ((2, 0), (1, 0), (3, 0))) -> dict:
    """
    Ping ext/wake and cycle through bms controls until the battery current changes (as seen in batPropertyRsp).
    Pass timeout_seconds=0 to run indefinitely until change detected.
    Returns a summary dict with the last seen current and whether a change was detected.
    """
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    seq = int(time.time()) & 0xFFFF
    txn = random.randint(0, 255)

    last_current: Optional[float] = None
    baseline_current: Optional[float] = initial_current
    change_detected = False
    last_dyn_decoded: Optional[dict] = None

    def next_header() -> bytes:
        nonlocal seq, txn
        seq = (seq + 1) & 0xFFFF
        txn = (txn + 1) & 0xFF
        return build_header(seq, txn)

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code != 0:
            logging.error(f"MQTT connect failed: {reason_code}")
            return
        print("✅ Connected")
        # Listen to dynamic responses (property) and reports and control responses
        client.subscribe(f"/SW_GPS/{device_id}/user/batPropertyRsp", qos=1)
        client.subscribe(f"/SW_GPS/{device_id}/user/batPropertyRprt", qos=1)
        client.subscribe(f"/SW_GPS/{device_id}/user/bmsCtrRsp", qos=1)
        print(f"📡 Subscribed: /SW_GPS/{device_id}/user/batPropertyRsp")
        print(f"📡 Subscribed: /SW_GPS/{device_id}/user/batPropertyRprt")
        print(f"📡 Subscribed: /SW_GPS/{device_id}/user/bmsCtrRsp")
        # Immediately send 'Allow discharge' (type=1, value=0)
        try:
            hdr = next_header()
            ctrl_payload = hdr + bytes([1, 0])
            print_payload_hex(ctrl_payload, "Initial bmsCtrReq (Allow discharge)")
            client.publish(f"/SW_GPS/{device_id}/user/bmsCtrReq", ctrl_payload, qos=1)
            print("✅ Sent discharge allow (bmsCtrReq type=1 value=0)")
        except Exception as e:
            logging.warning(f"Failed to send initial discharge allow: {e}")

    def on_message(client, userdata, msg):
        nonlocal last_current, baseline_current, change_detected, last_dyn_decoded
        topic = msg.topic
        try:
            if topic.endswith("bmsCtrRsp"):
                print("ℹ️  Control response observed (bmsCtrRsp)")
                print_payload_hex(msg.payload, "Received bmsCtrRsp")
                return
            payload_bytes = decode_payload_bytes(msg.payload)
            decoded = decode_battery_property_report(payload_bytes, device_id)
            if decoded:
                last_dyn_decoded = decoded
                curr = parse_current_from_dyn(decoded)
                if curr is not None:
                    if baseline_current is None:
                        baseline_current = curr
                        print(f"⚙️  Baseline current set: {baseline_current} A")
                    last_current = curr
                    print(f"🔄 Current observed: {last_current} A")
                    # Detect change with small epsilon (0.05 A)
                    if baseline_current is not None and abs(curr - baseline_current) > 0.05:
                        change_detected = True
                        print(f"🎉 Current change detected: {baseline_current} A -> {last_current} A")
        except Exception as e:
            logging.debug(f"Decode error on {msg.topic}: {e}")

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER_ADDRESS, BROKER_PORT, 60)
    client.loop_start()

    try:
        start = time.time()
        ctrl_index = 0
        while True:
            # Stop conditions
            if change_detected:
                break
            if timeout_seconds > 0 and (time.time() - start > timeout_seconds):
                break

            # Send ext, wake, and one control
            try:
                hdr = next_header()
                
                # Send batPropertyExtReq
                print_payload_hex(hdr, "batPropertyExtReq")
                client.publish(f"/SW_GPS/{device_id}/user/batPropertyExtReq", hdr, qos=1)
                
                # Send batPropertyReq  
                print_payload_hex(hdr, "batPropertyReq")
                client.publish(f"/SW_GPS/{device_id}/user/batPropertyReq", hdr, qos=1)
                
                # Send control if available
                if controls and len(controls) > 0:
                    ctype, cval = controls[ctrl_index % len(controls)]
                    ctrl_payload = hdr + bytes([ctype & 0xFF, cval & 0xFF])
                    print_payload_hex(ctrl_payload, f"bmsCtrReq (type={ctype}, value={cval})")
                    client.publish(f"/SW_GPS/{device_id}/user/bmsCtrReq", ctrl_payload, qos=1)
                    print(f"📤 Sent bmsCtrReq type={ctype} value={cval}")
                    ctrl_index += 1
                print("📤 Pinged (ext, wake, optional ctrl)")
            except Exception as e:
                logging.warning(f"Publish error: {e}")

            # Sleep ~repeat_seconds with small checks in between
            end_wait = time.time() + repeat_seconds
            while time.time() < end_wait:
                if change_detected:
                    break
                time.sleep(0.1)
        
        return {
            'device_id': device_id,
            'baseline_current_A': baseline_current,
            'last_current_A': last_current,
            'change_detected': change_detected,
            'last_dynamic_decoded': last_dyn_decoded,
            'duration_s': round(time.time() - start, 1)
        }
    finally:
        client.loop_stop()
        client.disconnect()


def show_control_menu() -> Tuple[int, int]:
    """Show interactive menu of battery controls and return selected type, value"""
    controls = [
        (1, 0, "Allow discharge"),
        (1, 1, "Prohibit discharge"),
        (2, 0, "Allow charging"),
        (2, 1, "Prohibit charging"),
        (3, 0, "Standing mode not allowed"),
        (3, 1, "Allow static mode"),
    ]
    
    print("\n" + "="*50)
    print("BATTERY CONTROL MENU")
    print("="*50)
    print("Please select a control to send:")
    print()
    
    for i, (ctrl_type, ctrl_value, description) in enumerate(controls, 1):
        print(f"{i}. {description} (Type={ctrl_type}, Value={ctrl_value})")
    
    print()
    while True:
        try:
            choice = input("Enter your choice (1-6): ").strip()
            if choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(controls):
                    ctrl_type, ctrl_value, description = controls[idx]
                    print(f"Selected: {description}")
                    return ctrl_type, ctrl_value
            print("Invalid choice. Please enter 1-6.")
        except KeyboardInterrupt:
            print("\nExiting...")
            sys.exit(0)


def send_single_control(device_id: str, ctrl_type: int, ctrl_value: int) -> dict:
    """Send a single control command and wait for response"""
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    
    seq = int(time.time()) & 0xFFFF
    txn = random.randint(0, 255)
    response_received = False
    response_data = None
    
    def next_header() -> bytes:
        nonlocal seq, txn
        seq = (seq + 1) & 0xFFFF
        txn = (txn + 1) & 0xFF
        return build_header(seq, txn)
    
    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code != 0:
            logging.error(f"MQTT connect failed: {reason_code}")
            return
        print("✅ Connected to MQTT broker")
        client.subscribe(f"/SW_GPS/{device_id}/user/bmsCtrRsp", qos=1)
        print(f"📡 Subscribed to control responses")
        
        # Send the control command
        try:
            hdr = next_header()
            ctrl_payload = hdr + bytes([ctrl_type & 0xFF, ctrl_value & 0xFF])
            print_payload_hex(ctrl_payload, f"Single bmsCtrReq (type={ctrl_type}, value={ctrl_value})")
            client.publish(f"/SW_GPS/{device_id}/user/bmsCtrReq", ctrl_payload, qos=1)
            print(f"📤 Sent control command: Type={ctrl_type}, Value={ctrl_value}")
        except Exception as e:
            logging.error(f"Failed to send control command: {e}")
    
    def on_message(client, userdata, msg):
        nonlocal response_received, response_data
        if msg.topic.endswith("bmsCtrRsp"):
            try:
                print_payload_hex(msg.payload, "Received bmsCtrRsp")
                payload_bytes = decode_payload_bytes(msg.payload)
                if len(payload_bytes) >= 8:  # Header + 2 bytes control response
                    resp_type = payload_bytes[6]
                    resp_value = payload_bytes[7]
                    response_data = {
                        'control_type': resp_type,
                        'control_value': resp_value,
                        'raw_payload_hex': payload_bytes.hex()
                    }
                    response_received = True
                    print(f"✅ Control response received: Type={resp_type}, Value={resp_value}")
            except Exception as e:
                logging.error(f"Error parsing control response: {e}")
    
    client.on_connect = on_connect
    client.on_message = on_message
    
    client.connect(BROKER_ADDRESS, BROKER_PORT, 60)
    client.loop_start()
    
    try:
        # Wait for response (max 10 seconds)
        start_time = time.time()
        while not response_received and (time.time() - start_time < 10):
            time.sleep(0.1)
        
        if not response_received:
            print("⚠️  No control response received within 10 seconds")
            response_data = {'error': 'No response received'}
            
    finally:
        client.loop_stop()
        client.disconnect()
    
    return response_data or {}


def main():
    parser = argparse.ArgumentParser(description='Battery Control Tool')
    parser.add_argument('identifier', help='Device ID or /SW_GPS/<device>')
    parser.add_argument('--menu', action='store_true', help='Show interactive control menu')
    parser.add_argument('--control', type=str, help='Send specific control: TYPE:VALUE (e.g., 1:0)')
    parser.add_argument('--repeat', type=int, default=3, help='Seconds between pings (default 3)')
    parser.add_argument('--timeout', type=int, default=600, help='Overall timeout seconds (0 = no timeout)')
    parser.add_argument('--baseline', type=float, default=None, help='Baseline current A to compare (optional)')
    parser.add_argument('--controls', type=str, default="2:0,1:0,3:0", help='Comma list TYPE:VALUE e.g. "2:0,1:0,3:0"')
    args = parser.parse_args()

    device_id = extract_device_id(args.identifier)
    
    if args.menu:
        # Interactive menu mode
        while True:
            try:
                ctrl_type, ctrl_value = show_control_menu()
                result = send_single_control(device_id, ctrl_type, ctrl_value)
                print(f"\nResult: {json.dumps(result, ensure_ascii=False, indent=2)}")
                
                print("\n" + "-"*50)
                continue_choice = input("Send another control? (y/n): ").strip().lower()
                if continue_choice not in ['y', 'yes']:
                    break
            except KeyboardInterrupt:
                print("\nExiting...")
                break
        return
    
    if args.control:
        # Single control mode
        try:
            ctrl_type, ctrl_value = map(int, args.control.split(':'))
            result = send_single_control(device_id, ctrl_type, ctrl_value)
            print(json.dumps(result, ensure_ascii=False, indent=2))
        except ValueError:
            print("Error: --control must be in format TYPE:VALUE (e.g., 1:0)")
        return
    
    # Original ping mode
    ctrl_list: Tuple[Tuple[int, int], ...] = tuple(
        (int(p.split(':', 1)[0]), int(p.split(':', 1)[1]))
        for p in [x.strip() for x in args.controls.split(',') if x.strip()]
        if ':' in p
    )

    result = run_pinger_until_current_changes(
        device_id=device_id,
        repeat_seconds=max(1, int(args.repeat)),
        timeout_seconds=int(args.timeout),
        initial_current=args.baseline,
        controls=ctrl_list if ctrl_list else ((2, 0), (1, 0), (3, 0)),
    )

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
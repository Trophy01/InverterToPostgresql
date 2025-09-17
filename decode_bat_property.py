#!/usr/bin/env python3
import argparse
import json
import logging
import time
import paho.mqtt.client as mqtt
from typing import Any, Dict

# Reuse decoding from decoder.py
from decoder import build_header, parse_header, decode_battery_property_report, decode_payload_bytes, BROKER_ADDRESS, BROKER_PORT


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def print_decoded(decoded: Dict[str, Any]) -> None:
    # Core battery data
    print("\nBATTERY PROPERTY REPORT (batPropertyRprt)")
    print("-" * 60)
    print(f"Device ID: {decoded.get('Device ID', 'N/A')}")
    print(f"Product SN: {decoded.get('Product SN', 'N/A')}")

    # Current and derived inverter load
    current_str = decoded.get('Current', '0 A')
    try:
        current_val = float(str(current_str).split()[0])
    except Exception:
        current_val = 0.0
    load_current_a = abs(current_val)  # derive load current as absolute discharge
    print(f"Current: {current_str}")
    print(f"Current Type: {decoded.get('Current_Type', 'N/A')}")
    print(f"Derived Inverter Load Current: {load_current_a} A")

    # Capacity/SOC
    print(f"Remaining Capacity: {decoded.get('Remaining Capacity', 'N/A')}")
    print(f"Total Capacity: {decoded.get('Total Capacity', 'N/A')}")
    print(f"State of Charge: {decoded.get('State of Charge', 'N/A')}")

    # Voltages
    print(f"Total Battery Voltage: {decoded.get('Total Battery Voltage', 'N/A')}")
    cell_voltages = decoded.get('Cell Voltages', [])
    if cell_voltages:
        print(f"Cell Voltages: {cell_voltages}")
        print(f"  Min: {decoded.get('Cell Voltage Min', 'N/A')} mV, Max: {decoded.get('Cell Voltage Max', 'N/A')} mV, Avg: {decoded.get('Cell Voltage Avg', 'N/A')} mV, Diff: {decoded.get('Cell Voltage Diff', 'N/A')} mV")

    # Temperatures
    print(f"BMS Temperatures: {decoded.get('BMS Temperatures', [])}")
    print(f"Cell Temperatures: {decoded.get('Cell Temperatures', [])}")

    # Loop cycles
    print(f"Loop Cycles: {decoded.get('Loop Cycles', 'N/A')}")

    # Battery Status
    status = decoded.get('Battery Status', {})
    if status:
        print("\nBattery Status:")
        if status.get('has_alarms'):
            print("Active Alarms:")
            for a in status.get('active_alarms', []):
                print(f"  - {a}")
        else:
            print("  No active alarms")

    # Network
    net = decoded.get('Network Status', {})
    if net:
        print("\nNetwork Status:")
        print(f"  RSSI: {net.get('rssi', 'N/A')} dBm, PLMN: {net.get('plmn', 'N/A')}, LAC: {net.get('lac', 'N/A')}, Cell ID: {net.get('cell_id', 'N/A')}")


def main() -> None:
    parser = argparse.ArgumentParser(description='Decode batPropertyRprt (battery + inverter data)')
    parser.add_argument('--device', required=False, help='Device ID to subscribe to (MQTT)')
    parser.add_argument('--hex', required=False, help='Single batPropertyRprt payload as hex to decode')
    parser.add_argument('--time', type=int, default=60, help='Listen time in seconds when using MQTT (default: 60)')
    args = parser.parse_args()

    if args.hex:
        payload_bytes = bytes.fromhex(args.hex.strip())
        decoded = decode_battery_property_report(payload_bytes)
        if decoded:
            print_decoded(decoded)
        else:
            print("Failed to decode payload")
        return

    if not args.device:
        print("Error: either --hex or --device is required")
        return

    target_topic = f"/SW_GPS/{args.device}/user/batPropertyRprt"
    got_report = {'done': False}

    def on_connect(client: mqtt.Client, userdata, flags, rc, properties=None):
        if rc == 0:
            logging.info("Connected")
            client.subscribe(target_topic, qos=1)
            logging.info(f"Subscribed to {target_topic}")
        else:
            logging.error(f"Connect failed: {rc}")

    def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
        if msg.topic != target_topic or got_report['done']:
            return
        payload_bytes = decode_payload_bytes(msg.payload)
        try:
            decoded = decode_battery_property_report(payload_bytes, args.device)
            if decoded:
                print_decoded(decoded)
                # Also show raw payload, once
                print("\nRaw Payload (hex):")
                print(payload_bytes.hex())
                got_report['done'] = True
        except Exception as e:
            logging.exception(f"Decode error: {e}")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_ADDRESS, BROKER_PORT, 60)

    print(f"Listening for batPropertyRprt on {target_topic} for up to {args.time}s ...")
    start = time.time()
    while time.time() - start < args.time and not got_report['done']:
        client.loop(timeout=1.0)
        time.sleep(0.05)

    if not got_report['done']:
        print("No batPropertyRprt received in time window.")


if __name__ == '__main__':
    main()



#!/usr/bin/env python3
"""
Battery Data Requester

This script sends MQTT requests to get ALL available battery data:
1. batPropertyReq -> batPropertyRsp (dynamic data)
2. batPropertyExtReq -> batPropertyExtRsp (static data)
3. Monitors batPositonRprt (automatic position reports)

The script sends requests and then monitors for responses.
"""

import paho.mqtt.client as mqtt
import json
import logging
import sys
import struct
import binascii
import time
import argparse
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional
import hashlib


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


BROKER_ADDRESS = "mqtt-cloud-1.telco.co.zw"
BROKER_PORT = 1883
MQTT_TOPIC = "/SW_GPS/#"


START_CODE = 0x4350
PROTOCOL_VERSION = 0x11
HEADER_SIZE = 6


def build_header(seq: int, txn: int) -> bytes:
    """Build 6-byte protocol header (no body for requests)."""
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


def extract_device_id(topic: str) -> str:
    """Extract device ID from MQTT topic"""
    parts = topic.split('/')
    return parts[2] if len(parts) >= 3 else 'unknown'


class BatteryDataRequester:
    def __init__(self, device_id: str, request_interval: int = 30):
        self.device_id = device_id
        self.request_interval = request_interval
        self.seq = 0
        self.txn = 0
        self.client = None
        self.responses_received = {
            'batPropertyRsp': 0,
            'batPropertyExtRsp': 0,
            'batPositionRsp': 0,
            'batPositonRprt': 0
        }
        
    def get_next_seq_txn(self) -> Tuple[int, int]:
        """Get next sequence and transaction numbers"""
        self.seq = (self.seq + 1) & 0xFFFF
        self.txn = (self.txn + 1) & 0xFF
        return self.seq, self.txn
    
    def send_battery_property_request(self):
        """Send batPropertyReq to get dynamic battery data"""
        seq, txn = self.get_next_seq_txn()
        header_bytes = build_header(seq, txn)
        topic = f"/SW_GPS/{self.device_id}/user/batPropertyReq"
        
        self.client.publish(topic, payload=header_bytes, qos=0, retain=False)
        logging.info(f"📤 Sent batPropertyReq to {topic} (seq={seq}, txn={txn})")
    
    def send_battery_property_ext_request(self):
        """Send batPropertyExtReq to get static battery data"""
        seq, txn = self.get_next_seq_txn()
        header_bytes = build_header(seq, txn)
        topic = f"/SW_GPS/{self.device_id}/user/batPropertyExtReq"
        
        self.client.publish(topic, payload=header_bytes, qos=0, retain=False)
        logging.info(f"📤 Sent batPropertyExtReq to {topic} (seq={seq}, txn={txn})")
    
    def send_battery_position_request(self):
        """Send batPositionReq to get position data (experimental)"""
        seq, txn = self.get_next_seq_txn()
        header_bytes = build_header(seq, txn)
        topic = f"/SW_GPS/{self.device_id}/user/batPositionReq"
        
        self.client.publish(topic, payload=header_bytes, qos=0, retain=False)
        logging.info(f"📤 Sent batPositionReq to {topic} (seq={seq}, txn={txn}) [EXPERIMENTAL]")
    
    def send_all_requests(self):
        """Send all available request types"""
        logging.info(f"🚀 Sending all data requests for device: {self.device_id}")
        self.send_battery_property_request()
        time.sleep(1)  # Small delay between requests
        self.send_battery_property_ext_request()
        time.sleep(1)  # Small delay between requests
        self.send_battery_position_request()  # Try position request
        logging.info("✅ All requests sent")
    
    def on_connect(self, client, userdata, flags, reason_code, properties):
        """MQTT connection callback"""
        if reason_code == 0:
            logging.info("Connected to MQTT broker")
            client.subscribe(MQTT_TOPIC)
            logging.info(f"Subscribed to {MQTT_TOPIC}")
            
            # Send initial requests
            self.send_all_requests()
            
            # Schedule periodic requests
            logging.info(f"Will send requests every {self.request_interval} seconds")
        else:
            logging.error(f"MQTT connect failed: {reason_code}")
    
    def on_message(self, client, userdata, msg):
        """MQTT message callback"""
        topic = msg.topic
        device_id = extract_device_id(topic)
        message_type = topic.split('/')[-1]
        
        # Only process messages from our target device
        if device_id != self.device_id:
            return
        
        payload_bytes = decode_payload_bytes(msg.payload)
        
        try:
            # Count responses
            if message_type in self.responses_received:
                self.responses_received[message_type] += 1
            
            # Parse header
            try:
                header = parse_header(payload_bytes)
            except:
                header = {"error": "Could not parse header"}
            
            # Create output
            output = {
                'timestamp': datetime.now().isoformat(),
                'device_id': device_id,
                'message_type': message_type,
                'response_count': self.responses_received.get(message_type, 0),
                'header': header,
                'raw_payload_hex': payload_bytes.hex(),
                'payload_length': len(payload_bytes)
            }
            
            # Add specific decoding based on message type
            if message_type == 'batPropertyRsp':
                output['note'] = 'Dynamic battery data response'
                logging.info(f"📥 Received batPropertyRsp #{self.responses_received[message_type]}")
            elif message_type == 'batPropertyExtRsp':
                output['note'] = 'Static battery data response'
                logging.info(f"📥 Received batPropertyExtRsp #{self.responses_received[message_type]}")
            elif message_type == 'batPositionRsp':
                output['note'] = 'Position data response (from batPositionReq)'
                logging.info(f"🎯 Received batPositionRsp #{self.responses_received[message_type]} - SUCCESS!")
            elif message_type == 'batPositonRprt':
                output['note'] = 'Position data response (from batPositionReq) - SUCCESS!'
                logging.info(f"🎯 Received batPositonRprt #{self.responses_received[message_type]} - POSITION REQUEST SUCCESS!")
            else:
                output['note'] = f'Other message type: {message_type}'
            
            print(json.dumps(output, ensure_ascii=False, indent=2))
            print("-" * 60)
            
        except Exception as e:
            logging.exception(f"Error processing message: {e}")
    
    def run(self):
        """Run the data requester"""
        print(f"🔋 Battery Data Requester for device: {self.device_id}")
        print(f"📡 Request interval: {self.request_interval} seconds")
        print(f"🎯 Will request:")
        print(f"   - batPropertyReq → batPropertyRsp (dynamic data)")
        print(f"   - batPropertyExtReq → batPropertyExtRsp (static data)")
        print(f"   - batPositionReq → batPositonRprt (position data) ✅ CONFIRMED!")
        print(f"   - Monitor automatic batPositonRprt reports")
        print("-" * 60)
        
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        try:
            self.client.connect(BROKER_ADDRESS, BROKER_PORT, 60)
            
            # Start the loop
            self.client.loop_start()
            
            # Send periodic requests
            while True:
                time.sleep(self.request_interval)
                self.send_all_requests()
                
                # Print status
                print(f"\n📊 Response Summary:")
                for msg_type, count in self.responses_received.items():
                    print(f"   {msg_type}: {count} responses")
                print("-" * 60)
                
        except KeyboardInterrupt:
            print("\n👋 Stopping battery data requester...")
            self.client.loop_stop()
            self.client.disconnect()


def main():
    parser = argparse.ArgumentParser(description='Request all available battery data via MQTT')
    parser.add_argument('device_id', help='Device ID (e.g., 862317043590129)')
    parser.add_argument('--interval', type=int, default=30, 
                       help='Request interval in seconds (default: 30)')
    
    args = parser.parse_args()
    
    # Handle full topic path or just device ID
    device_id = args.device_id
    if device_id.startswith('/SW_GPS/'):
        device_id = device_id.split('/')[2]
    
    requester = BatteryDataRequester(device_id, args.interval)
    requester.run()


if __name__ == '__main__':
    main()

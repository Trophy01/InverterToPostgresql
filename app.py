#!/usr/bin/env python3
"""
Enhanced Battery Data Client with Proper Decoding

This script uses the same decoding functions as decoder.py to properly
decode battery MQTT messages and display comprehensive battery data.
"""

import paho.mqtt.client as mqtt
import time
import struct
import threading
import json
import logging
import binascii
from typing import Dict, Any, List, Optional

# Import decoding functions from decoder.py
from decoder import (
    parse_header, decode_battery_status, decode_battery_property_report,
    decode_battery_property_ext, decode_battery_position, decode_bms_control_response,
    decode_payload_bytes, build_header, extract_device_id
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Protocol constants
START_CODE = 0x4350
PROTOCOL_VERSION = 0x11
HEADER_SIZE = 6

class BatteryDataClient:
    def __init__(self, broker_host, broker_port=1883, device_id="862317043590129"):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.device_id = device_id
        self.client = mqtt.Client()
        self.response_received = threading.Event()
        self.response_data = None
        self.sequence_number = 1
        self.transaction_id = 1
        self.latest_data = {}  # Store latest decoded data
        
        # Set up MQTT client callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        
    def create_header(self):
        """Create the 6-byte message header according to protocol"""
        return build_header(self.sequence_number, self.transaction_id)
    
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected to MQTT broker at {self.broker_host}")
            # Subscribe to all relevant topics
            topics = [
                f"/SW_GPS/{self.device_id}/user/batPropertyRsp",
                f"/SW_GPS/{self.device_id}/user/batPropertyExtRsp",
                f"/SW_GPS/{self.device_id}/user/batPropertyRprt",
                f"/SW_GPS/{self.device_id}/user/batPropertyExtRprt",
                f"/SW_GPS/{self.device_id}/user/batPositonRprt",
                f"/SW_GPS/{self.device_id}/user/bmsCtrRsp"
            ]
            
            for topic in topics:
                client.subscribe(topic)
                print(f"Subscribed to: {topic}")
        else:
            print(f"Failed to connect to MQTT broker. Return code: {rc}")
    
    def on_disconnect(self, client, userdata, rc):
        print(f"Disconnected from MQTT broker. Return code: {rc}")
    
    def on_message(self, client, userdata, msg):
        """Enhanced message handler with proper decoding"""
        topic = msg.topic
        device_id = extract_device_id(topic)
        message_type = topic.split('/')[-1]
        payload_bytes = decode_payload_bytes(msg.payload)
        
        print(f"\n=== Message Received ===")
        print(f"Topic: {topic}")
        print(f"Message Type: {message_type}")
        print(f"Payload length: {len(payload_bytes)} bytes")
        print(f"Raw payload (hex): {payload_bytes.hex()}")
        
        # Store response and signal that we received it
        self.response_data = {
            'topic': topic,
            'payload': payload_bytes,
            'timestamp': time.time(),
            'message_type': message_type
        }
        self.response_received.set()
        
        # Decode the message using proper functions
        self.decode_and_display_message(payload_bytes, message_type, device_id)
    
    def decode_and_display_message(self, payload_bytes, message_type, device_id):
        """Decode message using the same functions as decoder.py"""
        try:
            # Route to appropriate decoder based on message type
            if message_type == 'batPropertyRprt':
                decoded = decode_battery_property_report(payload_bytes, device_id)
                self.display_dynamic_data(decoded, "Dynamic Report")
            elif message_type == 'batPropertyRsp':
                decoded = decode_battery_property_report(payload_bytes, device_id)
                self.display_dynamic_data(decoded, "Dynamic Response")
            elif message_type == 'batPropertyExtRprt' or message_type == 'batPropertyExtRsp':
                decoded = decode_battery_property_ext(payload_bytes, device_id)
                self.display_static_data(decoded, "Static Data")
            elif message_type == 'batPositonRprt':
                decoded = decode_battery_position(payload_bytes, device_id)
                self.display_position_data(decoded, "Position Data")
            elif message_type == 'bmsCtrRsp':
                decoded = decode_bms_control_response(payload_bytes, device_id)
                self.display_control_response(decoded, "Control Response")
            else:
                # Unknown message type - try to decode header at least
                try:
                    header = parse_header(payload_bytes)
                    decoded = {
                        'header': header,
                        'message_type': message_type,
                        'note': 'Unknown message type - only header decoded'
                    }
                    self.display_unknown_message(decoded)
                except Exception:
                    decoded = {
                        'message_type': message_type,
                        'error': 'Could not decode message header',
                        'raw_payload_hex': payload_bytes.hex()
                    }
                    self.display_error_message(decoded)
            
            # Store latest data
            self.latest_data[message_type] = decoded
            
        except Exception as e:
            logging.exception(f"Decode error for {device_id} {message_type}: {e}")
            err = {
                'device_id': device_id,
                'message_type': message_type,
                'error': str(e),
                'raw_payload_hex': payload_bytes.hex()
            }
            self.display_error_message(err)
    
    def display_dynamic_data(self, decoded, title):
        """Display decoded dynamic battery data"""
        if not decoded:
            print(f"❌ {title}: Failed to decode")
            return
            
        print(f"\n🔋 {title}")
        print("=" * 50)
        
        # Device info
        if 'Device ID' in decoded:
            print(f"Device ID: {decoded['Device ID']}")
        if 'Product SN' in decoded:
            print(f"Product SN: {decoded['Product SN']}")
        
        # Battery status
        if 'Battery Status' in decoded:
            status = decoded['Battery Status']
            print(f"\n📊 Battery Status:")
            if 'descriptions' in status:
                for key, desc in status['descriptions'].items():
                    print(f"  • {key}: {desc}")
            if status.get('has_alarms'):
                print(f"  ⚠️  Active Alarms:")
                for alarm in status.get('active_alarms', []):
                    print(f"    - {alarm}")
        
        # Current and voltage data
        if 'Current' in decoded:
            print(f"\n⚡ Current: {decoded['Current']}")
        if 'Current_Type' in decoded:
            print(f"Current Type: {decoded['Current_Type']}")
        
        if 'Cell Voltages' in decoded:
            voltages = decoded['Cell Voltages']
            if voltages:
                print(f"\n🔋 Cell Voltages:")
                print(f"  Count: {len(voltages)} cells")
                print(f"  Min: {decoded.get('Cell Voltage Min', 'N/A')} mV")
                print(f"  Max: {decoded.get('Cell Voltage Max', 'N/A')} mV")
                print(f"  Avg: {decoded.get('Cell Voltage Avg', 'N/A')} mV")
                print(f"  Diff: {decoded.get('Cell Voltage Diff', 'N/A')} mV")
                print(f"  Total: {decoded.get('Total Battery Voltage', 'N/A')}")
        
        # Temperature data
        if 'BMS Temperatures' in decoded:
            temps = decoded['BMS Temperatures']
            if temps:
                print(f"\n🌡️  BMS Temperatures: {temps}°C")
        if 'Cell Temperatures' in decoded:
            temps = decoded['Cell Temperatures']
            if temps:
                print(f"\n🌡️  Cell Temperatures: {temps}°C")
        
        # Capacity and cycles
        if 'Loop Cycles' in decoded:
            print(f"\n🔄 Loop Cycles: {decoded['Loop Cycles']}")
        if 'Remaining Capacity' in decoded:
            print(f"Remaining Capacity: {decoded['Remaining Capacity']}")
        if 'Total Capacity' in decoded:
            print(f"Total Capacity: {decoded['Total Capacity']}")
        if 'State of Charge' in decoded:
            print(f"State of Charge: {decoded['State of Charge']}")
        
        # Network status
        if 'Network Status' in decoded:
            net = decoded['Network Status']
            if net:
                print(f"\n📡 Network Status:")
                if 'rssi' in net and net['rssi'] is not None:
                    print(f"  Signal Strength: {net['rssi']} dBm")
                if 'plmn' in net:
                    print(f"  PLMN: {net['plmn']}")
                if 'rat' in net:
                    rat_str = "4G" if net['rat'] == 8 else "2G" if net['rat'] == 1 else f"Unknown({net['rat']})"
                    print(f"  Network Type: {rat_str}")
    
    def display_static_data(self, decoded, title):
        """Display decoded static battery data"""
        if not decoded:
            print(f"❌ {title}: Failed to decode")
            return
            
        print(f"\n📋 {title}")
        print("=" * 50)
        
        # Device info
        if 'Device ID' in decoded:
            print(f"Device ID: {decoded['Device ID']}")
        if 'Product SN' in decoded:
            print(f"Product SN: {decoded['Product SN']}")
        
        # GPS info
        if 'GPS SN' in decoded:
            print(f"\n📍 GPS Information:")
            print(f"  GPS SN: {decoded['GPS SN']}")
            print(f"  GPS IMSI: {decoded['GPS IMSI']}")
            print(f"  GPS IMEI: {decoded['GPS IMEI']}")
            print(f"  GPS Software: {decoded['GPS Software Version']}")
            print(f"  GPS Hardware: {decoded['GPS Hardware Version']}")
        
        # BMS info
        if 'BMS SN' in decoded:
            print(f"\n🔧 BMS Information:")
            print(f"  BMS SN: {decoded['BMS SN']}")
            if decoded.get('BMS Software Version'):
                print(f"  BMS Software: {decoded['BMS Software Version']}")
            if decoded.get('BMS Hardware Version'):
                print(f"  BMS Hardware: {decoded['BMS Hardware Version']}")
    
    def display_position_data(self, decoded, title):
        """Display decoded position data"""
        if not decoded:
            print(f"❌ {title}: Failed to decode")
            return
            
        print(f"\n📍 {title}")
        print("=" * 50)
        
        if 'header' in decoded:
            header = decoded['header']
            print(f"Header: Seq={header.get('sequence_number')}, Txn={header.get('transaction_id')}")
        
        if 'position_count' in decoded:
            print(f"Position Count: {decoded['position_count']}")
        
        if 'positions' in decoded:
            positions = decoded['positions']
            for i, pos in enumerate(positions):
                print(f"\n📍 Position {i+1}:")
                print(f"  Latitude: {pos.get('latitude', 'N/A')}°")
                print(f"  Longitude: {pos.get('longitude', 'N/A')}°")
                print(f"  Date: {pos.get('date', 'N/A')}")
                print(f"  Time: {pos.get('time', 'N/A')}")
                print(f"  Speed: {pos.get('speed_kmh', 'N/A')} km/h")
                print(f"  Direction: {pos.get('direction_deg', 'N/A')}°")
                print(f"  GPS Satellites: {pos.get('gps_satellites', 'N/A')}")
                print(f"  BeiDou Satellites: {pos.get('beidou_satellites', 'N/A')}")
    
    def display_control_response(self, decoded, title):
        """Display decoded control response"""
        if not decoded:
            print(f"❌ {title}: Failed to decode")
            return
            
        print(f"\n🎛️  {title}")
        print("=" * 50)
        
        if 'header' in decoded:
            header = decoded['header']
            print(f"Header: Seq={header.get('sequence_number')}, Txn={header.get('transaction_id')}")
        
        if 'control_type_description' in decoded:
            print(f"Control Type: {decoded['control_type_description']}")
        if 'value_meaning' in decoded:
            print(f"Value: {decoded['value_meaning']}")
        if 'response_status' in decoded:
            print(f"Status: {decoded['response_status']}")
    
    def display_unknown_message(self, decoded):
        """Display unknown message type"""
        print(f"\n❓ Unknown Message Type")
        print("=" * 50)
        print(f"Message Type: {decoded.get('message_type', 'Unknown')}")
        print(f"Note: {decoded.get('note', 'No additional info')}")
        if 'header' in decoded:
            header = decoded['header']
            print(f"Header: Seq={header.get('sequence_number')}, Txn={header.get('transaction_id')}")
    
    def display_error_message(self, decoded):
        """Display error message"""
        print(f"\n❌ Decode Error")
        print("=" * 50)
        print(f"Message Type: {decoded.get('message_type', 'Unknown')}")
        print(f"Error: {decoded.get('error', 'Unknown error')}")
        if 'raw_payload_hex' in decoded:
            print(f"Raw Payload: {decoded['raw_payload_hex']}")
    
    def connect(self):
        """Connect to MQTT broker"""
        try:
            self.client.connect(self.broker_host, self.broker_port, 60)
            self.client.loop_start()
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from MQTT broker"""
        self.client.loop_stop()
        self.client.disconnect()
    
    def request_dynamic_data(self, timeout=10):
        """Request battery dynamic data"""
        print("\n=== Requesting Battery Dynamic Data ===")
        
        topic = f"/SW_GPS/{self.device_id}/user/batPropertyReq"
        header = self.create_header()
        
        print(f"Publishing to topic: {topic}")
        print(f"Header (hex): {header.hex()}")
        
        # Reset response event
        self.response_received.clear()
        self.response_data = None
        
        # Send request
        result = self.client.publish(topic, header)
        if result.rc == 0:
            print("Request sent successfully")
        else:
            print(f"Failed to send request. Return code: {result.rc}")
        return None
        
        # Wait for response
        print(f"Waiting for response (timeout: {timeout}s)...")
        if self.response_received.wait(timeout):
            print("Response received!")
            return self.response_data
        else:
            print("Timeout - no response received")
            return None
    
    def request_static_data(self, timeout=10):
        """Request battery static data"""
        print("\n=== Requesting Battery Static Data ===")
        
        topic = f"/SW_GPS/{self.device_id}/user/batPropertyExtReq"
        header = self.create_header()
        
        print(f"Publishing to topic: {topic}")
        print(f"Header (hex): {header.hex()}")
        
        # Reset response event
        self.response_received.clear()
        self.response_data = None
        
        # Send request
        result = self.client.publish(topic, header)
        if result.rc == 0:
            print("Request sent successfully")
        else:
            print(f"Failed to send request. Return code: {result.rc}")
            return None
        
        # Wait for response
        print(f"Waiting for response (timeout: {timeout}s)...")
        if self.response_received.wait(timeout):
            print("Response received!")
            return self.response_data
        else:
            print("Timeout - no response received")
            return None
    
    def get_latest_data_summary(self):
        """Get a summary of all latest data"""
        print(f"\n📊 Latest Data Summary")
        print("=" * 50)
        
        if not self.latest_data:
            print("No data received yet")
            return
        
        for msg_type, data in self.latest_data.items():
            if data:
                print(f"\n📋 {msg_type}:")
                if msg_type in ['batPropertyRprt', 'batPropertyRsp']:
                    if 'State of Charge' in data:
                        print(f"  SOC: {data['State of Charge']}")
                    if 'Current' in data:
                        print(f"  Current: {data['Current']}")
                    if 'Cell Voltages' in data and data['Cell Voltages']:
                        print(f"  Cell Count: {len(data['Cell Voltages'])}")
                elif msg_type in ['batPropertyExtRprt', 'batPropertyExtRsp']:
                    if 'GPS SN' in data:
                        print(f"  GPS SN: {data['GPS SN']}")
                    if 'BMS SN' in data:
                        print(f"  BMS SN: {data['BMS SN']}")
                elif msg_type == 'batPositonRprt':
                    if 'position_count' in data:
                        print(f"  Positions: {data['position_count']}")
                elif msg_type == 'bmsCtrRsp':
                    if 'control_type_description' in data:
                        print(f"  Control: {data['control_type_description']}")


def main():
    # Configuration
    BROKER_HOST = "41.191.236.27"
    DEVICE_ID = "862317043590129"
    
    # Create client
    client = BatteryDataClient(BROKER_HOST, device_id=DEVICE_ID)
    
    try:
        # Connect to broker
        print("Connecting to MQTT broker...")
        if not client.connect():
            print("Failed to connect to MQTT broker")
            return
        
        # Wait a moment for connection to establish
        time.sleep(2)
        
        # Request dynamic data
        dynamic_response = client.request_dynamic_data(timeout=15)
        
        # Wait a bit between requests
        time.sleep(2)
        
        # Request static data
        static_response = client.request_static_data(timeout=15)
        
        # Keep listening for additional messages
        print("\nListening for additional messages...")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                time.sleep(1)
                # Show summary every 30 seconds
                if int(time.time()) % 30 == 0:
                    client.get_latest_data_summary()
        except KeyboardInterrupt:
            print("\nStopping...")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        print("Disconnecting...")
        client.disconnect()


if __name__ == "__main__":
    main()
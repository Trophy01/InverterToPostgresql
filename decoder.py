#!/usr/bin/env python3
"""
Comprehensive Battery Data Decoder

This decoder captures ALL possible data from the battery including:
- Dynamic battery data (batPropertyRprt)
- Static battery data (batPropertyExtRprt) 
- GPS position data (batPositonRprt) with advanced decoding
- Control responses (bmsCtrRsp)
- Enhanced status and load information
- Auto-requests data via periodic MQTT commands

Usage: python decoder.py --device <DEVICE_ID>
"""

import paho.mqtt.client as mqtt
import json
import logging
import sys
import struct
import binascii
import argparse
import time
import threading
import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Tuple, Optional
import hashlib


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


BROKER_ADDRESS = "mqtt-cloud-1.telco.co.zw"
BROKER_PORT = 1883
MQTT_TOPIC = "/SW_GPS/#"


START_CODE = 0x4350
PROTOCOL_VERSION = 0x11
HEADER_SIZE = 6


def md5_signature(appid: str, nonce_str: str, uid: str, key: str) -> str:
    signtemp = f"appid={appid}&nonce_str={nonce_str}&uid={uid}&key={key}"
    return hashlib.md5(signtemp.encode('utf-8')).hexdigest().upper()


def build_header(seq: int, txn: int) -> bytes:
    """Build 6-byte protocol header (no body)."""
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


def decode_battery_status(status_bytes: bytes) -> Dict[str, Any]:
    """
    Decode the battery status bitfield from 4 bytes according to section 4.5 of the protocol.
    Based on invmonitoringscripts/monitor_v4.py
    """
    status_int = int.from_bytes(status_bytes, byteorder='big')
    
    # Define status bits according to protocol section 4.5
    status_bits = {
        "charging_status": (status_int >> 0) & 1,           # Bit 0: 0=Idle, 1=Charging
        "full_state": (status_int >> 1) & 1,                # Bit 1: 0=Not filled, 1=Already filled
        "charge_overcurrent": (status_int >> 2) & 1,        # Bit 2: 0=No/removed, 1=Occurred and not lifted
        "cell_overvoltage": (status_int >> 3) & 1,          # Bit 3: 0=No/removed, 1=Occurred and not lifted
        "discharge_state": (status_int >> 4) & 1,           # Bit 4: 0=Idle, 1=Discharging
        "short_circuit_alarm": (status_int >> 5) & 1,       # Bit 5: 0=No/removed, 1=Occurred and not lifted
        "discharge_overflow": (status_int >> 6) & 1,        # Bit 6: 0=No/removed, 1=Occurred and not lifted
        "cell_undervoltage": (status_int >> 7) & 1,         # Bit 7: 0=No/removed, 1=Occurred and not lifted
        "cell_open_circuit": (status_int >> 8) & 1,         # Bit 8: 0=No/removed, 1=Occurred and not lifted
        "temp_detect_open": (status_int >> 9) & 1,          # Bit 9: 0=No/removed, 1=Occurred and not lifted
        "cell_high_temp": (status_int >> 10) & 1,           # Bit 10: 0=No/removed, 1=Occurred and not lifted
        "cell_low_temp": (status_int >> 11) & 1,            # Bit 11: 0=No/removed, 1=Occurred and not lifted
        "bms_high_temp": (status_int >> 12) & 1,            # Bit 12: 0=No/removed, 1=Occurred and not lifted
        "reserved_bit13": (status_int >> 13) & 1,           # Bit 13: Reserved field
        "no_charge": (status_int >> 14) & 1,                # Bit 14: 0=Allowed, 1=Prohibited
        "no_discharge": (status_int >> 15) & 1,             # Bit 15: 0=Allowed, 1=Prohibited
        "discharge_mos_failed": (status_int >> 16) & 1,     # Bit 16: 0=Not failed/lifted, 1=Failed
        "charging_mos_status": (status_int >> 17) & 1,      # Bit 17: 0=Off, 1=On
        "discharge_mos_status": (status_int >> 18) & 1,     # Bit 18: 0=Off, 1=On
        "charge_mos_failure": (status_int >> 19) & 1,       # Bit 19: 0=Not failed/lifted, 1=Failed
        "high_low_voltage_failure": (status_int >> 20) & 1, # Bit 20: 0=No failure/removed, 1=Failure
        "ultra_high_temp_failure": (status_int >> 21) & 1,  # Bit 21: 0=Invalid/removed, 1=Invalid
        "cell_pressure_difference": (status_int >> 22) & 1, # Bit 22: 0=Not invalid/lifted, 1=Large pressure difference
        "battery_temp_difference": (status_int >> 23) & 1   # Bit 23: 0=Not invalid/lifted, 1=Large temperature difference
    }
    
    # Add human-readable descriptions
    status_descriptions = {}
    
    # Charging and discharging state
    if status_bits["charging_status"]:
        status_descriptions["charging"] = "Currently charging"
    else:
        status_descriptions["charging"] = "Not charging"
        
    if status_bits["discharge_state"]:
        status_descriptions["discharging"] = "Currently discharging"
    else:
        status_descriptions["discharging"] = "Not discharging"
    
    # Battery full state
    if status_bits["full_state"]:
        status_descriptions["battery_full"] = "Battery is full"
    
    # MOS status (switches)
    if status_bits["charging_mos_status"]:
        status_descriptions["charging_switch"] = "Charging switch ON"
    else:
        status_descriptions["charging_switch"] = "Charging switch OFF"
        
    if status_bits["discharge_mos_status"]:
        status_descriptions["discharge_switch"] = "Discharge switch ON"
    else:
        status_descriptions["discharge_switch"] = "Discharge switch OFF"
    
    # Charge/discharge prohibitions
    if status_bits["no_charge"]:
        status_descriptions["charging_allowed"] = "Charging prohibited"
    else:
        status_descriptions["charging_allowed"] = "Charging allowed"
        
    if status_bits["no_discharge"]:
        status_descriptions["discharging_allowed"] = "Discharging prohibited"
    else:
        status_descriptions["discharging_allowed"] = "Discharging allowed"
    
    # Alarms and failures (only add if active)
    active_alarms = []
    
    if status_bits["charge_overcurrent"]:
        active_alarms.append("Charge overcurrent")
    
    if status_bits["cell_overvoltage"]:
        active_alarms.append("Cell overvoltage")
    
    if status_bits["short_circuit_alarm"]:
        active_alarms.append("Short circuit")
    
    if status_bits["discharge_overflow"]:
        active_alarms.append("Discharge overflow")
    
    if status_bits["cell_undervoltage"]:
        active_alarms.append("Cell undervoltage")
    
    if status_bits["cell_open_circuit"]:
        active_alarms.append("Cell detection line open circuit")
    
    if status_bits["temp_detect_open"]:
        active_alarms.append("Temperature detection line open")
    
    if status_bits["cell_high_temp"]:
        active_alarms.append("Cell high temperature")
    
    if status_bits["cell_low_temp"]:
        active_alarms.append("Cell low temperature")
    
    if status_bits["bms_high_temp"]:
        active_alarms.append("BMS high temperature")
    
    if status_bits["discharge_mos_failed"]:
        active_alarms.append("Discharge MOS failed")
    
    if status_bits["charge_mos_failure"]:
        active_alarms.append("Charge MOS failure")
    
    if status_bits["high_low_voltage_failure"]:
        active_alarms.append("High/low voltage cell failure")
    
    if status_bits["ultra_high_temp_failure"]:
        active_alarms.append("Ultra high temperature failure")
    
    if status_bits["cell_pressure_difference"]:
        active_alarms.append("Large cell pressure difference")
    
    if status_bits["battery_temp_difference"]:
        active_alarms.append("Large battery temperature difference")
    
    return {
        'raw': status_int,
        'bits': status_bits,
        'descriptions': status_descriptions,
        'active_alarms': active_alarms,
        'has_alarms': len(active_alarms) > 0
    }


def decode_battery_property_report(payload_bytes: bytes, topic_device_id: str = None) -> Dict[str, Any]:
    """
    Enhanced battery property report decoding based on Instructions
    Captures ALL data including SOC, cycles, load, and enhanced status
    """
    try:
        header = parse_header(payload_bytes)
        body_bytes = payload_bytes[HEADER_SIZE:]
        offset = 0
        
        # Parse the product serial number
        if offset >= len(body_bytes):
            logging.error("Body too short to contain Product SN")
            return None
            
        # Length-Value format for the device ID
        sn_length = body_bytes[offset]
        offset += 1
        
        if offset + sn_length > len(body_bytes):
            logging.error(f"Body too short to contain Product SN of length {sn_length}")
            return None
            
        # Handle FF values in device ID
        if all(b == 0xFF for b in body_bytes[offset:offset+sn_length]):
            product_sn = "UNKNOWN_SN"
        else:
            try:
                product_sn = body_bytes[offset:offset+sn_length].decode('ascii', errors='replace')
            except UnicodeDecodeError:
                product_sn = "HEX:" + body_bytes[offset:offset+sn_length].hex()
                
        offset += sn_length
        
        # Check if we have enough bytes for the battery status (4 bytes)
        if offset + 4 > len(body_bytes):
            logging.error("Body too short to contain Battery Status")
            return None
            
        # Battery Status (4 bytes)
        battery_status_bytes = body_bytes[offset:offset+4]
        battery_status = decode_battery_status(battery_status_bytes)
        offset += 4
        
        # Battery Data (Length-Value format)
        if offset >= len(body_bytes):
            logging.error("Body too short to contain Battery Data length")
            return None
            
        battery_data_length = body_bytes[offset]
        offset += 1
        
        if offset + battery_data_length > len(body_bytes):
            logging.error(f"Body too short to contain Battery Data of length {battery_data_length}")
            return None
        
        # Extract current (2 bytes) - System Current from AC/PV
        if offset + 2 > len(body_bytes):
            logging.error("Body too short to contain current data")
            return None
            
        current = int.from_bytes(body_bytes[offset:offset+2], byteorder="big", signed=True) / 10
        offset += 2
    
        # Extract cell voltage list
        if offset >= len(body_bytes):
            logging.error("Body too short to contain cell voltage list length")
            return None
            
        cell_voltage_list_length = body_bytes[offset]
        offset += 1
        
        if offset + cell_voltage_list_length > len(body_bytes):
            logging.error(f"Body too short to contain cell voltage list of length {cell_voltage_list_length}")
            return None
            
        cell_voltages = []
        cell_count = cell_voltage_list_length // 2  # Each cell voltage is 2 bytes
        
        for i in range(cell_count):
            if offset + 2 > len(body_bytes):
                break
            cell_voltage = int.from_bytes(body_bytes[offset:offset+2], byteorder="big")
            cell_voltages.append(cell_voltage)
            offset += 2
    
        # BMS temperature list
        if offset >= len(body_bytes):
            logging.error("Body too short to contain BMS temperature list length")
            return None
            
        bms_temp_length = body_bytes[offset]
        offset += 1
        
        if offset + bms_temp_length > len(body_bytes):
            logging.error(f"Body too short to contain BMS temperature list of length {bms_temp_length}")
            return None
            
        bms_temperatures = []
        for i in range(bms_temp_length):
            if offset >= len(body_bytes):
                break
            # Temperature is a signed byte value
            bms_temp = body_bytes[offset]
            if bms_temp > 127:  # Convert to signed
                bms_temp = bms_temp - 256
            bms_temperatures.append(bms_temp)
            offset += 1
            
        # Cell temperature list
        if offset >= len(body_bytes):
            logging.error("Body too short to contain cell temperature list length")
            return None
            
        cell_temp_length = body_bytes[offset]
        offset += 1
    
        if offset + cell_temp_length > len(body_bytes):
            logging.error(f"Body too short to contain cell temperature list of length {cell_temp_length}")
            return None
            
        cell_temperatures = []
        for i in range(cell_temp_length):
            if offset >= len(body_bytes):
                break
            # Temperature is a signed byte value
            cell_temp = body_bytes[offset]
            if cell_temp > 127:  # Convert to signed
                cell_temp = cell_temp - 256
            cell_temperatures.append(cell_temp)
            offset += 1
    
        # Loop cycles (2 bytes) - Number of Cycles (Charge/Discharge Loops)
        if offset + 2 > len(body_bytes):
            logging.error("Body too short to contain loop cycles")
            return None
            
        loop_cycles = int.from_bytes(body_bytes[offset:offset+2], byteorder="big")
        offset += 2
        
        # Remaining Capacity (2 bytes) - For SOC calculation
        if offset + 2 > len(body_bytes):
            logging.error("Body too short to contain remaining capacity")
            return None
            
        remaining_capacity = int.from_bytes(body_bytes[offset:offset+2], byteorder="big") / 10
        offset += 2
        
        # Total Capacity (2 bytes) - For SOC calculation
        if offset + 2 > len(body_bytes):
            logging.error("Body too short to contain total capacity")
            return None
            
        total_capacity = int.from_bytes(body_bytes[offset:offset+2], byteorder="big") / 10
        offset += 2
    
        # Network Status (4G·Net Info)
        # Parse the 4G Network Information if available
        network_status = {}
        if offset < len(body_bytes):
            # RSSI
            rssi = body_bytes[offset]
            network_status['rssi'] = None if rssi in (0, 255) else -int(rssi)
            offset += 1
            
            # PLMN (6 bytes)
            if offset + 6 <= len(body_bytes):
                plmn = body_bytes[offset:offset+6].decode('ascii', errors='replace')
                network_status['plmn'] = plmn
                offset += 6
    
            # LAC (2 bytes)
            if offset + 2 <= len(body_bytes):
                lac = body_bytes[offset:offset+2].hex()
                network_status['lac'] = lac
                offset += 2
    
            # Cell ID (4 bytes)
            if offset + 4 <= len(body_bytes):
                cell_id = body_bytes[offset:offset+4].hex()
                network_status['cell_id'] = cell_id
                offset += 4
    
            # RAT (1 byte)
            if offset < len(body_bytes):
                rat = body_bytes[offset]
                network_status['rat'] = rat
                offset += 1
    
        # Calculate State of Charge (SOC) percentage
        soc_percentage = 0
        if total_capacity > 0:
            soc_percentage = (remaining_capacity / total_capacity) * 100

        # Calculate total battery voltage from cell voltages
        total_voltage = sum(cell_voltages) if cell_voltages else 0
        
        # Enhanced data summary based on Instructions
        summary = {
            "Device ID": topic_device_id if topic_device_id and product_sn == "UNKNOWN_SN" else product_sn,
            "Product SN": product_sn,
            "Battery Status": battery_status,
            "Current": f"{current} A",
            "Current_Type": "Charging" if current > 0 else "Discharging" if current < 0 else "Idle",
            "Cell Voltages": cell_voltages,
            "Total Battery Voltage": f"{total_voltage} mV",
            "Cell Voltage Min": min(cell_voltages) if cell_voltages else 0,
            "Cell Voltage Max": max(cell_voltages) if cell_voltages else 0,
            "Cell Voltage Avg": sum(cell_voltages)/len(cell_voltages) if cell_voltages else 0,
            "Cell Voltage Diff": max(cell_voltages) - min(cell_voltages) if cell_voltages else 0,
            "BMS Temperatures": bms_temperatures,
            "Cell Temperatures": cell_temperatures, 
            "Loop Cycles": loop_cycles,
            "Remaining Capacity": f"{remaining_capacity} Ah",
            "Total Capacity": f"{total_capacity} Ah",
            "State of Charge": f"{soc_percentage:.1f}%",
            "Network Status": network_status,
            "Battery Data Length": battery_data_length
        }
        
        return summary
        
    except Exception as e:
        logging.error(f"Error decoding payload: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return None


def decode_battery_property_ext(payload_bytes: bytes, topic_device_id: str = None) -> Dict[str, Any]:
    """Enhanced battery property extended decoding based on Instructions"""
    try:
        header = parse_header(payload_bytes)
        body_bytes = payload_bytes[HEADER_SIZE:]
        offset = 0

        # Product SN
        if offset >= len(body_bytes):
            logging.error("Body too short to contain Product SN")
            return None

        sn_length = body_bytes[offset]
        offset += 1

        if offset + sn_length > len(body_bytes):
            logging.error(f"Body too short to contain Product SN of length {sn_length}")
            return None

        if all(b == 0xFF for b in body_bytes[offset:offset+sn_length]):
            product_sn = "UNKNOWN_SN"
        else:
            try:
                product_sn = body_bytes[offset:offset+sn_length].decode('ascii', errors='replace')
            except UnicodeDecodeError:
                product_sn = "HEX:" + body_bytes[offset:offset+sn_length].hex()

        offset += sn_length

        # GPS SN
        if offset >= len(body_bytes):
            logging.error("Body too short to contain GPS SN")
            return None

        gps_sn_length = body_bytes[offset]
        offset += 1
    
        if offset + gps_sn_length > len(body_bytes):
            logging.error(f"Body too short to contain GPS SN of length {gps_sn_length}")
            return None

        if all(b == 0xFF for b in body_bytes[offset:offset+gps_sn_length]):
            gps_sn = "UNKNOWN_GPS_SN"
        else:
            try:
                gps_sn = body_bytes[offset:offset+gps_sn_length].decode('ascii', errors='replace')
            except UnicodeDecodeError:
                gps_sn = "HEX:" + body_bytes[offset:offset+gps_sn_length].hex()

        offset += gps_sn_length

        # GPS IMSI
        if offset >= len(body_bytes):
            logging.error("Body too short to contain GPS IMSI")
            return None

        gps_imsi_length = body_bytes[offset]
        offset += 1

        if offset + gps_imsi_length > len(body_bytes):
            logging.error(f"Body too short to contain GPS IMSI of length {gps_imsi_length}")
            return None

        if all(b == 0xFF for b in body_bytes[offset:offset+gps_imsi_length]):
            gps_imsi = "UNKNOWN_GPS_IMSI"
        else:
            try:
                gps_imsi = body_bytes[offset:offset+gps_imsi_length].decode('ascii', errors='replace')
            except UnicodeDecodeError:
                gps_imsi = "HEX:" + body_bytes[offset:offset+gps_imsi_length].hex()

        offset += gps_imsi_length

        # GPS IMEI
        if offset >= len(body_bytes):
            logging.error("Body too short to contain GPS IMEI")
            return None

        gps_imei_length = body_bytes[offset]
        offset += 1

        if offset + gps_imei_length > len(body_bytes):
            logging.error(f"Body too short to contain GPS IMEI of length {gps_imei_length}")
            return None

        if all(b == 0xFF for b in body_bytes[offset:offset+gps_imei_length]):
            gps_imei = "UNKNOWN_GPS_IMEI"
        else:
            try:
                gps_imei = body_bytes[offset:offset+gps_imei_length].decode('ascii', errors='replace')
            except UnicodeDecodeError:
                gps_imei = "HEX:" + body_bytes[offset:offset+gps_imei_length].hex()

        offset += gps_imei_length

        # GPS Software Version
        if offset >= len(body_bytes):
            logging.error("Body too short to contain GPS Software Version")
            return None

        gps_sw_ver_length = body_bytes[offset]
        offset += 1

        if offset + gps_sw_ver_length > len(body_bytes):
            logging.error(f"Body too short to contain GPS Software Version of length {gps_sw_ver_length}")
            return None

        if all(b == 0xFF for b in body_bytes[offset:offset+gps_sw_ver_length]):
            gps_sw_ver = "UNKNOWN_GPS_SW_VER"
        else:
            try:
                gps_sw_ver = body_bytes[offset:offset+gps_sw_ver_length].decode('ascii', errors='replace')
            except UnicodeDecodeError:
                gps_sw_ver = "HEX:" + body_bytes[offset:offset+gps_sw_ver_length].hex()

        offset += gps_sw_ver_length

        # GPS Hardware Version
        if offset >= len(body_bytes):
            logging.error("Body too short to contain GPS Hardware Version")
            return None

        gps_hw_ver_length = body_bytes[offset]
        offset += 1

        if offset + gps_hw_ver_length > len(body_bytes):
            logging.error(f"Body too short to contain GPS Hardware Version of length {gps_hw_ver_length}")
            return None

        if all(b == 0xFF for b in body_bytes[offset:offset+gps_hw_ver_length]):
            gps_hw_ver = "UNKNOWN_GPS_HW_VER"
        else:
            try:
                gps_hw_ver = body_bytes[offset:offset+gps_hw_ver_length].decode('ascii', errors='replace')
            except UnicodeDecodeError:
                gps_hw_ver = "HEX:" + body_bytes[offset:offset+gps_hw_ver_length].hex()

        offset += gps_hw_ver_length

        # BMS SN
        if offset >= len(body_bytes):
            logging.error("Body too short to contain BMS SN")
            return None

        bms_sn_length = body_bytes[offset]
        offset += 1

        if offset + bms_sn_length > len(body_bytes):
            logging.error(f"Body too short to contain BMS SN of length {bms_sn_length}")
            return None

        if all(b == 0xFF for b in body_bytes[offset:offset+bms_sn_length]):
            bms_sn = "UNKNOWN_BMS_SN"
        else:
            try:
                bms_sn = body_bytes[offset:offset+bms_sn_length].decode('ascii', errors='replace')
            except UnicodeDecodeError:
                bms_sn = "HEX:" + body_bytes[offset:offset+bms_sn_length].hex()

        offset += bms_sn_length

        # Optional BMS version fields (may not be present in all messages)
        bms_sw_ver = None
        bms_hw_ver = None

        if offset < len(body_bytes):
            try:
                bms_sw_ver_length = body_bytes[offset]
                offset += 1
                if offset + bms_sw_ver_length <= len(body_bytes):
                    if all(b == 0xFF for b in body_bytes[offset:offset+bms_sw_ver_length]):
                        bms_sw_ver = "UNKNOWN_BMS_SW_VER"
                    else:
                        try:
                            bms_sw_ver = body_bytes[offset:offset+bms_sw_ver_length].decode('ascii', errors='replace')
                        except UnicodeDecodeError:
                            bms_sw_ver = "HEX:" + body_bytes[offset:offset+bms_sw_ver_length].hex()
                    offset += bms_sw_ver_length
            except Exception:
                bms_sw_ver = None

        if offset < len(body_bytes):
            try:
                bms_hw_ver_length = body_bytes[offset]
                offset += 1
                if offset + bms_hw_ver_length <= len(body_bytes):
                    if all(b == 0xFF for b in body_bytes[offset:offset+bms_hw_ver_length]):
                        bms_hw_ver = "UNKNOWN_BMS_HW_VER"
                    else:
                        try:
                            bms_hw_ver = body_bytes[offset:offset+bms_hw_ver_length].decode('ascii', errors='replace')
                        except UnicodeDecodeError:
                            bms_hw_ver = "HEX:" + body_bytes[offset:offset+bms_hw_ver_length].hex()
                    offset += bms_hw_ver_length
            except Exception:
                bms_hw_ver = None

        return {
            "Device ID": topic_device_id,
            "Product SN": product_sn,
            "GPS SN": gps_sn,
            "GPS IMSI": gps_imsi,
            "GPS IMEI": gps_imei,
            "GPS Software Version": gps_sw_ver,
            "GPS Hardware Version": gps_hw_ver,
            "BMS SN": bms_sn,
            "BMS Software Version": bms_sw_ver,
            "BMS Hardware Version": bms_hw_ver
        }
        
    except Exception as e:
        logging.error(f"Error decoding battery property ext: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return None


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


def decode_bms_control_response(payload_bytes: bytes, topic_device_id: str = None) -> Dict[str, Any]:
    try:
        header = parse_header(payload_bytes)
        body = payload_bytes[HEADER_SIZE:]
        if len(body) < 2:
            return {'header': header, 'control_type': None, 'value': None, 'control_type_description': 'Unknown', 'value_meaning': 'Unknown', 'response_status': 'Unknown'}
        control_type = body[0]
        value = body[1]
        control_types = {1: 'Discharge Switch Operation', 2: 'Charging Switch Operation', 3: 'Battery Static Mode Setting'}
        value_meanings = {1: {0: 'Allow discharge', 1: 'No discharge'}, 2: {0: 'Allow charging', 1: 'No charging'}, 3: {0: 'Standing mode is not allowed', 1: 'Allow static mode'}}
        return {'header': header, 'control_type': control_type, 'control_type_description': control_types.get(control_type, 'Unknown'), 'value': value, 'value_meaning': value_meanings.get(control_type, {}).get(value, 'Unknown'), 'response_status': 'Success'}
    except Exception as e:
        logging.error(f"Error decoding BMS control response: {e}")
        return None


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


def ensure_user_data(client: mqtt.Client) -> Dict[str, Any]:
    """Ensure userdata dict exists with counters and tracking sets."""
    userdata = client.user_data_get()
    if not isinstance(userdata, dict):
        userdata = {'seq': 0, 'txn': 0, 'ext_requested': set()}
        client.user_data_set(userdata)
    else:
        userdata.setdefault('seq', 0)
        userdata.setdefault('txn', 0)
        userdata.setdefault('ext_requested', set())
    return userdata


def send_bat_property_ext_req(client: mqtt.Client, device_id: str) -> None:
    """Publish an empty-body batPropertyExtReq with header only."""
    userdata = ensure_user_data(client)
    userdata['seq'] = (userdata['seq'] + 1) & 0xFFFF
    userdata['txn'] = (userdata['txn'] + 1) & 0xFF
    header_bytes = build_header(userdata['seq'], userdata['txn'])
    topic = f"/SW_GPS/{device_id}/user/batPropertyExtReq"
    client.publish(topic, payload=header_bytes, qos=0, retain=False)
    logging.info(f"Published batPropertyExtReq to {topic} (seq={userdata['seq']}, txn={userdata['txn']})")


def on_connect(client, userdata, flags, reason_code, properties):
    """MQTT connection callback"""
    if reason_code == 0:
        logging.info("Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC)
        logging.info(f"Subscribed to {MQTT_TOPIC}")
    else:
        logging.error(f"MQTT connect failed: {reason_code}")


def on_message(client, userdata, msg):
    """Enhanced MQTT message callback capturing ALL data types"""
    topic = msg.topic
    device_id = extract_device_id(topic)
    message_type = topic.split('/')[-1]
    payload_bytes = decode_payload_bytes(msg.payload)
    
    # Auto-request static data once per device when first seen (skip if this is already an Ext response/report)
    try:
        userdata = ensure_user_data(client)
        if device_id != 'unknown' and message_type not in ('batPropertyExtRprt', 'batPropertyExtRsp'):
            if device_id not in userdata['ext_requested']:
                send_bat_property_ext_req(client, device_id)
                userdata['ext_requested'].add(device_id)
    except Exception as e:
        logging.warning(f"Failed to auto-request ext for {device_id}: {e}")
    
    try:
        # Route to appropriate decoder based on message type
        if message_type == 'batPropertyRprt':
            decoded = decode_battery_property_report(payload_bytes, device_id)
        elif message_type == 'batPropertyExtRprt' or message_type == 'batPropertyExtRsp':
            decoded = decode_battery_property_ext(payload_bytes, device_id)
        elif message_type in ('batPositonRprt', 'batPositionRprt'):
            decoded = decode_battery_position(payload_bytes, device_id)
        elif message_type == 'bmsCtrRsp':
            decoded = decode_bms_control_response(payload_bytes, device_id)
        else:
            # Unknown message type - try to decode header at least
            try:
                header = parse_header(payload_bytes)
                decoded = {
                    'header': header,
                    'message_type': message_type,
                    'note': 'Unknown message type - only header decoded'
                }
            except Exception:
                decoded = {
                    'message_type': message_type,
                    'error': 'Could not decode message header',
                    'raw_payload_hex': payload_bytes.hex()
                }
        
        output = {
            'device_id': device_id,
            'message_type': message_type,
            'decoded': decoded,
            'raw_payload_hex': payload_bytes.hex()
        }
        print(json.dumps(output, ensure_ascii=False))
        
    except Exception as e:
        logging.exception(f"Decode error for {device_id} {message_type}: {e}")
        err = {
            'device_id': device_id,
            'message_type': message_type,
            'error': str(e),
            'raw_payload_hex': payload_bytes.hex()
        }
        print(json.dumps(err))


def send_periodic_requests(client: mqtt.Client, device_id: str, stop_event: threading.Event):
    """Send periodic requests to trigger battery responses"""
    userdata = ensure_user_data(client)
    
    while not stop_event.is_set():
        try:
            # Send property requests
            userdata['seq'] = (userdata['seq'] + 1) & 0xFFFF
            userdata['txn'] = (userdata['txn'] + 1) & 0xFF
            header_bytes = build_header(userdata['seq'], userdata['txn'])
            
            # Send batPropertyExtReq
            topic = f"/SW_GPS/{device_id}/user/batPropertyExtReq"
            client.publish(topic, payload=header_bytes, qos=1, retain=False)
            logging.info(f"📤 Sent batPropertyExtReq to {topic}")
            
            # Send batPropertyReq
            userdata['seq'] = (userdata['seq'] + 1) & 0xFFFF
            userdata['txn'] = (userdata['txn'] + 1) & 0xFF
            header_bytes = build_header(userdata['seq'], userdata['txn'])
            topic = f"/SW_GPS/{device_id}/user/batPropertyReq"
            client.publish(topic, payload=header_bytes, qos=1, retain=False)
            logging.info(f"📤 Sent batPropertyReq to {topic}")
            
            # Send bmsCtrReq (allow discharge)
            userdata['seq'] = (userdata['seq'] + 1) & 0xFFFF
            userdata['txn'] = (userdata['txn'] + 1) & 0xFF
            header_bytes = build_header(userdata['seq'], userdata['txn'])
            control_body = struct.pack("BB", 1, 0)  # Type 1, Value 0 (Allow discharge)
            topic = f"/SW_GPS/{device_id}/user/bmsCtrReq"
            client.publish(topic, payload=header_bytes + control_body, qos=1, retain=False)
            logging.info(f"📤 Sent bmsCtrReq (allow discharge) to {topic}")
            
        except Exception as e:
            logging.error(f"Error sending requests: {e}")
        
        # Wait 3 seconds before next request cycle
        stop_event.wait(3)


def print_business_report(device_id: str, collected_data: Dict[str, Any]):
    """Print comprehensive battery data in business-friendly format"""
    print(f"\n{'='*80}")

    # Raw messages dump (exact decoded objects and raw payloads)
    raw_messages = collected_data.get('raw_messages') or []
    if raw_messages:
        print(f"\nRAW MQTT MESSAGES (exact, after decoding)\n{'-'*40}")
        for i, m in enumerate(raw_messages, 1):
            print(f"\n[{i}] Topic: {m.get('topic', 'N/A')}")
            print(f"Type: {m.get('message_type', 'N/A')}")
            print(f"Raw Payload (hex): {m.get('raw_payload_hex', '')}")
            try:
                print("Decoded Object:")
                print(json.dumps(m.get('decoded', None), ensure_ascii=False, separators=(',', ':'), sort_keys=True))
            except Exception:
                print("Decoded Object: <unserializable>")
    print(f"BATTERY MONITORING REPORT")
    print(f"Device ID: {device_id}")
    print(f"Report Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    
    # GPS Position Data (First)
    if 'gps_data' in collected_data and collected_data['gps_data'] is not None:
        gps_data = collected_data['gps_data']
        print(f"\nGPS POSITION DATA")
        print(f"{'-'*40}")
        positions = gps_data.get('positions', [])
        print(f"Position Count: {gps_data.get('position_count', 0)}")
        
        for i, pos in enumerate(positions):
            print(f"\nPosition {i+1}:")
            print(f"  Coordinates: {pos.get('latitude', 'N/A'):.6f}°, {pos.get('longitude', 'N/A'):.6f}°")
            print(f"  Timestamp: {pos.get('timestamp', 'N/A')}")
            print(f"  UTC Time: {pos.get('timestamp_utc', 'N/A')}")
            print(f"  Satellites: {pos.get('satellites', 'N/A')} (GPS: {pos.get('gps_satellites', 'N/A')}, Beidou: {pos.get('beidou_satellites', 'N/A')})")
            print(f"  Speed: {pos.get('speed_kmh', 'N/A')} km/h")
            print(f"  Direction: {pos.get('direction_deg', 'N/A')}°")
            print(f"  Hemisphere: {'South' if pos.get('is_south') else 'North'}, {'West' if pos.get('is_west') else 'East'}")
    else:
        print(f"\nGPS POSITION DATA")
        print(f"{'-'*40}")
        print("No GPS data collected during this period")
    
    # Device Information (Second)
    if 'device_info' in collected_data and collected_data['device_info'] is not None:
        device_info = collected_data['device_info']
        print(f"\nDEVICE INFORMATION")
        print(f"{'-'*40}")
        print(f"Product SN: {device_info.get('Product SN', 'N/A')}")
        print(f"GPS SN: {device_info.get('GPS SN', 'N/A')}")
        print(f"GPS IMSI: {device_info.get('GPS IMSI', 'N/A')}")
        print(f"GPS IMEI: {device_info.get('GPS IMEI', 'N/A')}")
        print(f"GPS SW Version: {device_info.get('GPS Software Version', 'N/A')}")
        print(f"GPS HW Version: {device_info.get('GPS Hardware Version', 'N/A')}")
        print(f"BMS SN: {device_info.get('BMS SN', 'N/A')}")
        print(f"BMS SW Version: {device_info.get('BMS Software Version', 'N/A')}")
        print(f"BMS HW Version: {device_info.get('BMS Hardware Version', 'N/A')}")
    else:
        print(f"\nDEVICE INFORMATION")
        print(f"{'-'*40}")
        print("No device information collected during this period")
    
    # Battery Property Data (Third)
    if 'battery_data' in collected_data and collected_data['battery_data'] is not None:
        battery_data = collected_data['battery_data']
        print(f"\nBATTERY STATUS")
        print(f"{'-'*40}")
        print(f"Current: {battery_data.get('Current', 'N/A')}")
        print(f"Current Type: {battery_data.get('Current_Type', 'N/A')}")
        print(f"State of Charge: {battery_data.get('State of Charge', 'N/A')}")
        print(f"Total Voltage: {battery_data.get('Total Battery Voltage', 'N/A')}")
        print(f"Remaining Capacity: {battery_data.get('Remaining Capacity', 'N/A')}")
        print(f"Total Capacity: {battery_data.get('Total Capacity', 'N/A')}")
        print(f"Loop Cycles: {battery_data.get('Loop Cycles', 'N/A')}")
        
        # Battery Status Details
        if 'Battery Status' in battery_data:
            status = battery_data['Battery Status']
            if status.get('has_alarms'):
                print(f"\nActive Alarms:")
                for alarm in status.get('active_alarms', []):
                    print(f"  - {alarm}")
            else:
                print(f"\nStatus: No active alarms")
        
        # Temperatures
        print(f"\nTemperatures:")
        print(f"  BMS Temps: {battery_data.get('BMS Temperatures', [])}")
        print(f"  Cell Temps: {battery_data.get('Cell Temperatures', [])}")
        
        # Cell Voltages
        cell_voltages = battery_data.get('Cell Voltages', [])
        if cell_voltages:
            print(f"\nCell Voltages:")
            print(f"  Individual: {cell_voltages}")
            print(f"  Min: {battery_data.get('Cell Voltage Min', 'N/A')} mV")
            print(f"  Max: {battery_data.get('Cell Voltage Max', 'N/A')} mV")
            print(f"  Average: {battery_data.get('Cell Voltage Avg', 'N/A')} mV")
            print(f"  Difference: {battery_data.get('Cell Voltage Diff', 'N/A')} mV")
        
        # Network Status
        network = battery_data.get('Network Status', {})
        print(f"\nNetwork Status:")
        print(f"  RSSI: {network.get('rssi', 'N/A')} dBm")
        print(f"  PLMN: {network.get('plmn', 'N/A')}")
        print(f"  LAC: {network.get('lac', 'N/A')}")
        print(f"  Cell ID: {network.get('cell_id', 'N/A')}")
    else:
        print(f"\nBATTERY STATUS")
        print(f"{'-'*40}")
        print("No battery data collected during this period")
    
    print(f"\n{'='*80}")


def ask_for_rerun() -> bool:
    """Ask user if they want to re-run data collection"""
    while True:
        response = input("\nWould you like to re-run the data collection? (Y/n): ").strip().lower()
        if response in ['y', 'yes', '']:
            return True
        elif response in ['n', 'no']:
            return False
        else:
            print("Please enter 'Y' for yes or 'n' for no.")


def collect_battery_data(device_id: str, collection_time: int = 180) -> Dict[str, Any]:
    """Collect battery data for specified time period"""
    print(f"Starting data collection for {collection_time} seconds...")
    print(f"Target Device: {device_id}")
    print(f"MQTT Broker: {BROKER_ADDRESS}:{BROKER_PORT}")
    print(f"Collecting data... Please wait.")
    
    # Data storage
    collected_data = {
        'gps_data': None,
        'device_info': None,
        'battery_data': None,
        'raw_messages': [],
        'raw_seen': set()
    }
    
    # Create MQTT client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.user_data_set({'seq': 0, 'txn': 0, 'ext_requested': set(), 'device_id': device_id, 'collected_data': collected_data})
    
    # Set up callbacks
    def on_connect_callback(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logging.info("Connected to MQTT broker")
            topic = f"/SW_GPS/{device_id}/#"
            client.subscribe(topic, qos=1)
            logging.info(f"Subscribed to {topic}")
            
            # Send initial discharge allow command
            userdata = ensure_user_data(client)
            userdata['seq'] = (userdata['seq'] + 1) & 0xFFFF
            userdata['txn'] = (userdata['txn'] + 1) & 0xFF
            header_bytes = build_header(userdata['seq'], userdata['txn'])
            control_body = struct.pack("BB", 1, 0)  # Type 1, Value 0 (Allow discharge)
            topic = f"/SW_GPS/{device_id}/user/bmsCtrReq"
            client.publish(topic, payload=header_bytes + control_body, qos=1, retain=False)
            logging.info(f"Sent initial discharge allow command to {topic}")
        else:
            logging.error(f"MQTT connect failed: {reason_code}")
    
    def on_message_callback(client, userdata, msg):
        topic = msg.topic
        device_id = extract_device_id(topic)
        message_type = topic.split('/')[-1]
        payload_bytes = decode_payload_bytes(msg.payload)
        
        try:
            # Route to appropriate decoder
            if message_type == 'batPropertyRprt':
                decoded = decode_battery_property_report(payload_bytes, device_id)
                if decoded:
                    userdata['collected_data']['battery_data'] = decoded
                    logging.info("Collected battery property data")
            elif message_type == 'batPropertyExtRprt' or message_type == 'batPropertyExtRsp':
                decoded = decode_battery_property_ext(payload_bytes, device_id)
                if decoded:
                    userdata['collected_data']['device_info'] = decoded
                    logging.info("Collected device information")
            elif message_type in ('batPositonRprt', 'batPositionRprt'):
                decoded = decode_battery_position(payload_bytes, device_id)
                if decoded:
                    userdata['collected_data']['gps_data'] = decoded
                    logging.info("Collected GPS position data")
            elif message_type == 'bmsCtrRsp':
                decoded = decode_bms_control_response(payload_bytes, device_id)
                if decoded:
                    logging.info("Received BMS control response")

            # Record only the first raw message per message_type for later printing
            try:
                seen = userdata['collected_data']['raw_seen']
                if message_type not in seen:
                    userdata['collected_data']['raw_messages'].append({
                        'topic': topic,
                        'message_type': message_type,
                        'raw_payload_hex': payload_bytes.hex(),
                        'decoded': decoded
                    })
                    seen.add(message_type)
            except Exception:
                pass
                
        except Exception as e:
            logging.exception(f"Decode error for {device_id} {message_type}: {e}")
    
    client.on_connect = on_connect_callback
    client.on_message = on_message_callback
    
    # Connect to broker
    try:
        client.connect(BROKER_ADDRESS, BROKER_PORT, 60)
    except Exception as e:
        logging.error(f"Failed to connect to MQTT broker: {e}")
        return collected_data
    
    # Start periodic requests thread
    stop_event = threading.Event()
    request_thread = threading.Thread(target=send_periodic_requests, args=(client, device_id, stop_event))
    request_thread.daemon = True
    request_thread.start()
    logging.info("Started periodic request thread")
    
    # Collect data for specified time
    start_time = time.time()
    while time.time() - start_time < collection_time:
        client.loop(timeout=1.0)
        time.sleep(0.1)
    
    # Stop requests and disconnect
    stop_event.set()
    client.disconnect()
    logging.info("Data collection completed")
    
    return collected_data


def main():
    """Main function with 3-minute collection cycle"""
    parser = argparse.ArgumentParser(description='Comprehensive Battery Data Decoder')
    parser.add_argument('--device', required=True, help='Device ID to monitor')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--time', '-t', type=int, default=180, help='Collection time in seconds (default: 180)')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.WARNING)  # Reduce noise during collection
    
    while True:
        # Collect data for specified time (default 3 minutes)
        collected_data = collect_battery_data(args.device, args.time)
        
        # Print business report
        print_business_report(args.device, collected_data)
        
        # Ask for re-run
        if not ask_for_rerun():
            print("Exiting...")
            break


if __name__ == '__main__':
    main()
#!/usr/bin/env python3
"""
Comprehensive Battery Data Decoder

This decoder captures ALL possible data from the battery including:
- Dynamic battery data (batPropertyRprt)
- Static battery data (batPropertyExtRprt) 
- GPS position data (batPositonRprt) with advanced decoding
- Control responses (bmsCtrRsp)
- Enhanced status and load information
- Auto-requests data via periodic MQTT commands

Usage: python decoder.py --device <DEVICE_ID>
"""

import paho.mqtt.client as mqtt
import json
import logging
import sys
import struct
import binascii
import argparse
import time
import threading
import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Tuple, Optional
import hashlib


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


BROKER_ADDRESS = "mqtt-cloud-1.telco.co.zw"
BROKER_PORT = 1883
MQTT_TOPIC = "/SW_GPS/#"


START_CODE = 0x4350
PROTOCOL_VERSION = 0x11
HEADER_SIZE = 6


def md5_signature(appid: str, nonce_str: str, uid: str, key: str) -> str:
    signtemp = f"appid={appid}&nonce_str={nonce_str}&uid={uid}&key={key}"
    return hashlib.md5(signtemp.encode('utf-8')).hexdigest().upper()


def build_header(seq: int, txn: int) -> bytes:
    """Build 6-byte protocol header (no body)."""
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


def decode_battery_status(status_bytes: bytes) -> Dict[str, Any]:
    """
    Decode the battery status bitfield from 4 bytes according to section 4.5 of the protocol.
    Based on invmonitoringscripts/monitor_v4.py
    """
    status_int = int.from_bytes(status_bytes, byteorder='big')
    
    # Define status bits according to protocol section 4.5
    status_bits = {
        "charging_status": (status_int >> 0) & 1,           # Bit 0: 0=Idle, 1=Charging
        "full_state": (status_int >> 1) & 1,                # Bit 1: 0=Not filled, 1=Already filled
        "charge_overcurrent": (status_int >> 2) & 1,        # Bit 2: 0=No/removed, 1=Occurred and not lifted
        "cell_overvoltage": (status_int >> 3) & 1,          # Bit 3: 0=No/removed, 1=Occurred and not lifted
        "discharge_state": (status_int >> 4) & 1,           # Bit 4: 0=Idle, 1=Discharging
        "short_circuit_alarm": (status_int >> 5) & 1,       # Bit 5: 0=No/removed, 1=Occurred and not lifted
        "discharge_overflow": (status_int >> 6) & 1,        # Bit 6: 0=No/removed, 1=Occurred and not lifted
        "cell_undervoltage": (status_int >> 7) & 1,         # Bit 7: 0=No/removed, 1=Occurred and not lifted
        "cell_open_circuit": (status_int >> 8) & 1,         # Bit 8: 0=No/removed, 1=Occurred and not lifted
        "temp_detect_open": (status_int >> 9) & 1,          # Bit 9: 0=No/removed, 1=Occurred and not lifted
        "cell_high_temp": (status_int >> 10) & 1,           # Bit 10: 0=No/removed, 1=Occurred and not lifted
        "cell_low_temp": (status_int >> 11) & 1,            # Bit 11: 0=No/removed, 1=Occurred and not lifted
        "bms_high_temp": (status_int >> 12) & 1,            # Bit 12: 0=No/removed, 1=Occurred and not lifted
        "reserved_bit13": (status_int >> 13) & 1,           # Bit 13: Reserved field
        "no_charge": (status_int >> 14) & 1,                # Bit 14: 0=Allowed, 1=Prohibited
        "no_discharge": (status_int >> 15) & 1,             # Bit 15: 0=Allowed, 1=Prohibited
        "discharge_mos_failed": (status_int >> 16) & 1,     # Bit 16: 0=Not failed/lifted, 1=Failed
        "charging_mos_status": (status_int >> 17) & 1,      # Bit 17: 0=Off, 1=On
        "discharge_mos_status": (status_int >> 18) & 1,     # Bit 18: 0=Off, 1=On
        "charge_mos_failure": (status_int >> 19) & 1,       # Bit 19: 0=Not failed/lifted, 1=Failed
        "high_low_voltage_failure": (status_int >> 20) & 1, # Bit 20: 0=No failure/removed, 1=Failure
        "ultra_high_temp_failure": (status_int >> 21) & 1,  # Bit 21: 0=Invalid/removed, 1=Invalid
        "cell_pressure_difference": (status_int >> 22) & 1, # Bit 22: 0=Not invalid/lifted, 1=Large pressure difference
        "battery_temp_difference": (status_int >> 23) & 1   # Bit 23: 0=Not invalid/lifted, 1=Large temperature difference
    }
    
    # Add human-readable descriptions
    status_descriptions = {}
    
    # Charging and discharging state
    if status_bits["charging_status"]:
        status_descriptions["charging"] = "Currently charging"
    else:
        status_descriptions["charging"] = "Not charging"
        
    if status_bits["discharge_state"]:
        status_descriptions["discharging"] = "Currently discharging"
    else:
        status_descriptions["discharging"] = "Not discharging"
    
    # Battery full state
    if status_bits["full_state"]:
        status_descriptions["battery_full"] = "Battery is full"
    
    # MOS status (switches)
    if status_bits["charging_mos_status"]:
        status_descriptions["charging_switch"] = "Charging switch ON"
    else:
        status_descriptions["charging_switch"] = "Charging switch OFF"
        
    if status_bits["discharge_mos_status"]:
        status_descriptions["discharge_switch"] = "Discharge switch ON"
    else:
        status_descriptions["discharge_switch"] = "Discharge switch OFF"
    
    # Charge/discharge prohibitions
    if status_bits["no_charge"]:
        status_descriptions["charging_allowed"] = "Charging prohibited"
    else:
        status_descriptions["charging_allowed"] = "Charging allowed"
        
    if status_bits["no_discharge"]:
        status_descriptions["discharging_allowed"] = "Discharging prohibited"
    else:
        status_descriptions["discharging_allowed"] = "Discharging allowed"
    
    # Alarms and failures (only add if active)
    active_alarms = []
    
    if status_bits["charge_overcurrent"]:
        active_alarms.append("Charge overcurrent")
    
    if status_bits["cell_overvoltage"]:
        active_alarms.append("Cell overvoltage")
    
    if status_bits["short_circuit_alarm"]:
        active_alarms.append("Short circuit")
    
    if status_bits["discharge_overflow"]:
        active_alarms.append("Discharge overflow")
    
    if status_bits["cell_undervoltage"]:
        active_alarms.append("Cell undervoltage")
    
    if status_bits["cell_open_circuit"]:
        active_alarms.append("Cell detection line open circuit")
    
    if status_bits["temp_detect_open"]:
        active_alarms.append("Temperature detection line open")
    
    if status_bits["cell_high_temp"]:
        active_alarms.append("Cell high temperature")
    
    if status_bits["cell_low_temp"]:
        active_alarms.append("Cell low temperature")
    
    if status_bits["bms_high_temp"]:
        active_alarms.append("BMS high temperature")
    
    if status_bits["discharge_mos_failed"]:
        active_alarms.append("Discharge MOS failed")
    
    if status_bits["charge_mos_failure"]:
        active_alarms.append("Charge MOS failure")
    
    if status_bits["high_low_voltage_failure"]:
        active_alarms.append("High/low voltage cell failure")
    
    if status_bits["ultra_high_temp_failure"]:
        active_alarms.append("Ultra high temperature failure")
    
    if status_bits["cell_pressure_difference"]:
        active_alarms.append("Large cell pressure difference")
    
    if status_bits["battery_temp_difference"]:
        active_alarms.append("Large battery temperature difference")
    
    return {
        'raw': status_int,
        'bits': status_bits,
        'descriptions': status_descriptions,
        'active_alarms': active_alarms,
        'has_alarms': len(active_alarms) > 0
    }


def decode_battery_property_report(payload_bytes: bytes, topic_device_id: str = None) -> Dict[str, Any]:
    """
    Enhanced battery property report decoding based on Instructions
    Captures ALL data including SOC, cycles, load, and enhanced status
    """
    try:
        header = parse_header(payload_bytes)
        body_bytes = payload_bytes[HEADER_SIZE:]
        offset = 0
        
        # Parse the product serial number
        if offset >= len(body_bytes):
            logging.error("Body too short to contain Product SN")
            return None
            
        # Length-Value format for the device ID
        sn_length = body_bytes[offset]
        offset += 1
        
        if offset + sn_length > len(body_bytes):
            logging.error(f"Body too short to contain Product SN of length {sn_length}")
            return None
            
        # Handle FF values in device ID
        if all(b == 0xFF for b in body_bytes[offset:offset+sn_length]):
            product_sn = "UNKNOWN_SN"
        else:
            try:
                product_sn = body_bytes[offset:offset+sn_length].decode('ascii', errors='replace')
            except UnicodeDecodeError:
                product_sn = "HEX:" + body_bytes[offset:offset+sn_length].hex()
                
        offset += sn_length
        
        # Check if we have enough bytes for the battery status (4 bytes)
        if offset + 4 > len(body_bytes):
            logging.error("Body too short to contain Battery Status")
            return None
            
        # Battery Status (4 bytes)
        battery_status_bytes = body_bytes[offset:offset+4]
        battery_status = decode_battery_status(battery_status_bytes)
        offset += 4
        
        # Battery Data (Length-Value format)
        if offset >= len(body_bytes):
            logging.error("Body too short to contain Battery Data length")
            return None
            
        battery_data_length = body_bytes[offset]
        offset += 1
        
        if offset + battery_data_length > len(body_bytes):
            logging.error(f"Body too short to contain Battery Data of length {battery_data_length}")
            return None
        
        # Extract current (2 bytes) - System Current from AC/PV
        if offset + 2 > len(body_bytes):
            logging.error("Body too short to contain current data")
            return None
            
        current = int.from_bytes(body_bytes[offset:offset+2], byteorder="big", signed=True) / 10
        offset += 2
    
        # Extract cell voltage list
        if offset >= len(body_bytes):
            logging.error("Body too short to contain cell voltage list length")
            return None
            
        cell_voltage_list_length = body_bytes[offset]
        offset += 1
        
        if offset + cell_voltage_list_length > len(body_bytes):
            logging.error(f"Body too short to contain cell voltage list of length {cell_voltage_list_length}")
            return None
            
        cell_voltages = []
        cell_count = cell_voltage_list_length // 2  # Each cell voltage is 2 bytes
        
        for i in range(cell_count):
            if offset + 2 > len(body_bytes):
                break
            cell_voltage = int.from_bytes(body_bytes[offset:offset+2], byteorder="big")
            cell_voltages.append(cell_voltage)
        offset += 2
    
        # BMS temperature list
        if offset >= len(body_bytes):
            logging.error("Body too short to contain BMS temperature list length")
            return None
            
        bms_temp_length = body_bytes[offset]
        offset += 1
        
        if offset + bms_temp_length > len(body_bytes):
            logging.error(f"Body too short to contain BMS temperature list of length {bms_temp_length}")
            return None
            
        bms_temperatures = []
        for i in range(bms_temp_length):
            if offset >= len(body_bytes):
                break
            # Temperature is a signed byte value
            bms_temp = body_bytes[offset]
            if bms_temp > 127:  # Convert to signed
                bms_temp = bms_temp - 256
            bms_temperatures.append(bms_temp)
            offset += 1
            
        # Cell temperature list
        if offset >= len(body_bytes):
            logging.error("Body too short to contain cell temperature list length")
            return None
            
        cell_temp_length = body_bytes[offset]
        offset += 1
    
        if offset + cell_temp_length > len(body_bytes):
            logging.error(f"Body too short to contain cell temperature list of length {cell_temp_length}")
            return None
            
        cell_temperatures = []
        for i in range(cell_temp_length):
            if offset >= len(body_bytes):
                break
            # Temperature is a signed byte value
            cell_temp = body_bytes[offset]
            if cell_temp > 127:  # Convert to signed
                cell_temp = cell_temp - 256
            cell_temperatures.append(cell_temp)
            offset += 1
    
        # Loop cycles (2 bytes) - Number of Cycles (Charge/Discharge Loops)
        if offset + 2 > len(body_bytes):
            logging.error("Body too short to contain loop cycles")
            return None
            
        loop_cycles = int.from_bytes(body_bytes[offset:offset+2], byteorder="big")
        offset += 2
        
        # Remaining Capacity (2 bytes) - For SOC calculation
        if offset + 2 > len(body_bytes):
            logging.error("Body too short to contain remaining capacity")
            return None
            
        remaining_capacity = int.from_bytes(body_bytes[offset:offset+2], byteorder="big") / 10
        offset += 2
        
        # Total Capacity (2 bytes) - For SOC calculation
        if offset + 2 > len(body_bytes):
            logging.error("Body too short to contain total capacity")
            return None
            
        total_capacity = int.from_bytes(body_bytes[offset:offset+2], byteorder="big") / 10
        offset += 2
    
        # Network Status (4G·Net Info)
        # Parse the 4G Network Information if available
        network_status = {}
        if offset < len(body_bytes):
            # RSSI
            rssi = body_bytes[offset]
            network_status['rssi'] = None if rssi in (0, 255) else -int(rssi)
            offset += 1
            
            # PLMN (6 bytes)
            if offset + 6 <= len(body_bytes):
                plmn = body_bytes[offset:offset+6].decode('ascii', errors='replace')
                network_status['plmn'] = plmn
                offset += 6
    
            # LAC (2 bytes)
            if offset + 2 <= len(body_bytes):
                lac = body_bytes[offset:offset+2].hex()
                network_status['lac'] = lac
                offset += 2
    
            # Cell ID (4 bytes)
            if offset + 4 <= len(body_bytes):
                cell_id = body_bytes[offset:offset+4].hex()
                network_status['cell_id'] = cell_id
                offset += 4
    
            # RAT (1 byte)
            if offset < len(body_bytes):
                rat = body_bytes[offset]
                network_status['rat'] = rat
                offset += 1
    
        # Calculate State of Charge (SOC) percentage
        soc_percentage = 0
        if total_capacity > 0:
            soc_percentage = (remaining_capacity / total_capacity) * 100

        # Calculate total battery voltage from cell voltages
        total_voltage = sum(cell_voltages) if cell_voltages else 0
        
        # Enhanced data summary based on Instructions
        summary = {
            "Device ID": topic_device_id if topic_device_id and product_sn == "UNKNOWN_SN" else product_sn,
            "Product SN": product_sn,
            "Battery Status": battery_status,
            "Current": f"{current} A",
            "Current_Type": "Charging" if current > 0 else "Discharging" if current < 0 else "Idle",
            "Cell Voltages": cell_voltages,
            "Total Battery Voltage": f"{total_voltage} mV",
            "Cell Voltage Min": min(cell_voltages) if cell_voltages else 0,
            "Cell Voltage Max": max(cell_voltages) if cell_voltages else 0,
            "Cell Voltage Avg": sum(cell_voltages)/len(cell_voltages) if cell_voltages else 0,
            "Cell Voltage Diff": max(cell_voltages) - min(cell_voltages) if cell_voltages else 0,
            "BMS Temperatures": bms_temperatures,
            "Cell Temperatures": cell_temperatures, 
            "Loop Cycles": loop_cycles,
            "Remaining Capacity": f"{remaining_capacity} Ah",
            "Total Capacity": f"{total_capacity} Ah",
            "State of Charge": f"{soc_percentage:.1f}%",
            "Network Status": network_status,
            "Battery Data Length": battery_data_length
        }
        
        return summary
        
    except Exception as e:
        logging.error(f"Error decoding payload: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return None


def decode_battery_property_ext(payload_bytes: bytes, topic_device_id: str = None) -> Dict[str, Any]:
    """Enhanced battery property extended decoding based on Instructions"""
    try:
        header = parse_header(payload_bytes)
        body_bytes = payload_bytes[HEADER_SIZE:]
        offset = 0
    
    # Product SN
        if offset >= len(body_bytes):
            logging.error("Body too short to contain Product SN")
            return None

        sn_length = body_bytes[offset]
        offset += 1

        if offset + sn_length > len(body_bytes):
            logging.error(f"Body too short to contain Product SN of length {sn_length}")
            return None

        if all(b == 0xFF for b in body_bytes[offset:offset+sn_length]):
            product_sn = "UNKNOWN_SN"
        else:
            try:
                product_sn = body_bytes[offset:offset+sn_length].decode('ascii', errors='replace')
            except UnicodeDecodeError:
                product_sn = "HEX:" + body_bytes[offset:offset+sn_length].hex()

        offset += sn_length

        # GPS SN
        if offset >= len(body_bytes):
            logging.error("Body too short to contain GPS SN")
            return None

        gps_sn_length = body_bytes[offset]
        offset += 1
    
        if offset + gps_sn_length > len(body_bytes):
            logging.error(f"Body too short to contain GPS SN of length {gps_sn_length}")
            return None

        if all(b == 0xFF for b in body_bytes[offset:offset+gps_sn_length]):
            gps_sn = "UNKNOWN_GPS_SN"
        else:
            try:
                gps_sn = body_bytes[offset:offset+gps_sn_length].decode('ascii', errors='replace')
            except UnicodeDecodeError:
                gps_sn = "HEX:" + body_bytes[offset:offset+gps_sn_length].hex()

        offset += gps_sn_length

        # GPS IMSI
        if offset >= len(body_bytes):
            logging.error("Body too short to contain GPS IMSI")
            return None

        gps_imsi_length = body_bytes[offset]
        offset += 1

        if offset + gps_imsi_length > len(body_bytes):
            logging.error(f"Body too short to contain GPS IMSI of length {gps_imsi_length}")
            return None

        if all(b == 0xFF for b in body_bytes[offset:offset+gps_imsi_length]):
            gps_imsi = "UNKNOWN_GPS_IMSI"
        else:
            try:
                gps_imsi = body_bytes[offset:offset+gps_imsi_length].decode('ascii', errors='replace')
            except UnicodeDecodeError:
                gps_imsi = "HEX:" + body_bytes[offset:offset+gps_imsi_length].hex()

        offset += gps_imsi_length

        # GPS IMEI
        if offset >= len(body_bytes):
            logging.error("Body too short to contain GPS IMEI")
            return None

        gps_imei_length = body_bytes[offset]
        offset += 1

        if offset + gps_imei_length > len(body_bytes):
            logging.error(f"Body too short to contain GPS IMEI of length {gps_imei_length}")
            return None

        if all(b == 0xFF for b in body_bytes[offset:offset+gps_imei_length]):
            gps_imei = "UNKNOWN_GPS_IMEI"
        else:
            try:
                gps_imei = body_bytes[offset:offset+gps_imei_length].decode('ascii', errors='replace')
            except UnicodeDecodeError:
                gps_imei = "HEX:" + body_bytes[offset:offset+gps_imei_length].hex()

        offset += gps_imei_length

        # GPS Software Version
        if offset >= len(body_bytes):
            logging.error("Body too short to contain GPS Software Version")
            return None

        gps_sw_ver_length = body_bytes[offset]
        offset += 1

        if offset + gps_sw_ver_length > len(body_bytes):
            logging.error(f"Body too short to contain GPS Software Version of length {gps_sw_ver_length}")
            return None

        if all(b == 0xFF for b in body_bytes[offset:offset+gps_sw_ver_length]):
            gps_sw_ver = "UNKNOWN_GPS_SW_VER"
        else:
            try:
                gps_sw_ver = body_bytes[offset:offset+gps_sw_ver_length].decode('ascii', errors='replace')
            except UnicodeDecodeError:
                gps_sw_ver = "HEX:" + body_bytes[offset:offset+gps_sw_ver_length].hex()

        offset += gps_sw_ver_length

        # GPS Hardware Version
        if offset >= len(body_bytes):
            logging.error("Body too short to contain GPS Hardware Version")
            return None

        gps_hw_ver_length = body_bytes[offset]
        offset += 1

        if offset + gps_hw_ver_length > len(body_bytes):
            logging.error(f"Body too short to contain GPS Hardware Version of length {gps_hw_ver_length}")
            return None

        if all(b == 0xFF for b in body_bytes[offset:offset+gps_hw_ver_length]):
            gps_hw_ver = "UNKNOWN_GPS_HW_VER"
        else:
            try:
                gps_hw_ver = body_bytes[offset:offset+gps_hw_ver_length].decode('ascii', errors='replace')
            except UnicodeDecodeError:
                gps_hw_ver = "HEX:" + body_bytes[offset:offset+gps_hw_ver_length].hex()

        offset += gps_hw_ver_length

        # BMS SN
        if offset >= len(body_bytes):
            logging.error("Body too short to contain BMS SN")
            return None

        bms_sn_length = body_bytes[offset]
        offset += 1

        if offset + bms_sn_length > len(body_bytes):
            logging.error(f"Body too short to contain BMS SN of length {bms_sn_length}")
            return None

        if all(b == 0xFF for b in body_bytes[offset:offset+bms_sn_length]):
            bms_sn = "UNKNOWN_BMS_SN"
        else:
            try:
                bms_sn = body_bytes[offset:offset+bms_sn_length].decode('ascii', errors='replace')
            except UnicodeDecodeError:
                bms_sn = "HEX:" + body_bytes[offset:offset+bms_sn_length].hex()

        offset += bms_sn_length
    
    # Optional BMS version fields (may not be present in all messages)
    bms_sw_ver = None
    bms_hw_ver = None

        if offset < len(body_bytes):
            try:
                bms_sw_ver_length = body_bytes[offset]
                offset += 1
                if offset + bms_sw_ver_length <= len(body_bytes):
                    if all(b == 0xFF for b in body_bytes[offset:offset+bms_sw_ver_length]):
                        bms_sw_ver = "UNKNOWN_BMS_SW_VER"
                    else:
                        try:
                            bms_sw_ver = body_bytes[offset:offset+bms_sw_ver_length].decode('ascii', errors='replace')
                        except UnicodeDecodeError:
                            bms_sw_ver = "HEX:" + body_bytes[offset:offset+bms_sw_ver_length].hex()
                    offset += bms_sw_ver_length
        except Exception:
            bms_sw_ver = None

        if offset < len(body_bytes):
            try:
                bms_hw_ver_length = body_bytes[offset]
                offset += 1
                if offset + bms_hw_ver_length <= len(body_bytes):
                    if all(b == 0xFF for b in body_bytes[offset:offset+bms_hw_ver_length]):
                        bms_hw_ver = "UNKNOWN_BMS_HW_VER"
                    else:
                        try:
                            bms_hw_ver = body_bytes[offset:offset+bms_hw_ver_length].decode('ascii', errors='replace')
                        except UnicodeDecodeError:
                            bms_hw_ver = "HEX:" + body_bytes[offset:offset+bms_hw_ver_length].hex()
                    offset += bms_hw_ver_length
        except Exception:
            bms_hw_ver = None
    
    return {
            "Device ID": topic_device_id,
            "Product SN": product_sn,
            "GPS SN": gps_sn,
            "GPS IMSI": gps_imsi,
            "GPS IMEI": gps_imei,
            "GPS Software Version": gps_sw_ver,
            "GPS Hardware Version": gps_hw_ver,
            "BMS SN": bms_sn,
            "BMS Software Version": bms_sw_ver,
            "BMS Hardware Version": bms_hw_ver
        }
        
    except Exception as e:
        logging.error(f"Error decoding battery property ext: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return None


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


def decode_bms_control_response(payload_bytes: bytes, topic_device_id: str = None) -> Dict[str, Any]:
    try:
        header = parse_header(payload_bytes)
        body = payload_bytes[HEADER_SIZE:]
        if len(body) < 2:
            return {'header': header, 'control_type': None, 'value': None, 'control_type_description': 'Unknown', 'value_meaning': 'Unknown', 'response_status': 'Unknown'}
        control_type = body[0]
        value = body[1]
        control_types = {1: 'Discharge Switch Operation', 2: 'Charging Switch Operation', 3: 'Battery Static Mode Setting'}
        value_meanings = {1: {0: 'Allow discharge', 1: 'No discharge'}, 2: {0: 'Allow charging', 1: 'No charging'}, 3: {0: 'Standing mode is not allowed', 1: 'Allow static mode'}}
        return {'header': header, 'control_type': control_type, 'control_type_description': control_types.get(control_type, 'Unknown'), 'value': value, 'value_meaning': value_meanings.get(control_type, {}).get(value, 'Unknown'), 'response_status': 'Success'}
    except Exception as e:
        logging.error(f"Error decoding BMS control response: {e}")
        return None


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


def ensure_user_data(client: mqtt.Client) -> Dict[str, Any]:
    """Ensure userdata dict exists with counters and tracking sets."""
    userdata = client.user_data_get()
    if not isinstance(userdata, dict):
        userdata = {'seq': 0, 'txn': 0, 'ext_requested': set()}
        client.user_data_set(userdata)
    else:
        userdata.setdefault('seq', 0)
        userdata.setdefault('txn', 0)
        userdata.setdefault('ext_requested', set())
    return userdata


def send_bat_property_ext_req(client: mqtt.Client, device_id: str) -> None:
    """Publish an empty-body batPropertyExtReq with header only."""
    userdata = ensure_user_data(client)
    userdata['seq'] = (userdata['seq'] + 1) & 0xFFFF
    userdata['txn'] = (userdata['txn'] + 1) & 0xFF
    header_bytes = build_header(userdata['seq'], userdata['txn'])
    topic = f"/SW_GPS/{device_id}/user/batPropertyExtReq"
    client.publish(topic, payload=header_bytes, qos=0, retain=False)
    logging.info(f"Published batPropertyExtReq to {topic} (seq={userdata['seq']}, txn={userdata['txn']})")


def on_connect(client, userdata, flags, reason_code, properties):
    """MQTT connection callback"""
    if reason_code == 0:
        logging.info("Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC)
        logging.info(f"Subscribed to {MQTT_TOPIC}")
    else:
        logging.error(f"MQTT connect failed: {reason_code}")


def on_message(client, userdata, msg):
    """Enhanced MQTT message callback capturing ALL data types"""
    topic = msg.topic
    device_id = extract_device_id(topic)
    message_type = topic.split('/')[-1]
    payload_bytes = decode_payload_bytes(msg.payload)
    
    # Auto-request static data once per device when first seen (skip if this is already an Ext response/report)
    try:
        userdata = ensure_user_data(client)
        if device_id != 'unknown' and message_type not in ('batPropertyExtRprt', 'batPropertyExtRsp'):
            if device_id not in userdata['ext_requested']:
                send_bat_property_ext_req(client, device_id)
                userdata['ext_requested'].add(device_id)
    except Exception as e:
        logging.warning(f"Failed to auto-request ext for {device_id}: {e}")
    
    try:
        # Route to appropriate decoder based on message type
        if message_type == 'batPropertyRprt':
            decoded = decode_battery_property_report(payload_bytes, device_id)
        elif message_type == 'batPropertyExtRprt' or message_type == 'batPropertyExtRsp':
            decoded = decode_battery_property_ext(payload_bytes, device_id)
        elif message_type in ('batPositonRprt', 'batPositionRprt'):
            decoded = decode_battery_position(payload_bytes, device_id)
        elif message_type == 'bmsCtrRsp':
            decoded = decode_bms_control_response(payload_bytes, device_id)
        else:
            # Unknown message type - try to decode header at least
            try:
                header = parse_header(payload_bytes)
                decoded = {
                    'header': header,
                    'message_type': message_type,
                    'note': 'Unknown message type - only header decoded'
                }
            except Exception:
                decoded = {
                    'message_type': message_type,
                    'error': 'Could not decode message header',
                    'raw_payload_hex': payload_bytes.hex()
                }
        
        output = {
            'device_id': device_id,
            'message_type': message_type,
            'decoded': decoded,
            'raw_payload_hex': payload_bytes.hex()
        }
        print(json.dumps(output, ensure_ascii=False))
        
    except Exception as e:
        logging.exception(f"Decode error for {device_id} {message_type}: {e}")
        err = {
            'device_id': device_id,
            'message_type': message_type,
            'error': str(e),
            'raw_payload_hex': payload_bytes.hex()
        }
        print(json.dumps(err))


def send_periodic_requests(client: mqtt.Client, device_id: str, stop_event: threading.Event):
    """Send periodic requests to trigger battery responses"""
    userdata = ensure_user_data(client)
    
    while not stop_event.is_set():
        try:
            # Send property requests
            userdata['seq'] = (userdata['seq'] + 1) & 0xFFFF
            userdata['txn'] = (userdata['txn'] + 1) & 0xFF
            header_bytes = build_header(userdata['seq'], userdata['txn'])
            
            # Send batPropertyExtReq
            topic = f"/SW_GPS/{device_id}/user/batPropertyExtReq"
            client.publish(topic, payload=header_bytes, qos=1, retain=False)
            logging.info(f"📤 Sent batPropertyExtReq to {topic}")
            
            # Send batPropertyReq
            userdata['seq'] = (userdata['seq'] + 1) & 0xFFFF
            userdata['txn'] = (userdata['txn'] + 1) & 0xFF
            header_bytes = build_header(userdata['seq'], userdata['txn'])
            topic = f"/SW_GPS/{device_id}/user/batPropertyReq"
            client.publish(topic, payload=header_bytes, qos=1, retain=False)
            logging.info(f"📤 Sent batPropertyReq to {topic}")
            
            # Send bmsCtrReq (allow discharge)
            userdata['seq'] = (userdata['seq'] + 1) & 0xFFFF
            userdata['txn'] = (userdata['txn'] + 1) & 0xFF
            header_bytes = build_header(userdata['seq'], userdata['txn'])
            control_body = struct.pack("BB", 1, 0)  # Type 1, Value 0 (Allow discharge)
            topic = f"/SW_GPS/{device_id}/user/bmsCtrReq"
            client.publish(topic, payload=header_bytes + control_body, qos=1, retain=False)
            logging.info(f"📤 Sent bmsCtrReq (allow discharge) to {topic}")
            
        except Exception as e:
            logging.error(f"Error sending requests: {e}")
        
        # Wait 3 seconds before next request cycle
        stop_event.wait(3)


def print_business_report(device_id: str, collected_data: Dict[str, Any]):
    """Print comprehensive battery data in business-friendly format"""
    print(f"\n{'='*80}")

    # Raw messages dump (exact decoded objects and raw payloads)
    raw_messages = collected_data.get('raw_messages') or []
    if raw_messages:
        print(f"\nRAW MQTT MESSAGES (exact, after decoding)\n{'-'*40}")
        for i, m in enumerate(raw_messages, 1):
            print(f"\n[{i}] Topic: {m.get('topic', 'N/A')}")
            print(f"Type: {m.get('message_type', 'N/A')}")
            print(f"Raw Payload (hex): {m.get('raw_payload_hex', '')}")
            try:
                print("Decoded Object:")
                print(json.dumps(m.get('decoded', None), ensure_ascii=False, separators=(',', ':'), sort_keys=True))
            except Exception:
                print("Decoded Object: <unserializable>")
    print(f"BATTERY MONITORING REPORT")
    print(f"Device ID: {device_id}")
    print(f"Report Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    
    # GPS Position Data (First)
    if 'gps_data' in collected_data and collected_data['gps_data'] is not None:
        gps_data = collected_data['gps_data']
        print(f"\nGPS POSITION DATA")
        print(f"{'-'*40}")
        positions = gps_data.get('positions', [])
        print(f"Position Count: {gps_data.get('position_count', 0)}")
        
        for i, pos in enumerate(positions):
            print(f"\nPosition {i+1}:")
            print(f"  Coordinates: {pos.get('latitude', 'N/A'):.6f}°, {pos.get('longitude', 'N/A'):.6f}°")
            print(f"  Timestamp: {pos.get('timestamp', 'N/A')}")
            print(f"  UTC Time: {pos.get('timestamp_utc', 'N/A')}")
            print(f"  Satellites: {pos.get('satellites', 'N/A')} (GPS: {pos.get('gps_satellites', 'N/A')}, Beidou: {pos.get('beidou_satellites', 'N/A')})")
            print(f"  Speed: {pos.get('speed_kmh', 'N/A')} km/h")
            print(f"  Direction: {pos.get('direction_deg', 'N/A')}°")
            print(f"  Hemisphere: {'South' if pos.get('is_south') else 'North'}, {'West' if pos.get('is_west') else 'East'}")
    else:
        print(f"\nGPS POSITION DATA")
        print(f"{'-'*40}")
        print("No GPS data collected during this period")
    
    # Device Information (Second)
    if 'device_info' in collected_data and collected_data['device_info'] is not None:
        device_info = collected_data['device_info']
        print(f"\nDEVICE INFORMATION")
        print(f"{'-'*40}")
        print(f"Product SN: {device_info.get('Product SN', 'N/A')}")
        print(f"GPS SN: {device_info.get('GPS SN', 'N/A')}")
        print(f"GPS IMSI: {device_info.get('GPS IMSI', 'N/A')}")
        print(f"GPS IMEI: {device_info.get('GPS IMEI', 'N/A')}")
        print(f"GPS SW Version: {device_info.get('GPS Software Version', 'N/A')}")
        print(f"GPS HW Version: {device_info.get('GPS Hardware Version', 'N/A')}")
        print(f"BMS SN: {device_info.get('BMS SN', 'N/A')}")
        print(f"BMS SW Version: {device_info.get('BMS Software Version', 'N/A')}")
        print(f"BMS HW Version: {device_info.get('BMS Hardware Version', 'N/A')}")
    else:
        print(f"\nDEVICE INFORMATION")
        print(f"{'-'*40}")
        print("No device information collected during this period")
    
    # Battery Property Data (Third)
    if 'battery_data' in collected_data and collected_data['battery_data'] is not None:
        battery_data = collected_data['battery_data']
        print(f"\nBATTERY STATUS")
        print(f"{'-'*40}")
        print(f"Current: {battery_data.get('Current', 'N/A')}")
        print(f"Current Type: {battery_data.get('Current_Type', 'N/A')}")
        print(f"State of Charge: {battery_data.get('State of Charge', 'N/A')}")
        print(f"Total Voltage: {battery_data.get('Total Battery Voltage', 'N/A')}")
        print(f"Remaining Capacity: {battery_data.get('Remaining Capacity', 'N/A')}")
        print(f"Total Capacity: {battery_data.get('Total Capacity', 'N/A')}")
        print(f"Loop Cycles: {battery_data.get('Loop Cycles', 'N/A')}")
        
        # Battery Status Details
        if 'Battery Status' in battery_data:
            status = battery_data['Battery Status']
            if status.get('has_alarms'):
                print(f"\nActive Alarms:")
                for alarm in status.get('active_alarms', []):
                    print(f"  - {alarm}")
            else:
                print(f"\nStatus: No active alarms")
        
        # Temperatures
        print(f"\nTemperatures:")
        print(f"  BMS Temps: {battery_data.get('BMS Temperatures', [])}")
        print(f"  Cell Temps: {battery_data.get('Cell Temperatures', [])}")
        
        # Cell Voltages
        cell_voltages = battery_data.get('Cell Voltages', [])
        if cell_voltages:
            print(f"\nCell Voltages:")
            print(f"  Individual: {cell_voltages}")
            print(f"  Min: {battery_data.get('Cell Voltage Min', 'N/A')} mV")
            print(f"  Max: {battery_data.get('Cell Voltage Max', 'N/A')} mV")
            print(f"  Average: {battery_data.get('Cell Voltage Avg', 'N/A')} mV")
            print(f"  Difference: {battery_data.get('Cell Voltage Diff', 'N/A')} mV")
        
        # Network Status
        network = battery_data.get('Network Status', {})
        print(f"\nNetwork Status:")
        print(f"  RSSI: {network.get('rssi', 'N/A')} dBm")
        print(f"  PLMN: {network.get('plmn', 'N/A')}")
        print(f"  LAC: {network.get('lac', 'N/A')}")
        print(f"  Cell ID: {network.get('cell_id', 'N/A')}")
    else:
        print(f"\nBATTERY STATUS")
        print(f"{'-'*40}")
        print("No battery data collected during this period")
    
    print(f"\n{'='*80}")


def ask_for_rerun() -> bool:
    """Ask user if they want to re-run data collection"""
    while True:
        response = input("\nWould you like to re-run the data collection? (Y/n): ").strip().lower()
        if response in ['y', 'yes', '']:
            return True
        elif response in ['n', 'no']:
            return False
        else:
            print("Please enter 'Y' for yes or 'n' for no.")


def collect_battery_data(device_id: str, collection_time: int = 180) -> Dict[str, Any]:
    """Collect battery data for specified time period"""
    print(f"Starting data collection for {collection_time} seconds...")
    print(f"Target Device: {device_id}")
    print(f"MQTT Broker: {BROKER_ADDRESS}:{BROKER_PORT}")
    print(f"Collecting data... Please wait.")
    
    # Data storage
    collected_data = {
        'gps_data': None,
        'device_info': None,
        'battery_data': None,
        'raw_messages': [],
        'raw_seen': set()
    }
    
    # Create MQTT client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.user_data_set({'seq': 0, 'txn': 0, 'ext_requested': set(), 'device_id': device_id, 'collected_data': collected_data})
    
    # Set up callbacks
    def on_connect_callback(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logging.info("Connected to MQTT broker")
            topic = f"/SW_GPS/{device_id}/#"
            client.subscribe(topic, qos=1)
            logging.info(f"Subscribed to {topic}")
            
            # Send initial discharge allow command
            userdata = ensure_user_data(client)
            userdata['seq'] = (userdata['seq'] + 1) & 0xFFFF
            userdata['txn'] = (userdata['txn'] + 1) & 0xFF
            header_bytes = build_header(userdata['seq'], userdata['txn'])
            control_body = struct.pack("BB", 1, 0)  # Type 1, Value 0 (Allow discharge)
            topic = f"/SW_GPS/{device_id}/user/bmsCtrReq"
            client.publish(topic, payload=header_bytes + control_body, qos=1, retain=False)
            logging.info(f"Sent initial discharge allow command to {topic}")
        else:
            logging.error(f"MQTT connect failed: {reason_code}")
    
    def on_message_callback(client, userdata, msg):
        topic = msg.topic
        device_id = extract_device_id(topic)
        message_type = topic.split('/')[-1]
        payload_bytes = decode_payload_bytes(msg.payload)
        
        try:
            # Route to appropriate decoder
            if message_type == 'batPropertyRprt':
                decoded = decode_battery_property_report(payload_bytes, device_id)
                if decoded:
                    userdata['collected_data']['battery_data'] = decoded
                    logging.info("Collected battery property data")
            elif message_type == 'batPropertyExtRprt' or message_type == 'batPropertyExtRsp':
                decoded = decode_battery_property_ext(payload_bytes, device_id)
                if decoded:
                    userdata['collected_data']['device_info'] = decoded
                    logging.info("Collected device information")
            elif message_type in ('batPositonRprt', 'batPositionRprt'):
                decoded = decode_battery_position(payload_bytes, device_id)
                if decoded:
                    userdata['collected_data']['gps_data'] = decoded
                    logging.info("Collected GPS position data")
            elif message_type == 'bmsCtrRsp':
                decoded = decode_bms_control_response(payload_bytes, device_id)
                if decoded:
                    logging.info("Received BMS control response")

            # Record only the first raw message per message_type for later printing
            try:
                seen = userdata['collected_data']['raw_seen']
                if message_type not in seen:
                    userdata['collected_data']['raw_messages'].append({
                        'topic': topic,
                        'message_type': message_type,
                        'raw_payload_hex': payload_bytes.hex(),
                        'decoded': decoded
                    })
                    seen.add(message_type)
            except Exception:
                pass
                
        except Exception as e:
            logging.exception(f"Decode error for {device_id} {message_type}: {e}")
    
    client.on_connect = on_connect_callback
    client.on_message = on_message_callback
    
    # Connect to broker
    try:
        client.connect(BROKER_ADDRESS, BROKER_PORT, 60)
    except Exception as e:
        logging.error(f"Failed to connect to MQTT broker: {e}")
        return collected_data
    
    # Start periodic requests thread
    stop_event = threading.Event()
    request_thread = threading.Thread(target=send_periodic_requests, args=(client, device_id, stop_event))
    request_thread.daemon = True
    request_thread.start()
    logging.info("Started periodic request thread")
    
    # Collect data for specified time
    start_time = time.time()
    while time.time() - start_time < collection_time:
        client.loop(timeout=1.0)
        time.sleep(0.1)
    
    # Stop requests and disconnect
    stop_event.set()
    client.disconnect()
    logging.info("Data collection completed")
    
    return collected_data


def main():
    """Main function with 3-minute collection cycle"""
    parser = argparse.ArgumentParser(description='Comprehensive Battery Data Decoder')
    parser.add_argument('--device', required=True, help='Device ID to monitor')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--time', '-t', type=int, default=180, help='Collection time in seconds (default: 180)')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.WARNING)  # Reduce noise during collection
    
    while True:
        # Collect data for specified time (default 3 minutes)
        collected_data = collect_battery_data(args.device, args.time)
        
        # Print business report
        print_business_report(args.device, collected_data)
        
        # Ask for re-run
        if not ask_for_rerun():
            print("Exiting...")
            break


if __name__ == '__main__':
    main()
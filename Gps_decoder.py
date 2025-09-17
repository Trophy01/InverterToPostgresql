#!/usr/bin/env python3
"""
Corrected GPS payload decoder based on careful analysis of the SWS protocol.
This focuses specifically on the 21-byte position data format.
"""

# PUT YOUR PAYLOAD HERE
PAYLOAD = "43501100020216157147927230015768574090528035300c0000670000"

def analyze_position_data(pos_hex):
    """Analyze the 21-byte position data with different interpretations"""
    pos_bytes = bytes.fromhex(pos_hex)
    
    print(f"Position data: {pos_hex}")
    print(f"Length: {len(pos_bytes)} bytes")
    print()
    
    # Try to identify the correct structure by looking for reasonable values
    # Your data: 7147927230015768574090528035300c0000670000
    
    # Let's try different starting positions for lat/lon
    attempts = []
    
    # Attempt 1: Standard format (0-2 lat, 3-5 lon)
    lat_bytes = pos_bytes[0:3]  # 714792
    lon_bytes = pos_bytes[3:6]  # 723001
    attempts.append(("Standard", lat_bytes, lon_bytes, pos_bytes[6]))
    
    # Attempt 2: Different offset
    lat_bytes = pos_bytes[1:4]  # 479272
    lon_bytes = pos_bytes[4:7]  # 300157
    attempts.append(("Offset +1", lat_bytes, lon_bytes, pos_bytes[7]))
    
    # Attempt 3: Check if it's in a different format altogether
    # Maybe the coordinate encoding is different
    
    for attempt_name, lat_b, lon_b, flag_byte in attempts:
        print(f"=== {attempt_name} ===")
        print(f"Lat bytes: {lat_b.hex()}")
        print(f"Lon bytes: {lon_b.hex()}")
        print(f"Flag byte: 0x{flag_byte:02x}")
        
        # Try different coordinate interpretations
        coords = try_coordinate_formats(lat_b, lon_b, flag_byte)
        
        for method, lat, lon in coords:
            # Check if coordinates look reasonable for Zimbabwe
            zim_reasonable = -30 < lat < -10 and 20 < lon < 40
            marker = " *** ZIMBABWE RANGE ***" if zim_reasonable else ""
            print(f"  {method:20s}: {lat:10.6f}, {lon:10.6f}{marker}")
        
        print()

def try_coordinate_formats(lat_bytes, lon_bytes, ns_we_flag):
    """Try different ways to interpret the coordinate bytes"""
    results = []
    
    # Method 1: Pure decimal (treating as integer)
    lat_int = (lat_bytes[0] << 16) | (lat_bytes[1] << 8) | lat_bytes[2]
    lon_int = (lon_bytes[0] << 16) | (lon_bytes[1] << 8) | lon_bytes[2]
    
    # Try different divisors
    for divisor in [1000000, 100000, 10000, 1000, 600000, 360000]:
        lat_dec = lat_int / divisor
        lon_dec = lon_int / divisor
        
        # Apply hemisphere flags
        is_south = (ns_we_flag & 0x01) != 0
        is_west = (ns_we_flag & 0x02) != 0
        
        final_lat = -lat_dec if is_south else lat_dec
        final_lon = -lon_dec if is_west else lon_dec
        
        results.append((f"Int/{divisor}", final_lat, final_lon))
    
    # Method 2: BCD interpretation
    def bcd_to_int(bytes_data):
        result = 0
        for b in bytes_data:
            high = (b >> 4) & 0x0F
            low = b & 0x0F
            if high > 9 or low > 9:  # Invalid BCD
                return None
            result = result * 100 + (high * 10 + low)
        return result
    
    lat_bcd = bcd_to_int(lat_bytes)
    lon_bcd = bcd_to_int(lon_bytes)
    
    if lat_bcd is not None and lon_bcd is not None:
        for divisor in [1000000, 100000, 10000, 1000]:
            lat_dec = lat_bcd / divisor
            lon_dec = lon_bcd / divisor
            
            is_south = (ns_we_flag & 0x01) != 0
            is_west = (ns_we_flag & 0x02) != 0
            
            final_lat = -lat_dec if is_south else lat_dec
            final_lon = -lon_dec if is_west else lon_dec
            
            results.append((f"BCD/{divisor}", final_lat, final_lon))
    
    # Method 3: Degrees + decimal minutes
    # Format might be DDMM.MMMM for each coordinate
    if lat_bcd is not None and lon_bcd is not None:
        # Lat: first 2 digits = degrees, rest = decimal minutes
        lat_deg = lat_bcd // 10000
        lat_min = (lat_bcd % 10000) / 10000 * 60
        lat_final = lat_deg + lat_min / 60
        
        lon_deg = lon_bcd // 10000  
        lon_min = (lon_bcd % 10000) / 10000 * 60
        lon_final = lon_deg + lon_min / 60
        
        is_south = (ns_we_flag & 0x01) != 0
        is_west = (ns_we_flag & 0x02) != 0
        
        final_lat = -lat_final if is_south else lat_final
        final_lon = -lon_final if is_west else lon_final
        
        results.append(("Deg+Min", final_lat, final_lon))
    
    return results

def analyze_time_data(pos_hex):
    """Analyze potential time data in different positions"""
    pos_bytes = bytes.fromhex(pos_hex)
    
    print("=== TIME DATA ANALYSIS ===")
    
    # According to protocol: bytes 7-9 date, 10-12 time
    date_bytes = pos_bytes[7:10]   # 685740
    time_bytes = pos_bytes[10:13]  # 905280
    
    print(f"Assumed date bytes (7-9): {date_bytes.hex()}")
    print(f"Assumed time bytes (10-12): {time_bytes.hex()}")
    
    # Try different interpretations
    # Current time is around 10:55, so look for patterns that give us hour 10-11
    
    # Check if any 3-byte sequence gives us reasonable time
    for i in range(len(pos_bytes) - 2):
        test_bytes = pos_bytes[i:i+3]
        
        # Try as BCD time (hhmmss)
        h = ((test_bytes[0] >> 4) * 10) + (test_bytes[0] & 0x0F)
        m = ((test_bytes[1] >> 4) * 10) + (test_bytes[1] & 0x0F)
        s = ((test_bytes[2] >> 4) * 10) + (test_bytes[2] & 0x0F)
        
        if 0 <= h <= 23 and 0 <= m <= 59 and 0 <= s <= 59:
            current_time_match = h in [10, 11] and 50 <= m <= 59
            marker = " *** MATCHES CURRENT TIME ***" if current_time_match else ""
            print(f"  Bytes {i:2d}-{i+2:2d} ({test_bytes.hex()}): {h:02d}:{m:02d}:{s:02d}{marker}")

def main():
    print("=" * 80)
    print("DETAILED SWS GPS ANALYSIS")
    print("=" * 80)
    
    # Extract just the position data (skip header)
    payload_bytes = bytes.fromhex(PAYLOAD)
    print(f"Full payload: {PAYLOAD}")
    print(f"Header: {payload_bytes[0:6].hex()}")
    print(f"Position list length: {payload_bytes[6]}")
    print(f"First record length: {payload_bytes[7]}")
    
    # Extract the 21-byte position record
    pos_data_hex = PAYLOAD[16:58]  # 21 bytes * 2 chars = 42 chars, starting after header + length bytes
    
    print(f"Position record (21 bytes): {pos_data_hex}")
    print()
    
    # Analyze coordinates
    analyze_position_data(pos_data_hex)
    
    # Analyze time
    analyze_time_data(pos_data_hex)
    
    print("\n" + "=" * 80)
    print("RECOMMENDATIONS:")
    print("Look for coordinates in the 'ZIMBABWE RANGE' and time 'MATCHES CURRENT TIME'")
    print("These indicate the correct decoding method for your device.")

if __name__ == "__main__":
    main()
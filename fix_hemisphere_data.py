#!/usr/bin/env python3
"""
Fix longitude sign consistency in position tables.
This script corrects any negative longitude values to positive (East).
It does not modify latitude.
"""

import psycopg2
import psycopg2.extras
import sys

# Database configuration
DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "batteries"
DB_USER = "troy"
DB_PASSWORD = "s3rv3r5mx"

def get_connection():
    return psycopg2.connect(
        host=DB_HOST, 
        port=DB_PORT, 
        dbname=DB_NAME, 
        user=DB_USER, 
        password=DB_PASSWORD
    )

def fix_hemisphere_data():
    """Fix longitude sign in all position tables (make all lon positive)"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Get all position tables
            cur.execute("""
                SELECT tablename FROM pg_tables 
                WHERE schemaname='public' AND tablename LIKE 'pos_%'
            """)
            tables = [row[0] for row in cur.fetchall()]
            
            print(f"Found {len(tables)} position tables to check")
            
            total_fixed = 0
            for table in tables:
                print(f"\nChecking table: {table}")
                
                # Count records with negative longitude (we want all longitudes positive/East)
                cur.execute(f"SELECT COUNT(*) FROM {table} WHERE lon < 0")
                negative_lon_count = cur.fetchone()[0]
                
                if negative_lon_count > 0:
                    print(f"  Found {negative_lon_count} records with negative longitude")
                    
                    # Fix the records by making longitude positive
                    cur.execute(f"""
                        UPDATE {table}
                        SET lon = abs(lon)
                        WHERE lon < 0
                    """)
                    fixed_count = cur.rowcount
                    total_fixed += fixed_count
                    print(f"  Fixed {fixed_count} records")
                else:
                    print(f"  No negative longitude records found")
            
            conn.commit()
            print(f"\nTotal records fixed: {total_fixed}")
            
            if total_fixed > 0:
                print("Longitude data has been corrected!")
                print("All longitude values are now positive (East)")
            else:
                print("No corrections needed - all longitude data was already positive")

if __name__ == "__main__":
    try:
        fix_hemisphere_data()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

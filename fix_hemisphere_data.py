#!/usr/bin/env python3
"""
Fix hemisphere data inconsistency in position tables.
This script corrects any positive latitude values to negative for Zimbabwe (Southern Hemisphere).
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
    """Fix hemisphere data in all position tables"""
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
                
                # Count records with positive latitude (should be negative for Zimbabwe)
                cur.execute(f"SELECT COUNT(*) FROM {table} WHERE lat > 0")
                positive_lat_count = cur.fetchone()[0]
                
                if positive_lat_count > 0:
                    print(f"  Found {positive_lat_count} records with positive latitude")
                    
                    # Fix the records by making latitude negative
                    cur.execute(f"""
                        UPDATE {table} 
                        SET lat = -abs(lat) 
                        WHERE lat > 0
                    """)
                    fixed_count = cur.rowcount
                    total_fixed += fixed_count
                    print(f"  Fixed {fixed_count} records")
                else:
                    print(f"  No positive latitude records found")
            
            conn.commit()
            print(f"\nTotal records fixed: {total_fixed}")
            
            if total_fixed > 0:
                print("✅ Hemisphere data has been corrected!")
                print("All latitude values are now negative (Southern Hemisphere)")
            else:
                print("✅ No corrections needed - all data was already correct")

if __name__ == "__main__":
    try:
        fix_hemisphere_data()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

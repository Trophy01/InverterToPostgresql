import argparse
import psycopg2
import psycopg2.extras

DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "batteries"
DB_USER = "troy"
DB_PASSWORD = "s3rv3r5mx"


def _dsn() -> str:
    return f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"


def fetchone(sql: str):
    with psycopg2.connect(_dsn()) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return cur.fetchone()


def fetchall(sql: str):
    with psycopg2.connect(_dsn()) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return cur.fetchall()


def latest_position(device_id: str):
    table = f"pos_{''.join(c for c in device_id if c.isalnum() or c == '_')}"
    row = fetchone(f"SELECT * FROM {table} ORDER BY time DESC LIMIT 1;")
    return row


def print_latest_position(device_id: str):
    row = latest_position(device_id)
    if not row:
        print("No position data found.")
        return
    print(f"Device {device_id} latest position:")
    print(f"  time: {row['time']}")
    print(f"  lat: {row['lat']}")
    print(f"  lon: {row['lon']}")
    if row.get('direction') is not None:
        print(f"  direction: {row['direction']}")
    if row.get('sats_total') is not None:
        print(f"  satellites: {row['sats_total']} (GPS {row.get('sats_gps')}, Beidou {row.get('sats_beidou')})")


def latest_status(device_id: str):
    table = f"status_{''.join(c for c in device_id if c.isalnum() or c == '_')}"
    row = fetchone(f"SELECT * FROM {table} ORDER BY time DESC LIMIT 1;")
    return row


def print_latest_status(device_id: str):
    row = latest_status(device_id)
    if not row:
        print("No status data found.")
        return
    print(f"Device {device_id} latest status:")
    print(f"  time: {row['time']}")
    print(f"  current_amps: {row['current_amps']}")
    print(f"  soc_percent: {row['soc_percent']}")
    print(f"  total_voltage_mv: {row['total_voltage_mv']}")
    print(f"  status_text: {row['status_text']}")


def counts(device_id: str):
    did = ''.join(c for c in device_id if c.isalnum() or c == '_')
    tables = [f"pos_{did}", f"status_{did}", f"temps_{did}", f"cells_{did}", f"net_{did}"]
    out = {}
    for t in tables:
        try:
            row = fetchone(f"SELECT COUNT(*) AS c FROM {t};")
            out[t] = row['c'] if row else 0
        except Exception:
            out[t] = 'N/A'
    return out


def print_counts(device_id: str):
    out = counts(device_id)
    for t, c in out.items():
        print(f"{t}: {c}")


def main():
    parser = argparse.ArgumentParser(description="Query batteries TimescaleDB")
    parser.add_argument("device", help="Device ID (e.g., GPS SN)")
    parser.add_argument("action", choices=["latest_position", "latest_status", "counts"], help="Query action")
    args = parser.parse_args()

    if args.action == "latest_position":
        print_latest_position(args.device)
    elif args.action == "latest_status":
        print_latest_status(args.device)
    elif args.action == "counts":
        print_counts(args.device)


if __name__ == "__main__":
    main()



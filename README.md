# Battery Monitoring and Ingestion

A Python toolkit for collecting battery telemetry over MQTT, decoding device messages, and writing them to PostgreSQL. Includes a lightweight web dashboard and utilities for requesting data and sending control commands.

## Contents
- Overview
- Prerequisites
- Quick start
- Database and SSH tunnel
- Telemetry to PostgreSQL (ingestion window and batch write)
- Position to PostgreSQL (GPS decoding)
- Web dashboard
- Utilities (request and control)
- Querying the database
- Troubleshooting

## Overview
Devices publish under `/SW_GPS/{device_id}/user/*`. This project:
- Subscribes to MQTT and decodes battery messages (dynamic, static, and position)
- Logs outgoing requests and incoming payloads with exact hex
- Collects telemetry for a fixed window (default 2 minutes 30 seconds) and then writes to PostgreSQL using per-device tables

Per-device tables (auto-created on first write):
- `status_{device}`: snapshot (current, SOC, voltage, capacity, cycles, status)
- `temps_{device}`: BMS and cell temperatures
- `cells_{device}`: per-cell voltages
- `net_{device}`: radio network metrics
- `pos_{device}`: positions (time, lat, lon, satellites, direction)
- `info_{device}`: identification (IMEI, IMSI, versions)

## Prerequisites
- Python 3.13 (or 3.10+ should work)
- A running MQTT broker reachable from this host
- A PostgreSQL instance reachable on localhost via SSH tunnel or directly

Install dependencies in a virtualenv (recommended):
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt  # if present, else: pip install paho-mqtt psycopg2-binary flask plotly
```

## Quick start
1) Start an SSH tunnel to the remote PostgreSQL (example):
```bash
ssh -L 5432:127.0.0.1:5432 sa@154.119.80.42
```
2) Run the ingest bridge for 2m30s and write to PostgreSQL:
```bash
python mqtt_to_postgres.py -v \
  --device 862317043590129 \
  --db-host 127.0.0.1 --db-port 5432 \
  --db-name batteries --db-user troy --db-password s3rv3r5mx
```
This will:
- Subscribe to `/SW_GPS/#`
- Send periodic requests (`batPropertyExtReq`, `batPropertyReq`, `batPositionReq`) to listed devices
- Log outgoing/incoming topics and hex payloads
- Decode messages, collect in memory, and after 150 seconds batch-insert into PostgreSQL
- Print a readable summary of what was decoded and stored

## Database and SSH tunnel
Default DB settings used by the scripts (override with CLI flags):
- host: `127.0.0.1`
- port: `5432`
- database: `batteries`
- user: `troy`
- password: `s3rv3r5mx`

Open a tunnel before running the bridge:
```bash
ssh -L 5432:127.0.0.1:5432 sa@154.119.80.42
```
You can test connectivity with:
```bash
PGPASSWORD=s3rv3r5mx psql -h 127.0.0.1 -p 5432 -U troy -d batteries -c "select now();"
```

## Telemetry to PostgreSQL (ingestion window and batch write)
Use `mqtt_to_postgres.py` to ingest dynamic battery telemetry and write to DB.
```bash
python mqtt_to_postgres.py -v \
  --device <DEVICE_ID> \
  --db-host 127.0.0.1 --db-port 5432 \
  --db-name batteries --db-user troy --db-password s3rv3r5mx
```
Behavior:
- Sends periodic requests to the device(s) to stimulate responses
- Decodes `batPropertyRprt` (dynamic), `batPropertyExtRprt/Rsp` (static), and `batPositionRprt/PositonRprt` (GPS)
- Collects data for 150 seconds, then writes to the per-device tables
- Prints a clear summary showing topics seen, tables written, and key fields stored

Notes:
- You can seed device IDs via environment variable `MQTT_DEVICES` (comma-separated) or `--device` flags.
- Outgoing requests and all incoming raw payloads are logged with hex for traceability.

## Position to PostgreSQL (GPS decoding)
Position data is decoded and stored by `mqtt_to_postgres.py` during the same collection window. The GPS decoder:
- Locates 21-byte GPS records in position messages
- Decodes latitude/longitude using BCD and signed fallbacks
- Validates timestamps and extracts satellites, speed, and direction
- Inserts rows into `pos_{device}` with timestamp, coordinates, satellites and direction

To increase the chance of GPS data:
- Include the device in `--device` so `batPositionReq` is sent periodically
- Let the bridge run the full 2m30s window

## Web dashboard
A simple Flask app to visualize data (customize as needed):
```bash
python app.py
```
- Serves at `http://localhost:5000`
- Ensure your DB connection details in the app are correct (update if needed)

## Utilities
### Send request cycles to a device
`send_battery_requests.py` publishes common requests and logs responses.
```bash
python send_battery_requests.py --device 862317043581508 -v
```
Options:
- `--cycles N` and `--delay S` to control pacing
- Subscribes to relevant response topics and logs incoming headers

### Control commands
`mqtt_send_bms_ctrl.py` can publish BMS control requests.
```bash
python mqtt_send_bms_ctrl.py <device_id> <control_type> <value>
```
Refer to your protocol documentation for control types and values.

## Querying the database
Examples (psql):
```sql
-- list device tables
SELECT tablename FROM pg_tables WHERE schemaname='public' AND (
  tablename LIKE 'pos_%' OR tablename LIKE 'status_%' OR tablename LIKE 'info_%'
) ORDER BY tablename;

-- latest status for a device
SELECT * FROM status_862317043590129 ORDER BY time DESC LIMIT 1;

-- last 50 positions
SELECT time, lat, lon, sats_total FROM pos_862317043590129 ORDER BY time DESC LIMIT 50;
```

## Troubleshooting
- No DB writes: confirm SSH tunnel and DB credentials
- No incoming messages: verify MQTT broker/port and topic base, ensure device IDs are correct
- Only static info appears: keep the ingest running for the full window; not all devices respond immediately to position/property requests
- Permissions: ensure DB user can create tables and insert rows in the `batteries` database

---

## Pushing this project to GitLab
Create the remote repository first (at `https://git.telco.co.zw/troy/inverter_to_postgresql`). Then run from the project root:
```bash
git init
# if you prefer main as the default branch
git checkout -b main

git add .
git commit -m "Initial import: battery monitoring, MQTT→PostgreSQL bridge, utilities, dashboard"

git remote add origin https://git.telco.co.zw/troy/inverter_to_postgresql.git
# if the repository already exists and is empty, push the new branch
git push -u origin main
```
If the repository already has content and you need to force replace with this codebase, use with caution:
```bash
git push -u origin main --force
```

GitLab instance reference: [`https://git.telco.co.zw/troy/inverter_to_postgresql`](https://git.telco.co.zw/troy/inverter_to_postgresql)

# Battery Web UI

Flat pastel dashboard to browse battery telemetry from PostgreSQL.

## Setup

```
cd /home/trophy/BatteryMonitoring2/web
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Optionally set DB credentials (defaults match your existing setup):

```
export DB_HOST=127.0.0.1
export DB_PORT=5432
export DB_NAME=batteries
export DB_USER=troy
export DB_PASS=s3rv3r5mx
```

## Run

```
python app.py
```

Open http://127.0.0.1:5000

- Device selector is populated from `info_*` tables (fallback to `pos_*`).
- Charts: SOC/Voltage/Current, Temperatures, Cell Voltages, Positions (satellites & direction).
- Time window selector: 2h/6h/12h/24h.

-- sql queries for the database
-- Device ID eg: 862317043590129)

-- =============================================
-- QUICK QUERIES (use after running install_helpers.sql)
-- =============================================

-- A) Latest status for ALL devices
SELECT *
FROM public.latest_status_all()
ORDER BY status_time DESC NULLS LAST;

-- B) Latest full snapshot for ALL devices
SELECT *
FROM public.latest_full_snapshot_all()
ORDER BY COALESCE(pos_time, status_time) DESC NULLS LAST;

-- C) Latest full snapshot for ONE device (replace <DEVICE_ID>)
SELECT *
FROM public.latest_full_snapshot_all()
WHERE device_id = '<DEVICE_ID>'
ORDER BY COALESCE(pos_time, status_time) DESC NULLS LAST;

-- D) Latest positions (device, pos_time, lat, lon) across ALL devices
SELECT device_id, pos_time, lat, lon
FROM public.latest_full_snapshot_all()
ORDER BY pos_time DESC NULLS LAST;

-- E) Latest network signal across ALL devices
SELECT device_id, net_time, rssi, rsrp, rsrq, snr, network_type
FROM public.latest_full_snapshot_all()
ORDER BY net_time DESC NULLS LAST;

-- F) Latest temperatures across ALL devices
SELECT device_id, temps_time, bms_temps_c, cell_temps_c
FROM public.latest_full_snapshot_all()
ORDER BY temps_time DESC NULLS LAST;

-- G) Latest cell voltages across ALL devices
SELECT device_id, cells_time, cell_voltages_mv
FROM public.latest_full_snapshot_all()
ORDER BY cells_time DESC NULLS LAST;

-- H) Devices with no GPS position recorded yet
SELECT device_id
FROM public.latest_full_snapshot_all()
WHERE pos_time IS NULL
ORDER BY device_id;

-- I) Devices with no recent status in last 24h
SELECT device_id, status_time
FROM public.latest_status_all()
WHERE status_time < NOW() - INTERVAL '24 hours' OR status_time IS NULL
ORDER BY status_time NULLS FIRST;

-- J) Simple counts for a device (replace <DEVICE_ID>)
SELECT 'pos_'    || '<DEVICE_ID>' AS table, COUNT(*) AS rows FROM public.pos_<DEVICE_ID>
UNION ALL SELECT 'status_' || '<DEVICE_ID>', COUNT(*) FROM public.status_<DEVICE_ID>
UNION ALL SELECT 'temps_'  || '<DEVICE_ID>', COUNT(*) FROM public.temps_<DEVICE_ID>
UNION ALL SELECT 'cells_'  || '<DEVICE_ID>', COUNT(*) FROM public.cells_<DEVICE_ID>
UNION ALL SELECT 'net_'    || '<DEVICE_ID>', COUNT(*) FROM public.net_<DEVICE_ID>
ORDER BY 1;

-- RUN ONCE: install helper functions for simple querying
-- After running this block once, you can use easy SELECTs like:
--   SELECT * FROM public.latest_status_all() ORDER BY time DESC;
--   SELECT * FROM public.latest_full_snapshot_all() ORDER BY COALESCE(pos_time, status_time) DESC;
DO $$
BEGIN
  -- latest_status_all()
  EXECUTE $$
  CREATE OR REPLACE FUNCTION public.latest_status_all()
  RETURNS TABLE (
    device_id TEXT,
    time TIMESTAMPTZ,
    current_amps DOUBLE PRECISION,
    current_type TEXT,
    soc_percent DOUBLE PRECISION,
    total_voltage_mv INTEGER,
    remaining_capacity_ah DOUBLE PRECISION,
    total_capacity_ah DOUBLE PRECISION,
    loop_cycles INTEGER,
    status_text TEXT
  )
  LANGUAGE plpgsql AS $$$
  DECLARE r RECORD;
  BEGIN
    CREATE TEMP TABLE tmp_latest_status(
      device_id TEXT,
      time TIMESTAMPTZ,
      current_amps DOUBLE PRECISION,
      current_type TEXT,
      soc_percent DOUBLE PRECISION,
      total_voltage_mv INTEGER,
      remaining_capacity_ah DOUBLE PRECISION,
      total_capacity_ah DOUBLE PRECISION,
      loop_cycles INTEGER,
      status_text TEXT
    ) ON COMMIT DROP;

    FOR r IN
      SELECT table_name FROM information_schema.tables
      WHERE table_schema='public' AND table_name LIKE 'status_%'
    LOOP
      EXECUTE format(
        'INSERT INTO tmp_latest_status
         SELECT %L, time, current_amps, current_type, soc_percent, total_voltage_mv,
                remaining_capacity_ah, total_capacity_ah, loop_cycles, status_text
         FROM %I ORDER BY time DESC LIMIT 1',
        substring(r.table_name from 8), r.table_name
      );
    END LOOP;

    RETURN QUERY SELECT * FROM tmp_latest_status;
  END $$$;
  $$;

  -- latest_full_snapshot_all()
  EXECUTE $$
  CREATE OR REPLACE FUNCTION public.latest_full_snapshot_all()
  RETURNS TABLE (
    device_id TEXT,
    product_sn TEXT,
    gps_sn TEXT,
    gps_imsi TEXT,
    gps_imei TEXT,
    gps_sw TEXT,
    gps_hw TEXT,
    bms_sn TEXT,
    status_time TIMESTAMPTZ,
    current_amps DOUBLE PRECISION,
    current_type TEXT,
    soc_percent DOUBLE PRECISION,
    total_voltage_mv INTEGER,
    remaining_capacity_ah DOUBLE PRECISION,
    total_capacity_ah DOUBLE PRECISION,
    loop_cycles INTEGER,
    status_text TEXT,
    pos_time TIMESTAMPTZ,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    direction DOUBLE PRECISION,
    sats_total INTEGER,
    temps_time TIMESTAMPTZ,
    bms_temps_c INTEGER[],
    cell_temps_c INTEGER[],
    cells_time TIMESTAMPTZ,
    cell_voltages_mv INTEGER[],
    net_time TIMESTAMPTZ,
    rssi INTEGER,
    rsrp INTEGER,
    rsrq INTEGER,
    snr INTEGER,
    network_type TEXT
  )
  LANGUAGE plpgsql AS $$$
  DECLARE r RECORD;
  BEGIN
    CREATE TEMP TABLE tmp_devices(device_id TEXT) ON COMMIT DROP;
    INSERT INTO tmp_devices(device_id)
    SELECT SUBSTRING(table_name FROM 6)
    FROM information_schema.tables
    WHERE table_schema='public' AND table_name LIKE 'info_%';

    CREATE TEMP TABLE tmp_pos(device_id TEXT, time TIMESTAMPTZ, lat DOUBLE PRECISION, lon DOUBLE PRECISION, direction DOUBLE PRECISION, sats_total INT) ON COMMIT DROP;
    FOR r IN (
      SELECT 'pos_' || device_id AS tbl, device_id FROM tmp_devices
    ) LOOP
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = r.tbl) THEN
        EXECUTE format('INSERT INTO tmp_pos SELECT %L, time, lat, lon, direction, sats_total FROM %I ORDER BY time DESC LIMIT 1;', r.device_id, r.tbl);
      END IF;
    END LOOP;

    CREATE TEMP TABLE tmp_status(device_id TEXT, time TIMESTAMPTZ, current_amps DOUBLE PRECISION, current_type TEXT, soc_percent DOUBLE PRECISION, total_voltage_mv INT, remaining_capacity_ah DOUBLE PRECISION, total_capacity_ah DOUBLE PRECISION, loop_cycles INT, status_text TEXT) ON COMMIT DROP;
    FOR r IN (
      SELECT 'status_' || device_id AS tbl, device_id FROM tmp_devices
    ) LOOP
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = r.tbl) THEN
        EXECUTE format('INSERT INTO tmp_status SELECT %L, time, current_amps, current_type, soc_percent, total_voltage_mv, remaining_capacity_ah, total_capacity_ah, loop_cycles, status_text FROM %I ORDER BY time DESC LIMIT 1;', r.device_id, r.tbl);
      END IF;
    END LOOP;

    CREATE TEMP TABLE tmp_temps(device_id TEXT, time TIMESTAMPTZ, bms_temps_c INT[], cell_temps_c INT[]) ON COMMIT DROP;
    FOR r IN (
      SELECT 'temps_' || device_id AS tbl, device_id FROM tmp_devices
    ) LOOP
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = r.tbl) THEN
        EXECUTE format('INSERT INTO tmp_temps SELECT %L, time, bms_temps_c, cell_temps_c FROM %I ORDER BY time DESC LIMIT 1;', r.device_id, r.tbl);
      END IF;
    END LOOP;

    CREATE TEMP TABLE tmp_cells(device_id TEXT, time TIMESTAMPTZ, cell_voltages_mv INT[]) ON COMMIT DROP;
    FOR r IN (
      SELECT 'cells_' || device_id AS tbl, device_id FROM tmp_devices
    ) LOOP
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = r.tbl) THEN
        EXECUTE format('INSERT INTO tmp_cells SELECT %L, time, cell_voltages_mv FROM %I ORDER BY time DESC LIMIT 1;', r.device_id, r.tbl);
      END IF;
    END LOOP;

    CREATE TEMP TABLE tmp_net(device_id TEXT, time TIMESTAMPTZ, rssi INT, rsrp INT, rsrq INT, snr INT, network_type TEXT) ON COMMIT DROP;
    FOR r IN (
      SELECT 'net_' || device_id AS tbl, device_id FROM tmp_devices
    ) LOOP
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = r.tbl) THEN
        EXECUTE format('INSERT INTO tmp_net SELECT %L, time, rssi, rsrp, rsrq, snr, network_type FROM %I ORDER BY time DESC LIMIT 1;', r.device_id, r.tbl);
      END IF;
    END LOOP;

    CREATE TEMP TABLE tmp_info(device_id TEXT, inserted_at TIMESTAMPTZ, product_sn TEXT, gps_sn TEXT, gps_imsi TEXT, gps_imei TEXT, gps_sw TEXT, gps_hw TEXT, bms_sn TEXT, bms_sw TEXT, bms_hw TEXT) ON COMMIT DROP;
    FOR r IN (
      SELECT 'info_' || device_id AS tbl, device_id FROM tmp_devices
    ) LOOP
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = r.tbl) THEN
        EXECUTE format('INSERT INTO tmp_info SELECT %L, inserted_at, product_sn, gps_sn, gps_imsi, gps_imei, gps_sw, gps_hw, bms_sn, bms_sw, bms_hw FROM %I ORDER BY inserted_at DESC LIMIT 1;', r.device_id, r.tbl);
      END IF;
    END LOOP;

    RETURN QUERY
    SELECT
      d.device_id,
      i.product_sn,
      i.gps_sn,
      i.gps_imsi,
      i.gps_imei,
      i.gps_sw,
      i.gps_hw,
      i.bms_sn,
      s.time         AS status_time,
      s.current_amps,
      s.current_type,
      s.soc_percent,
      s.total_voltage_mv,
      s.remaining_capacity_ah,
      s.total_capacity_ah,
      s.loop_cycles,
      s.status_text,
      p.time         AS pos_time,
      p.lat,
      p.lon,
      p.direction,
      p.sats_total,
      t.time         AS temps_time,
      t.bms_temps_c,
      t.cell_temps_c,
      c.time         AS cells_time,
      c.cell_voltages_mv,
      n.time         AS net_time,
      n.rssi,
      n.rsrp,
      n.rsrq,
      n.snr,
      n.network_type
    FROM tmp_devices d
    LEFT JOIN tmp_info   i ON i.device_id = d.device_id
    LEFT JOIN tmp_status s ON s.device_id = d.device_id
    LEFT JOIN tmp_pos    p ON p.device_id = d.device_id
    LEFT JOIN tmp_temps  t ON t.device_id = d.device_id
    LEFT JOIN tmp_cells  c ON c.device_id = d.device_id
    LEFT JOIN tmp_net    n ON n.device_id = d.device_id;
  END $$$;
  $$;
END$$;

-- 1) List all devices discovered (from info_* tables)
SELECT SUBSTRING(table_name FROM 6) AS device_id
FROM information_schema.tables
WHERE table_schema = 'public' AND table_name LIKE 'info_%'
ORDER BY device_id;

-- 2) Row counts per table for one device
-- Replace <DEVICE_ID>
SELECT 'pos_'    || '<DEVICE_ID>' AS table, COUNT(*) AS rows FROM public.pos_<DEVICE_ID>
UNION ALL SELECT 'status_' || '<DEVICE_ID>', COUNT(*) FROM public.status_<DEVICE_ID>
UNION ALL SELECT 'temps_'  || '<DEVICE_ID>', COUNT(*) FROM public.temps_<DEVICE_ID>
UNION ALL SELECT 'cells_'  || '<DEVICE_ID>', COUNT(*) FROM public.cells_<DEVICE_ID>
UNION ALL SELECT 'net_'    || '<DEVICE_ID>', COUNT(*) FROM public.net_<DEVICE_ID>
ORDER BY 1;

-- 3) Latest position for one device (Harare timestamps)
-- Replace <DEVICE_ID>
SELECT time, lat, lon, direction, sats_total, sats_gps, sats_beidou, hemisphere
FROM public.pos_<DEVICE_ID>
ORDER BY time DESC
LIMIT 1;

-- 4) Latest status for one device
-- Replace <DEVICE_ID>
SELECT time, current_amps, current_type, soc_percent, total_voltage_mv, status_text
FROM public.status_<DEVICE_ID>
ORDER BY time DESC
LIMIT 1;

-- 5) Last 100 positions for one device
-- Replace <DEVICE_ID>
SELECT time, lat, lon, direction, sats_total
FROM public.pos_<DEVICE_ID>
ORDER BY time DESC
LIMIT 100;

-- 6) Positions for a device in a time range (inclusive)
-- Replace <DEVICE_ID> and timestamps (Harare time). Example: '2025-09-11 00:00:00+02'
SELECT time, lat, lon
FROM public.pos_<DEVICE_ID>
WHERE time BETWEEN TIMESTAMPTZ '2025-09-11 00:00:00+02' AND TIMESTAMPTZ '2025-09-12 00:00:00+02'
ORDER BY time;

-- 7) Temperatures timeline for one device (last 24 hours)
-- Replace <DEVICE_ID>
SELECT time, bms_temps_c, cell_temps_c
FROM public.temps_<DEVICE_ID>
WHERE time >= NOW() - INTERVAL '24 hours'
ORDER BY time;

-- 8) Cell voltages snapshot (latest)
-- Replace <DEVICE_ID>
SELECT time, cell_voltages_mv
FROM public.cells_<DEVICE_ID>
ORDER BY time DESC
LIMIT 1;

-- 9) Network signal timeline (last 24 hours)
-- Replace <DEVICE_ID>
SELECT time, rssi, rsrp, rsrq, snr, network_type
FROM public.net_<DEVICE_ID>
WHERE time >= NOW() - INTERVAL '24 hours'
ORDER BY time;

-- 10) Device static info (one row per device)
-- Replace <DEVICE_ID>
SELECT * FROM public.info_<DEVICE_ID> ORDER BY inserted_at DESC LIMIT 1;

-- 11) Show all position tables with their latest row time and coordinates
-- Uses a DO block to aggregate per-table latest rows dynamically
DO $$
DECLARE r RECORD;
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS tmp_latest_pos(
    device_id TEXT,
    time TIMESTAMPTZ,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION
  ) ON COMMIT DROP;
  TRUNCATE tmp_latest_pos;
  FOR r IN (
    SELECT table_name FROM information_schema.tables
    WHERE table_schema='public' AND table_name LIKE 'pos_%'
  ) LOOP
    EXECUTE format('INSERT INTO tmp_latest_pos
      SELECT %L, time, lat, lon FROM %I ORDER BY time DESC LIMIT 1;',
      substring(r.table_name from 5), r.table_name);
  END LOOP;
END$$;

SELECT * FROM tmp_latest_pos ORDER BY time DESC NULLS LAST;

-- 12) Show all devices' latest status
DO $$
DECLARE r RECORD;
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS tmp_latest_status(
    device_id TEXT,
    time TIMESTAMPTZ,
    current_amps DOUBLE PRECISION,
    soc_percent DOUBLE PRECISION,
    total_voltage_mv INTEGER,
    status_text TEXT
  ) ON COMMIT DROP;
  TRUNCATE tmp_latest_status;
  FOR r IN (
    SELECT table_name FROM information_schema.tables
    WHERE table_schema='public' AND table_name LIKE 'status_%'
  ) LOOP
    EXECUTE format('INSERT INTO tmp_latest_status
      SELECT %L, time, current_amps, soc_percent, total_voltage_mv, status_text
      FROM %I ORDER BY time DESC LIMIT 1;',
      substring(r.table_name from 8), r.table_name);
  END LOOP;
END$$;

SELECT * FROM tmp_latest_status ORDER BY status_time DESC NULLS LAST;
SELECT * FROM tmp_latest_status ORDER BY status_time DESC NULLS LAST;

-- 13) Find devices with no positions recorded yet
SELECT SUBSTRING(table_name FROM 6) AS device_id
FROM information_schema.tables t
WHERE t.table_schema='public' AND t.table_name LIKE 'info_%'
AND NOT EXISTS (
  SELECT 1 FROM information_schema.tables p
  WHERE p.table_schema='public' AND p.table_name = 'pos_' || SUBSTRING(t.table_name FROM 6)
);

-- 14) Latest full snapshot for ALL devices (one row per device)
-- Run the entire block (DO + final SELECT) together as one script
DO $$
DECLARE r RECORD;
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS tmp_devices(device_id TEXT) ON COMMIT DROP;
  TRUNCATE tmp_devices;
  INSERT INTO tmp_devices(device_id)
  SELECT SUBSTRING(table_name FROM 6)
  FROM information_schema.tables
  WHERE table_schema='public' AND table_name LIKE 'info_%';

  CREATE TEMP TABLE IF NOT EXISTS tmp_pos(device_id TEXT, time TIMESTAMPTZ, lat DOUBLE PRECISION, lon DOUBLE PRECISION, direction DOUBLE PRECISION, sats_total INT) ON COMMIT DROP;
  TRUNCATE tmp_pos;
  FOR r IN (
    SELECT 'pos_' || device_id AS tbl, device_id FROM tmp_devices
  ) LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = r.tbl) THEN
      EXECUTE format('INSERT INTO tmp_pos SELECT %L, time, lat, lon, direction, sats_total FROM %I ORDER BY time DESC LIMIT 1;', r.device_id, r.tbl);
    END IF;
  END LOOP;

  CREATE TEMP TABLE IF NOT EXISTS tmp_status(device_id TEXT, time TIMESTAMPTZ, current_amps DOUBLE PRECISION, current_type TEXT, soc_percent DOUBLE PRECISION, total_voltage_mv INT, remaining_capacity_ah DOUBLE PRECISION, total_capacity_ah DOUBLE PRECISION, loop_cycles INT, status_text TEXT) ON COMMIT DROP;
  TRUNCATE tmp_status;
  FOR r IN (
    SELECT 'status_' || device_id AS tbl, device_id FROM tmp_devices
  ) LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = r.tbl) THEN
      EXECUTE format('INSERT INTO tmp_status SELECT %L, time, current_amps, current_type, soc_percent, total_voltage_mv, remaining_capacity_ah, total_capacity_ah, loop_cycles, status_text FROM %I ORDER BY time DESC LIMIT 1;', r.device_id, r.tbl);
    END IF;
  END LOOP;

  CREATE TEMP TABLE IF NOT EXISTS tmp_temps(device_id TEXT, time TIMESTAMPTZ, bms_temps_c INT[], cell_temps_c INT[]) ON COMMIT DROP;
  TRUNCATE tmp_temps;
  FOR r IN (
    SELECT 'temps_' || device_id AS tbl, device_id FROM tmp_devices
  ) LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = r.tbl) THEN
      EXECUTE format('INSERT INTO tmp_temps SELECT %L, time, bms_temps_c, cell_temps_c FROM %I ORDER BY time DESC LIMIT 1;', r.device_id, r.tbl);
    END IF;
  END LOOP;

  CREATE TEMP TABLE IF NOT EXISTS tmp_cells(device_id TEXT, time TIMESTAMPTZ, cell_voltages_mv INT[]) ON COMMIT DROP;
  TRUNCATE tmp_cells;
  FOR r IN (
    SELECT 'cells_' || device_id AS tbl, device_id FROM tmp_devices
  ) LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = r.tbl) THEN
      EXECUTE format('INSERT INTO tmp_cells SELECT %L, time, cell_voltages_mv FROM %I ORDER BY time DESC LIMIT 1;', r.device_id, r.tbl);
    END IF;
  END LOOP;

  CREATE TEMP TABLE IF NOT EXISTS tmp_net(device_id TEXT, time TIMESTAMPTZ, rssi INT, rsrp INT, rsrq INT, snr INT, network_type TEXT) ON COMMIT DROP;
  TRUNCATE tmp_net;
  FOR r IN (
    SELECT 'net_' || device_id AS tbl, device_id FROM tmp_devices
  ) LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = r.tbl) THEN
      EXECUTE format('INSERT INTO tmp_net SELECT %L, time, rssi, rsrp, rsrq, snr, network_type FROM %I ORDER BY time DESC LIMIT 1;', r.device_id, r.tbl);
    END IF;
  END LOOP;

  CREATE TEMP TABLE IF NOT EXISTS tmp_info(device_id TEXT, inserted_at TIMESTAMPTZ, product_sn TEXT, gps_sn TEXT, gps_imsi TEXT, gps_imei TEXT, gps_sw TEXT, gps_hw TEXT, bms_sn TEXT, bms_sw TEXT, bms_hw TEXT) ON COMMIT DROP;
  TRUNCATE tmp_info;
  FOR r IN (
    SELECT 'info_' || device_id AS tbl, device_id FROM tmp_devices
  ) LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = r.tbl) THEN
      EXECUTE format('INSERT INTO tmp_info SELECT %L, inserted_at, product_sn, gps_sn, gps_imsi, gps_imei, gps_sw, gps_hw, bms_sn, bms_sw, bms_hw FROM %I ORDER BY inserted_at DESC LIMIT 1;', r.device_id, r.tbl);
    END IF;
  END LOOP;
END$$;

SELECT
  d.device_id,
  i.product_sn,
  i.gps_sn,
  i.gps_imsi,
  i.gps_imei,
  i.gps_sw,
  i.gps_hw,
  i.bms_sn,
  s.time         AS status_time,
  s.current_amps,
  s.current_type,
  s.soc_percent,
  s.total_voltage_mv,
  s.remaining_capacity_ah,
  s.total_capacity_ah,
  s.loop_cycles,
  s.status_text,
  p.time         AS pos_time,
  p.lat,
  p.lon,
  p.direction,
  p.sats_total,
  t.time         AS temps_time,
  t.bms_temps_c,
  t.cell_temps_c,
  c.time         AS cells_time,
  c.cell_voltages_mv,
  n.time         AS net_time,
  n.rssi,
  n.rsrp,
  n.rsrq,
  n.snr,
  n.network_type
FROM tmp_devices d
LEFT JOIN tmp_info   i ON i.device_id = d.device_id
LEFT JOIN tmp_status s ON s.device_id = d.device_id
LEFT JOIN tmp_pos    p ON p.device_id = d.device_id
LEFT JOIN tmp_temps  t ON t.device_id = d.device_id
LEFT JOIN tmp_cells  c ON c.device_id = d.device_id
LEFT JOIN tmp_net    n ON n.device_id = d.device_id
ORDER BY COALESCE(p.time, s.time, i.inserted_at) DESC NULLS LAST;



-- Run this file once in DBeaver on database: batteries (user: troy)
-- It creates two helper functions you can query later.

CREATE OR REPLACE FUNCTION public.latest_status_all()
RETURNS TABLE (
  device_id TEXT,
  status_time TIMESTAMPTZ,
  current_amps DOUBLE PRECISION,
  current_type TEXT,
  soc_percent DOUBLE PRECISION,
  total_voltage_mv INTEGER,
  remaining_capacity_ah DOUBLE PRECISION,
  total_capacity_ah DOUBLE PRECISION,
  loop_cycles INTEGER,
  status_text TEXT
)
LANGUAGE plpgsql AS $$
DECLARE r RECORD;
BEGIN
  CREATE TEMP TABLE tmp_latest_status(
    device_id TEXT,
    status_time TIMESTAMPTZ,
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
       SELECT %L, time AS status_time, current_amps, current_type, soc_percent, total_voltage_mv,
              remaining_capacity_ah, total_capacity_ah, loop_cycles, status_text
       FROM %I ORDER BY time DESC LIMIT 1',
      substring(r.table_name from 8), r.table_name
    );
  END LOOP;

  RETURN QUERY SELECT * FROM tmp_latest_status;
END $$;


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
LANGUAGE plpgsql AS $$
DECLARE r RECORD;
BEGIN
  CREATE TEMP TABLE tmp_devices(device_id TEXT) ON COMMIT DROP;
  INSERT INTO tmp_devices(device_id)
  SELECT SUBSTRING(table_name FROM 6)
  FROM information_schema.tables
  WHERE table_schema='public' AND table_name LIKE 'info_%';

  CREATE TEMP TABLE tmp_pos(device_id TEXT, time TIMESTAMPTZ, lat DOUBLE PRECISION, lon DOUBLE PRECISION, direction DOUBLE PRECISION, sats_total INT) ON COMMIT DROP;
  FOR r IN (
    SELECT 'pos_' || d.device_id AS tbl, d.device_id FROM tmp_devices d
  ) LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = r.tbl) THEN
      EXECUTE format('INSERT INTO tmp_pos SELECT %L, time, lat, lon, direction, sats_total FROM %I ORDER BY time DESC LIMIT 1;', r.device_id, r.tbl);
    END IF;
  END LOOP;

  CREATE TEMP TABLE tmp_status(device_id TEXT, time TIMESTAMPTZ, current_amps DOUBLE PRECISION, current_type TEXT, soc_percent DOUBLE PRECISION, total_voltage_mv INT, remaining_capacity_ah DOUBLE PRECISION, total_capacity_ah DOUBLE PRECISION, loop_cycles INT, status_text TEXT) ON COMMIT DROP;
  FOR r IN (
    SELECT 'status_' || d.device_id AS tbl, d.device_id FROM tmp_devices d
  ) LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = r.tbl) THEN
      EXECUTE format('INSERT INTO tmp_status SELECT %L, time, current_amps, current_type, soc_percent, total_voltage_mv, remaining_capacity_ah, total_capacity_ah, loop_cycles, status_text FROM %I ORDER BY time DESC LIMIT 1;', r.device_id, r.tbl);
    END IF;
  END LOOP;

  CREATE TEMP TABLE tmp_temps(device_id TEXT, time TIMESTAMPTZ, bms_temps_c INT[], cell_temps_c INT[]) ON COMMIT DROP;
  FOR r IN (
    SELECT 'temps_' || d.device_id AS tbl, d.device_id FROM tmp_devices d
  ) LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = r.tbl) THEN
      EXECUTE format('INSERT INTO tmp_temps SELECT %L, time, bms_temps_c, cell_temps_c FROM %I ORDER BY time DESC LIMIT 1;', r.device_id, r.tbl);
    END IF;
  END LOOP;

  CREATE TEMP TABLE tmp_cells(device_id TEXT, time TIMESTAMPTZ, cell_voltages_mv INT[]) ON COMMIT DROP;
  FOR r IN (
    SELECT 'cells_' || d.device_id AS tbl, d.device_id FROM tmp_devices d
  ) LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = r.tbl) THEN
      EXECUTE format('INSERT INTO tmp_cells SELECT %L, time, cell_voltages_mv FROM %I ORDER BY time DESC LIMIT 1;', r.device_id, r.tbl);
    END IF;
  END LOOP;

  CREATE TEMP TABLE tmp_net(device_id TEXT, time TIMESTAMPTZ, rssi INT, rsrp INT, rsrq INT, snr INT, network_type TEXT) ON COMMIT DROP;
  FOR r IN (
    SELECT 'net_' || d.device_id AS tbl, d.device_id FROM tmp_devices d
  ) LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name = r.tbl) THEN
      EXECUTE format('INSERT INTO tmp_net SELECT %L, time, rssi, rsrp, rsrq, snr, network_type FROM %I ORDER BY time DESC LIMIT 1;', r.device_id, r.tbl);
    END IF;
  END LOOP;

  CREATE TEMP TABLE tmp_info(device_id TEXT, inserted_at TIMESTAMPTZ, product_sn TEXT, gps_sn TEXT, gps_imsi TEXT, gps_imei TEXT, gps_sw TEXT, gps_hw TEXT, bms_sn TEXT, bms_sw TEXT, bms_hw TEXT) ON COMMIT DROP;
  FOR r IN (
    SELECT 'info_' || d.device_id AS tbl, d.device_id FROM tmp_devices d
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
END $$;



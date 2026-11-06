-- schema.sql - Esquema relacional para el Sistema IPS Hospitalario
-- Compatible con SQLite (Integrante 2 y 3)

CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    mac_address TEXT UNIQUE,
    patient_name TEXT,
    device_model TEXT,
    created_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL,
    status TEXT NOT NULL,
    connected_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    welcome_accepted INTEGER DEFAULT NULL,
    welcome_response_at TEXT,
    ip_address TEXT,
    measurements_count INTEGER DEFAULT 0,
    FOREIGN KEY(device_id) REFERENCES devices(device_id)
);

CREATE TABLE IF NOT EXISTS access_points (
    bssid TEXT PRIMARY KEY,
    ssid TEXT NOT NULL,
    piso TEXT NOT NULL,
    area TEXT NOT NULL,
    ip_address TEXT,
    channel INTEGER,
    status TEXT NOT NULL DEFAULT 'ONLINE',
    is_ingress_ap INTEGER DEFAULT 0,
    last_ping_at TEXT
);

CREATE TABLE IF NOT EXISTS access_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    device_id TEXT NOT NULL,
    session_id TEXT,
    ap_bssid TEXT NOT NULL,
    ap_ssid TEXT,
    piso TEXT,
    area TEXT,
    timestamp TEXT NOT NULL,
    details_json TEXT
);

CREATE TABLE IF NOT EXISTS location_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    piso TEXT NOT NULL,
    area TEXT NOT NULL,
    lugar_id TEXT NOT NULL,
    confidence REAL NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    coords_json TEXT
);

CREATE TABLE IF NOT EXISTS rssi_measurements_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL,
    session_id TEXT,
    bssid TEXT NOT NULL,
    ssid TEXT,
    rssi_dbm REAL NOT NULL,
    frecuencia_mhz INTEGER,
    timestamp_ms INTEGER NOT NULL,
    edad_s REAL
);

CREATE TABLE IF NOT EXISTS calibration_reference (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lugar_id TEXT NOT NULL,
    piso TEXT NOT NULL,
    area TEXT NOT NULL,
    bssid TEXT NOT NULL,
    ssid TEXT,
    rssi_dbm REAL NOT NULL,
    frecuencia_mhz INTEGER,
    fecha_hora TEXT
);

CREATE INDEX IF NOT EXISTS idx_sessions_device ON sessions(device_id);
CREATE INDEX IF NOT EXISTS idx_access_events_device ON access_events(device_id);
CREATE INDEX IF NOT EXISTS idx_access_events_time ON access_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_calib_lugar ON calibration_reference(lugar_id);
CREATE INDEX IF NOT EXISTS idx_calib_bssid ON calibration_reference(bssid);


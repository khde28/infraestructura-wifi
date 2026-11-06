# database/db_adapter.py - Adaptador de Base de Datos para el Servidor Central
import sqlite3
import json
import logging
import asyncio
from typing import List, Optional, Dict, Any
from pathlib import Path
from server.models import (
    DeviceSession, AccessEvent, AccessPoint, LocationResult,
    RSSIMeasurement, SessionStatus, AccessEventType, APStatus
)

logger = logging.getLogger("IPS.Database")

class DatabaseAdapter:
    """
    Gestiona el almacenamiento persistente para el Servidor Central.
    Totalmente preparado para interactuar con la entrega del Integrante 3.
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = asyncio.Lock()

    def init_schema(self, schema_file: Optional[str] = None):
        """Inicializa las tablas a partir del archivo schema.sql."""
        if not schema_file:
            schema_file = str(Path(__file__).parent / "schema.sql")

        with open(schema_file, "r", encoding="utf-8") as f:
            sql = f.read()

        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(sql)
            conn.commit()
        logger.info(f"Esquema de base de datos inicializado en {self.db_path}")

    # ==================== DISPOSITIVOS Y SESIONES ====================

    async def upsert_device(self, device_id: str, mac_address: Optional[str],
                            patient_name: Optional[str], device_model: Optional[str],
                            last_seen: str):
        def _sync():
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO devices (device_id, mac_address, patient_name, device_model, created_at, last_seen_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(device_id) DO UPDATE SET
                        mac_address = COALESCE(excluded.mac_address, devices.mac_address),
                        patient_name = COALESCE(excluded.patient_name, devices.patient_name),
                        device_model = COALESCE(excluded.device_model, devices.device_model),
                        last_seen_at = excluded.last_seen_at;
                """, (device_id, mac_address, patient_name, device_model, last_seen, last_seen))
                conn.commit()
        await asyncio.to_thread(_sync)

    async def save_session(self, session: DeviceSession):
        def _sync():
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                # Asegurar registro de dispositivo primero
                cur.execute("""
                    INSERT INTO devices (device_id, mac_address, patient_name, device_model, created_at, last_seen_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(device_id) DO UPDATE SET
                        mac_address = COALESCE(excluded.mac_address, devices.mac_address),
                        last_seen_at = excluded.last_seen_at;
                """, (session.device_id, session.mac_address, session.patient_name,
                      session.device_model, session.connected_at, session.last_seen_at))

                welcome_val = 1 if session.welcome_accepted is True else (0 if session.welcome_accepted is False else None)
                cur.execute("""
                    INSERT INTO sessions (session_id, device_id, status, connected_at, last_seen_at,
                                         welcome_accepted, welcome_response_at, ip_address, measurements_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_id) DO UPDATE SET
                        status = excluded.status,
                        last_seen_at = excluded.last_seen_at,
                        welcome_accepted = excluded.welcome_accepted,
                        welcome_response_at = excluded.welcome_response_at,
                        measurements_count = excluded.measurements_count;
                """, (session.session_id, session.device_id, session.status.value, session.connected_at,
                      session.last_seen_at, welcome_val, session.welcome_response_at,
                      session.ip_address, session.measurements_count))
                conn.commit()
        await asyncio.to_thread(_sync)

    async def update_session_welcome(self, session_id: str, accepted: bool, timestamp: str):
        def _sync():
            with sqlite3.connect(self.db_path) as conn:
                status = SessionStatus.ACTIVE.value if accepted else SessionStatus.OPTED_OUT.value
                conn.execute("""
                    UPDATE sessions
                    SET welcome_accepted = ?, welcome_response_at = ?, status = ?, last_seen_at = ?
                    WHERE session_id = ?;
                """, (1 if accepted else 0, timestamp, status, timestamp, session_id))
                conn.commit()
        await asyncio.to_thread(_sync)

    # ==================== PUNTOS DE ACCESO (AP / ROUTERS) ====================

    async def register_access_point(self, ap: AccessPoint):
        def _sync():
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO access_points (bssid, ssid, piso, area, ip_address, channel, status, is_ingress_ap, last_ping_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(bssid) DO UPDATE SET
                        ssid = excluded.ssid,
                        piso = excluded.piso,
                        area = excluded.area,
                        ip_address = COALESCE(excluded.ip_address, access_points.ip_address),
                        channel = COALESCE(excluded.channel, access_points.channel),
                        status = excluded.status,
                        is_ingress_ap = excluded.is_ingress_ap,
                        last_ping_at = excluded.last_ping_at;
                """, (ap.bssid.lower(), ap.ssid, ap.piso, ap.area, ap.ip_address,
                      ap.channel, ap.status.value, 1 if ap.is_ingress_ap else 0, ap.last_ping_at))
                conn.commit()
        await asyncio.to_thread(_sync)

    async def get_all_access_points(self) -> List[AccessPoint]:
        def _sync() -> List[AccessPoint]:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute("SELECT * FROM access_points ORDER BY piso, area;")
                rows = cur.fetchall()
                aps = []
                for r in rows:
                    aps.append(AccessPoint(
                        bssid=r["bssid"],
                        ssid=r["ssid"],
                        piso=r["piso"],
                        area=r["area"],
                        ip_address=r["ip_address"],
                        channel=r["channel"],
                        status=APStatus(r["status"]),
                        is_ingress_ap=bool(r["is_ingress_ap"]),
                        last_ping_at=r["last_ping_at"]
                    ))
                return aps
        return await asyncio.to_thread(_sync)

    # ==================== EVENTOS DE ACCESO (INGRESO / SALIDA) ====================

    async def record_access_event(self, event: AccessEvent):
        def _sync():
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO access_events (event_id, event_type, device_id, session_id,
                                              ap_bssid, ap_ssid, piso, area, timestamp, details_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (event.event_id, event.event_type.value, event.device_id, event.session_id,
                      event.ap_bssid, event.ap_ssid, event.piso, event.area,
                      event.timestamp, json.dumps(event.details or {})))
                conn.commit()
        await asyncio.to_thread(_sync)

    async def get_recent_access_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        def _sync():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute("""
                    SELECT * FROM access_events
                    ORDER BY timestamp DESC
                    LIMIT ?;
                """, (limit,))
                return [dict(r) for r in cur.fetchall()]
        return await asyncio.to_thread(_sync)

    # ==================== LOGS Y UBICACIÓN ====================

    async def save_location_history(self, loc: LocationResult):
        def _sync():
            with sqlite3.connect(self.db_path) as conn:
                coords_str = json.dumps(loc.coordinates) if loc.coordinates else None
                conn.execute("""
                    INSERT INTO location_history (device_id, session_id, piso, area, lugar_id,
                                                  confidence, timestamp_ms, coords_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """, (loc.device_id, loc.session_id, loc.piso, loc.area, loc.lugar_id,
                      loc.confidence, loc.timestamp_ms, coords_str))
                conn.commit()
        await asyncio.to_thread(_sync)

    async def save_rssi_batch_log(self, device_id: str, session_id: str, measurements: List[RSSIMeasurement]):
        def _sync():
            with sqlite3.connect(self.db_path) as conn:
                data = [
                    (device_id, session_id, m.bssid, m.ssid, m.rssi_dbm,
                     m.frecuencia_mhz, m.timestamp_ms, m.edad_s)
                    for m in measurements
                ]
                conn.executemany("""
                    INSERT INTO rssi_measurements_log (device_id, session_id, bssid, ssid, rssi_dbm,
                                                      frecuencia_mhz, timestamp_ms, edad_s)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """, data)
                conn.commit()
        await asyncio.to_thread(_sync)

    # ==================== CALIBRACIÓN FINGERPRINTING ====================

    async def load_calibration_from_txt(self, file_path: str) -> int:
        """Carga datos de mapeo Wi-Fi de los archivos txt para referencia de posicionamiento."""
        import csv
        def _sync() -> int:
            count = 0
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                reader = csv.DictReader(f)
                rows_to_insert = []
                for row in reader:
                    lugar = row.get("lugar", "Desconocido")
                    # Extraer piso y área del lugar, ej: "302_8" -> Piso 3, Sala 302
                    piso = f"Piso {lugar[0]}" if lugar and lugar[0].isdigit() else "Piso 1"
                    area = f"Zona {lugar.split('_')[0]}" if "_" in lugar else lugar

                    rows_to_insert.append((
                        lugar, piso, area,
                        row.get("bssid", "").lower().strip(),
                        row.get("ssid", "").strip(),
                        float(row.get("rssi_dbm", -100)),
                        int(row.get("frecuencia_mhz", 2412)),
                        row.get("fecha_hora", "")
                    ))
                
                with sqlite3.connect(self.db_path) as conn:
                    conn.executemany("""
                        INSERT INTO calibration_reference (lugar_id, piso, area, bssid, ssid, rssi_dbm, frecuencia_mhz, fecha_hora)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                    """, rows_to_insert)
                    conn.commit()
                    count = len(rows_to_insert)
            return count
        return await asyncio.to_thread(_sync)

    async def get_calibration_fingerprints(self) -> Dict[str, Dict[str, Any]]:
        """Retorna las huellas de referencia agrupadas por lugar_id."""
        def _sync():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute("""
                    SELECT lugar_id, piso, area, bssid, AVG(rssi_dbm) as avg_rssi
                    FROM calibration_reference
                    GROUP BY lugar_id, bssid;
                """)
                rows = cur.fetchall()
                places: Dict[str, Dict[str, Any]] = {}
                for r in rows:
                    lid = r["lugar_id"]
                    if lid not in places:
                        places[lid] = {
                            "lugar_id": lid,
                            "piso": r["piso"],
                            "area": r["area"],
                            "signals": {}
                        }
                    places[lid]["signals"][r["bssid"]] = r["avg_rssi"]
                return places
        return await asyncio.to_thread(_sync)


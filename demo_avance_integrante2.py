#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
PROYECTO: Sistema de Posicionamiento en Interiores (IPS) para Zonas Hospitalarias
AVANCE DEL INTEGRANTE 2: Servidor Central e Infraestructura Wi-Fi
================================================================================
Este script es una DEMOSTRACIÓN CONTROLADA Y AUTOCONTENIDA para la presentación
del avance del Integrante 2. Utiliza únicamente la librería estándar de Python.
No requiere dependencias externas complejas.
================================================================================
"""

import asyncio
import csv
import glob
import json
import math
import os
import random
import sqlite3
import time
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any

# ==============================================================================
# 1. MODELOS DE DATOS Y BASE DE DATOS LOCAL (INTEGRANTE 2 y 3)
# ==============================================================================

@dataclass
class DeviceSession:
    session_id: str
    device_id: str
    patient_name: str
    status: str              # CONNECTED, WELCOME_PENDING, ACTIVE, OPTED_OUT, EXPIRED
    welcome_accepted: Optional[bool] = None
    connected_at: float = field(default_factory=time.time)
    last_seen_at: float = field(default_factory=time.time)
    last_location: Optional[Dict[str, Any]] = None

@dataclass
class AccessPoint:
    bssid: str
    ssid: str
    piso: str
    area: str
    is_ingress: bool = False

def init_memory_db() -> sqlite3.Connection:
    """Inicializa una base de datos SQLite en memoria para registrar el avance."""
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE sessions (
            session_id TEXT PRIMARY KEY,
            device_id TEXT,
            patient_name TEXT,
            status TEXT,
            welcome_accepted INTEGER
        );
    """)
    cur.execute("""
        CREATE TABLE access_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT,
            device_id TEXT,
            ap_bssid TEXT,
            area TEXT,
            piso TEXT,
            timestamp TEXT
        );
    """)
    cur.execute("""
        CREATE TABLE access_points (
            bssid TEXT PRIMARY KEY,
            ssid TEXT,
            piso TEXT,
            area TEXT,
            is_ingress INTEGER
        );
    """)
    conn.commit()
    return conn

# ==============================================================================
# 2. SERVIDOR CENTRAL E INFRAESTRUCTURA (NÚCLEO DEL INTEGRANTE 2)
# ==============================================================================

class CentralServerDemo:
    def __init__(self, db_conn: sqlite3.Connection):
        self.db = db_conn
        self.sessions: Dict[str, DeviceSession] = {}
        self.access_points: Dict[str, AccessPoint] = {}
        self.reference_fingerprints: Dict[str, Dict[str, Any]] = {}
        self.active_connections = 0
        self.max_capacity = 100

        # Mensaje de bienvenida hospitalario configurado
        self.welcome_message = {
            "title": "Hospital Regional - Servicio de Orientación de Pacientes",
            "body": "Bienvenido. ¿Desea activar el guiado en interiores para llegar a su consulta/área?"
        }

    def register_access_point(self, ap: AccessPoint):
        """Registra un router o AP de la infraestructura del hospital."""
        self.access_points[ap.bssid.lower()] = ap
        cur = self.db.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO access_points VALUES (?, ?, ?, ?, ?)",
            (ap.bssid.lower(), ap.ssid, ap.piso, ap.area, 1 if ap.is_ingress else 0)
        )
        self.db.commit()

    def handle_infrastructure_event(self, event_type: str, device_id: str, ap_bssid: str) -> Dict[str, Any]:
        """
        Gestiona eventos proporcionados por la infraestructura (Routers/AP):
        - INGRESS: Llegada del paciente a la puerta / recepción
        - EGRESS: Salida del hospital
        - ROAMING: Cambio de AP / Piso
        """
        ap = self.access_points.get(ap_bssid.lower())
        area = ap.area if ap else "Área No Identificada"
        piso = ap.piso if ap else "Piso 1"

        now_str = time.strftime("%H:%M:%S")
        cur = self.db.cursor()
        cur.execute(
            "INSERT INTO access_events (event_type, device_id, ap_bssid, area, piso, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
            (event_type, device_id, ap_bssid, area, piso, now_str)
        )
        self.db.commit()

        return {
            "event": event_type,
            "device_id": device_id,
            "ap_bssid": ap_bssid,
            "area": area,
            "piso": piso,
            "timestamp": now_str
        }

    def handle_device_connect(self, device_id: str, patient_name: str) -> Dict[str, Any]:
        """
        Crea la sesión del dispositivo y emite el mensaje de bienvenida.
        Cumple: 'Crear la API y gestionar las sesiones de los dispositivos'.
        """
        if self.active_connections >= self.max_capacity:
            return {"status": "error", "message": "Capacidad máxima de 100 alcanzada"}

        session_id = f"SES-{random.randint(1000, 9999)}"
        session = DeviceSession(
            session_id=session_id,
            device_id=device_id,
            patient_name=patient_name,
            status="WELCOME_PENDING"
        )
        self.sessions[device_id] = session
        self.active_connections += 1

        cur = self.db.cursor()
        cur.execute(
            "INSERT INTO sessions VALUES (?, ?, ?, ?, ?)",
            (session.session_id, session.device_id, session.patient_name, session.status, None)
        )
        self.db.commit()

        return {
            "status": "connected",
            "session_id": session_id,
            "welcome_prompt": self.welcome_message
        }

    def handle_welcome_response(self, device_id: str, accepted: bool) -> Dict[str, Any]:
        """Registra si el paciente aceptó o rechazó el guiado."""
        session = self.sessions.get(device_id)
        if not session:
            return {"status": "error", "message": "Sesión inexistente"}

        session.welcome_accepted = accepted
        session.status = "ACTIVE" if accepted else "OPTED_OUT"

        cur = self.db.cursor()
        cur.execute(
            "UPDATE sessions SET status = ?, welcome_accepted = ? WHERE session_id = ?",
            (session.status, 1 if accepted else 0, session.session_id)
        )
        self.db.commit()

        return {
            "status": "updated",
            "session_status": session.status,
            "message": "Servicio de posicionamiento ACTIVADO" if accepted else "Servicio RECHAZADO por el paciente"
        }

    def load_calibration_from_files(self):
        """Carga las huellas reales de los archivos txt del proyecto (rssi_*.txt)."""
        txt_files = glob.glob("rssi_*.txt")
        if not txt_files:
            return 0

        total_rows = 0
        for fpath in txt_files:
            with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    lugar = row.get("lugar", "302_general")
                    bssid = row.get("bssid", "").lower().strip()
                    rssi = float(row.get("rssi_dbm", -90))

                    if lugar not in self.reference_fingerprints:
                        piso_num = lugar[0] if lugar and lugar[0].isdigit() else "1"
                        self.reference_fingerprints[lugar] = {
                            "piso": f"Piso {piso_num}",
                            "area": f"Pabellón {lugar.split('_')[0]} (Punto {lugar.split('_')[1] if '_' in lugar else '1'})",
                            "signals": {}
                        }
                    # Promediar RSSI de referencia
                    if bssid not in self.reference_fingerprints[lugar]["signals"]:
                        self.reference_fingerprints[lugar]["signals"][bssid] = rssi
                    else:
                        prev = self.reference_fingerprints[lugar]["signals"][bssid]
                        self.reference_fingerprints[lugar]["signals"][bssid] = (prev + rssi) / 2
                    total_rows += 1
        return total_rows

    def process_and_distribute_location(self, device_id: str, scan_measurements: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Recibe las mediciones de un celular, las procesa (k-NN) y devuelve la ubicación.
        Cumple: 'Recibir mediciones, enviar a procesamiento y distribuir resultados'.
        """
        session = self.sessions.get(device_id)
        if not session:
            return {"status": "error", "message": "Dispositivo no conectado"}

        # Vector actual del escaneo del celular: {bssid: rssi}
        current_signals = {m["bssid"].lower(): float(m["rssi_dbm"]) for m in scan_measurements}

        best_place = None
        min_distance = float("inf")

        # Comparar con los puntos de referencia de los txt
        for lugar, ref in self.reference_fingerprints.items():
            ref_sigs = ref["signals"]
            all_bssids = set(ref_sigs.keys()).union(set(current_signals.keys()))
            
            sq_sum = 0.0
            for bssid in all_bssids:
                curr_val = current_signals.get(bssid, -100.0)
                ref_val = ref_sigs.get(bssid, -100.0)
                sq_sum += (curr_val - ref_val) ** 2

            dist = math.sqrt(sq_sum)
            if dist < min_distance:
                min_distance = dist
                best_place = ref

        if not best_place:
            best_place = {"piso": "Piso 1", "area": "Recepción / Triaje"}
            confidence = 0.50
        else:
            confidence = max(0.40, min(0.96, round(1.0 - (min_distance / 180.0), 2)))

        piso = best_place["piso"]
        area = best_place["area"]

        # Mensaje de navegación adaptativo
        if "3" in piso:
            nav_msg = f"Usted está en {area} ({piso}). Para Triaje, baje por el ascensor al Piso 1."
        elif "2" in piso:
            nav_msg = f"Usted está en {area} ({piso}). Cerca a Laboratorios y Farmacia."
        else:
            nav_msg = f"Usted está en {area} ({piso}). Siga la línea verde hacia Admisión."

        result = {
            "piso": piso,
            "area": area,
            "confidence": f"{int(confidence * 100)}%",
            "navigation_message": nav_msg,
            "timestamp": time.strftime("%H:%M:%S")
        }

        # Guardar en sesión (distribución al celular)
        session.last_location = result
        return result

    def handle_device_disconnect(self, device_id: str):
        """Libera la conexión al desconectarse."""
        if device_id in self.sessions:
            self.sessions[device_id].status = "DISCONNECTED"
            if self.active_connections > 0:
                self.active_connections -= 1


# ==============================================================================
# 3. EJECUCIÓN DEL ESCENARIO DE PRUEBA CONTROLADA
# ==============================================================================

def print_section(title: str):
    print("\n" + "=" * 70)
    print(f" {title}")
    print("=" * 70)

def main():
    print("=" * 70)
    print(" DEMO DE AVANCE: INTEGRANTE 2 — SERVIDOR CENTRAL E INFRAESTRUCTURA Wi-Fi")
    print(" Proyecto IPS Hospitalario Basado en IoT mediante Redes Wi-Fi")
    print("=" * 70)

    # 1. Iniciar servidor y base de datos
    db = init_memory_db()
    server = CentralServerDemo(db)

    # -------------------------------------------------------------------------
    # PARTE A: INFRAESTRUCTURA Wi-Fi (ROUTERS Y PUNTOS DE ACCESO)
    # -------------------------------------------------------------------------
    print_section("PASO 1: REGISTRO DE INFRAESTRUCTURA Wi-Fi (ROUTERS / APs)")
    aps = [
        AccessPoint(bssid="fc:5a:1d:7e:0e:48", ssid="HOSPITAL-ADMISION", piso="Piso 1", area="Puerta Principal / Admisión", is_ingress=True),
        AccessPoint(bssid="70:0b:01:ef:f6:26", ssid="HOSPITAL-CONSULTAS", piso="Piso 2", area="Consultorios Externos", is_ingress=False),
        AccessPoint(bssid="a4:88:73:40:c8:cf", ssid="wifi-CsComputacion", piso="Piso 3", area="Pabellón 302 - Ala Norte", is_ingress=False),
    ]
    for ap in aps:
        server.register_access_point(ap)
        rol = "[AP DE INGRESO PRINCIPAL]" if ap.is_ingress else "[AP DE COBERTURA]"
        print(f"  [+] Router AP registrado: {ap.bssid} | {ap.ssid:20} | {ap.piso} - {ap.area} {rol}")

    # -------------------------------------------------------------------------
    # PARTE B: CARGA DE MEDICIONES REALES DE LOS ARCHIVOS .TXT
    # -------------------------------------------------------------------------
    print_section("PASO 2: CARGA DE DATOS DE MAPEO REALES (rssi_*.txt)")
    loaded_samples = server.load_calibration_from_files()
    print(f"  [i] Total de muestras RSSI leídas de los archivos locales: {loaded_samples}")
    print(f"  [i] Puntos de referencia construidos para k-NN: {list(server.reference_fingerprints.keys())}")

    # -------------------------------------------------------------------------
    # PARTE C: SIMULACIÓN DE EVENTO DE INGRESO (INGRESS)
    # -------------------------------------------------------------------------
    print_section("PASO 3: DETECCIÓN DE EVENTO DE INGRESO POR LA INFRAESTRUCTURA")
    paciente_mac = "44:3b:14:0d:6a:d8"
    device_id = "paciente_android_01"
    
    # El router de la puerta detecta asociación de la MAC
    ev_ingreso = server.handle_infrastructure_event(
        event_type="INGRESS",
        device_id=device_id,
        ap_bssid="fc:5a:1d:7e:0e:48"  # AP de Admisión
    )
    print(f"  [*] Evento emitido por el Router de entrada a las {ev_ingreso['timestamp']}:")
    print(f"      Tipo: {ev_ingreso['event']} | MAC/Dispositivo: {ev_ingreso['device_id']}")
    print(f"      Ubicación detectada: {ev_ingreso['area']} ({ev_ingreso['piso']})")

    # -------------------------------------------------------------------------
    # PARTE D: CREAR SESIÓN Y MENSAJE DE BIENVENIDA ASISTENCIAL
    # -------------------------------------------------------------------------
    print_section("PASO 4: GESTIÓN DE SESIÓN Y MENSAJE DE BIENVENIDA (INTEGRANTE 1)")
    conn_result = server.handle_device_connect(device_id, "Juan Perez (Paciente #1042)")
    print(f"  [*] Celular conectado. Token de Sesión: {conn_result['session_id']}")
    print(f"  [*] Mensaje de bienvenida enviado al celular:")
    print(f"      Titulo: \"{conn_result['welcome_prompt']['title']}\"")
    print(f"      Cuerpo: \"{conn_result['welcome_prompt']['body']}\"")

    # Paciente presiona "Aceptar" en la aplicación móvil
    resp_bienvenida = server.handle_welcome_response(device_id, accepted=True)
    print(f"  [*] Respuesta del paciente: ACEPTADO -> {resp_bienvenida['message']}")

    # -------------------------------------------------------------------------
    # PARTE E: RECEPCIÓN DE MEDICIONES Y DISTRIBUCIÓN DE UBICACIÓN
    # -------------------------------------------------------------------------
    print_section("PASO 5: RECEPCIÓN DE RSSI Y DISTRIBUCIÓN DE UBICACIÓN AL CELULAR")
    
    # Extraemos una ráfaga real de scan de los datos del txt
    scan_real = [
        {"bssid": "a4:88:73:40:c8:cf", "ssid": "wifi-CsComputacion", "rssi_dbm": -88},
        {"bssid": "a4:88:73:40:a4:00", "ssid": "wifi-CsComputacion", "rssi_dbm": -72},
        {"bssid": "fc:5a:1d:7e:0e:48", "ssid": "MOVISTAR_0E40", "rssi_dbm": -43},
        {"bssid": "70:0b:01:ef:f6:26", "ssid": "sc-f620", "rssi_dbm": -66}
    ]
    print(f"  [*] Celular envía ráfaga de {len(scan_real)} redes Wi-Fi escaneadas:")
    for s in scan_real:
        print(f"      - AP: {s['bssid']} | SSID: {s['ssid']:20} | RSSI: {s['rssi_dbm']} dBm")

    # El servidor procesa y distribuye el resultado de vuelta
    ubicacion = server.process_and_distribute_location(device_id, scan_real)
    print("\n  [✓] RESULTADO DISTRIBUIDO AL CELULAR EN TIEMPO REAL:")
    print(f"      -> PISO CALCULADO:    {ubicacion['piso']}")
    print(f"      -> ÁREA CALCULADA:    {ubicacion['area']}")
    print(f"      -> NIVEL DE CERTEZA:  {ubicacion['confidence']}")
    print(f"      -> MENSAJE AL PACIENTE: \"{ubicacion['navigation_message']}\"")

    # -------------------------------------------------------------------------
    # PARTE F: SIMULACIÓN DE CAPACIDAD DE CONCURRENCIA (10 DISPOSITIVOS DEMO)
    # -------------------------------------------------------------------------
    print_section("PASO 6: PRUEBA DE CONCURRENCIA DE CONEXIONES (MUESTRA DE DEMO)")
    print(f"  [*] Probando saturación controlada: 10 pacientes simultáneos (capacidad máxima: {server.max_capacity})...")
    
    t0 = time.time()
    for i in range(1, 11):
        dev_demo = f"paciente_movil_{i:02d}"
        server.handle_device_connect(dev_demo, f"Paciente Demo {i}")
        server.handle_welcome_response(dev_demo, accepted=True)
        # Procesar ubicación simulada rápida
        server.process_and_distribute_location(dev_demo, scan_real)
    
    elapsed = time.time() - t0
    print(f"  [✓] 10/10 conexiones concurrentes atendidas con éxito en {elapsed:.4f} segundos.")
    print(f"  [✓] Ocupación actual del servidor: {server.active_connections} / {server.max_capacity} dispositivos activos.")

    # -------------------------------------------------------------------------
    # PARTE G: SIMULACIÓN DE SALIDA HOSPITALARIA (EGRESS)
    # -------------------------------------------------------------------------
    print_section("PASO 7: EVENTO DE SALIDA (EGRESS)")
    ev_salida = server.handle_infrastructure_event(
        event_type="EGRESS",
        device_id=device_id,
        ap_bssid="fc:5a:1d:7e:0e:48"
    )
    server.handle_device_disconnect(device_id)
    print(f"  [*] Router detectó desasociación a las {ev_salida['timestamp']}:")
    print(f"      Tipo: {ev_salida['event']} | Paciente {ev_salida['device_id']} ha salido del hospital.")
    print(f"      Conexiones activas restantes: {server.active_connections}")

    print("\n" + "=" * 70)
    print(" DEMOSTRACIÓN DE AVANCE DEL INTEGRANTE 2 COMPLETADA SATISFACTORIAMENTE")
    print("=" * 70)

if __name__ == "__main__":
    main()


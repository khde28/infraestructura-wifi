# scripts/simulate_devices.py - Simulador de hasta 100 celulares concurrentes
import asyncio
import aiohttp
import json
import csv
import glob
import random
import time
import argparse
from typing import List, Dict, Any

def load_rssi_samples_from_txt() -> List[List[Dict[str, Any]]]:
    """Carga y agrupa las mediciones de los archivos TXT reales en ráfagas de escaneo."""
    txt_files = glob.glob("rssi_*.txt")
    if not txt_files:
        txt_files = glob.glob("../rssi_*.txt")

    batches = []
    current_batch = []
    last_timestamp = None

    for fpath in txt_files:
        with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ts = row.get("timestamp_ms")
                if last_timestamp and ts != last_timestamp:
                    if current_batch:
                        batches.append(current_batch)
                        current_batch = []
                last_timestamp = ts
                current_batch.append({
                    "bssid": row.get("bssid", "").lower().strip(),
                    "ssid": row.get("ssid", "").strip(),
                    "rssi_dbm": float(row.get("rssi_dbm", -80)),
                    "frecuencia_mhz": int(row.get("frecuencia_mhz", 2412)),
                    "timestamp_ms": int(ts) if ts and ts.isdigit() else int(time.time() * 1000),
                    "edad_s": float(row.get("edad_s", 0.3)),
                    "fuente": "scan"
                })
            if current_batch:
                batches.append(current_batch)
                current_batch = []

    print(f"[Simulador] Cargadas {len(batches)} ráfagas de escaneo Wi-Fi reales desde archivos TXT.")
    return batches

async def simulate_single_device(device_idx: int, server_url: str,
                                 rssi_batches: List[List[Dict[str, Any]]],
                                 iterations: int = 5, delay_between_scans: float = 3.0):
    device_id = f"android_patient_{device_idx:03d}"
    mac_addr = f"cc:22:3d:4e:{device_idx // 100:02x}:{device_idx % 100:02x}"
    patient_name = f"Paciente #{1000 + device_idx}"
    model = random.choice(["Samsung Galaxy A54", "Redmi Note 14", "Motorola Edge 40", "Pixel 8A"])

    ws_url = f"{server_url}/ws/device"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(ws_url, timeout=10) as ws:
                # 1. Enviar mensaje de conexión / registro
                await ws.send_json({
                    "action": "connect",
                    "device_id": device_id,
                    "mac_address": mac_addr,
                    "patient_name": patient_name,
                    "device_model": model
                })

                # 2. Esperar confirmación de conexión y mensaje de bienvenida
                msg = await ws.receive_json(timeout=5)
                if msg.get("type") == "connected":
                    # print(f"[{device_id}] Conectado. Sesión: {msg.get('session_id')[:8]}")
                    pass

                # 3. Responder al mensaje de bienvenida (el 95% de los pacientes acepta)
                accepted = (random.random() > 0.05)
                await ws.send_json({
                    "action": "welcome_response",
                    "accepted": accepted
                })
                # Recibir confirmación de bienvenida
                _ = await ws.receive_json(timeout=5)

                # 4. Enviar ráfagas de telemetría periódicas y recibir ubicación calculada
                for i in range(iterations):
                    sample_batch = random.choice(rssi_batches)
                    # Añadir ligero jitter aleatorio para simular variación real en el hospital
                    jittered_batch = []
                    for m in sample_batch:
                        m_copy = dict(m)
                        m_copy["rssi_dbm"] = round(m["rssi_dbm"] + random.uniform(-2.0, 2.0), 1)
                        m_copy["timestamp_ms"] = int(time.time() * 1000)
                        jittered_batch.append(m_copy)

                    await ws.send_json({
                        "action": "rssi_batch",
                        "measurements": jittered_batch,
                        "client_timestamp_ms": int(time.time() * 1000)
                    })

                    # Esperar la distribución de ubicación devuelta por el servidor
                    loc_msg = await ws.receive_json(timeout=5)
                    if loc_msg.get("type") == "location_update":
                        if device_idx % 20 == 1 or i == 0:
                            print(f"[{device_id}] Ubicación calculada: {loc_msg.get('piso')} | {loc_msg.get('area')} | Confianza: {int(loc_msg.get('confidence', 0)*100)}%")

                    await asyncio.sleep(delay_between_scans + random.uniform(-0.5, 0.5))

    except Exception as e:
        print(f"[{device_id}] Error: {e}")

async def main():
    parser = argparse.ArgumentParser(description="Simulador de dispositivos para el Integrante 2")
    parser.add_argument("--devices", type=int, default=100, help="Cantidad de dispositivos concurrentes (ej. 100)")
    parser.add_argument("--iterations", type=int, default=3, help="Escaneos enviados por cada dispositivo")
    parser.add_argument("--url", type=str, default="http://localhost:8080", help="URL base del servidor")
    args = parser.parse_args()

    print(f"============================================================")
    print(f"Iniciando simulación de concurrencia: {args.devices} dispositivos Android")
    print(f"Servidor destino: {args.url}")
    print(f"============================================================")

    batches = load_rssi_samples_from_txt()
    if not batches:
        print("ERROR: No se encontraron archivos rssi_*.txt con datos de calibración.")
        return

    t0 = time.time()
    # Conexión escalonada de los dispositivos
    tasks = []
    for i in range(1, args.devices + 1):
        tasks.append(simulate_single_device(i, args.url, batches, iterations=args.iterations))
        if i % 25 == 0:
            await asyncio.sleep(0.2)  # Pequeño escalonamiento de ráfaga

    await asyncio.gather(*tasks)
    total_time = round(time.time() - t0, 2)
    print(f"\n============================================================")
    print(f"Simulación de {args.devices} dispositivos completada con éxito en {total_time} segundos.")
    print(f"============================================================")

if __name__ == "__main__":
    asyncio.run(main())


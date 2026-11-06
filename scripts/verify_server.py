# scripts/verify_server.py - Verificación automatizada de requerimientos del Integrante 2
import asyncio
import aiohttp
import time
import sys

BASE_URL = "http://127.0.0.1:8080"

async def test_requirement_1_sessions(session: aiohttp.ClientSession):
    """Req 1: Crear la API y gestionar las sesiones de los dispositivos."""
    print("\n[TEST 1] Verificando creación de API y gestión de sesiones...")
    url = f"{BASE_URL}/api/v1/devices/register"
    payload = {
        "device_id": "test_device_001",
        "mac_address": "aa:bb:cc:dd:ee:01",
        "patient_name": "Carlos Mendoza",
        "device_model": "Samsung S23"
    }
    async with session.post(url, json=payload) as resp:
        assert resp.status == 200, f"Error en registro: {resp.status}"
        data = await resp.json()
        assert data["status"] == "success"
        session_id = data["session"]["session_id"]
        assert session_id, "No se generó session_id"
        assert "welcome_prompt" in data, "No se incluyó mensaje de bienvenida"
        print(f"  ✓ Dispositivo registrado con éxito. ID Sesión: {session_id}")

        # Probar respuesta al mensaje de bienvenida
        resp_url = f"{BASE_URL}/api/v1/sessions/{session_id}/welcome"
        async with session.post(resp_url, json={"accepted": True}) as r_welc:
            assert r_welc.status == 200
            d_welc = await r_welc.json()
            assert d_welc["welcome_accepted"] is True
            assert d_welc["session_status"] == "ACTIVE"
            print("  ✓ Respuesta de bienvenida recibida y estado de sesión actualizado a ACTIVE.")

    # Probar heartbeat
    hb_url = f"{BASE_URL}/api/v1/sessions/heartbeat"
    async with session.post(hb_url, json={"device_id": "test_device_001"}) as resp_hb:
        assert resp_hb.status == 200
        print("  ✓ Heartbeat de sesión procesado correctamente.")
    return session_id

async def test_requirement_2_ap_communication(session: aiohttp.ClientSession):
    """Req 2: Desarrollar la comunicación con los routers o puntos de acceso disponibles."""
    print("\n[TEST 2] Verificando comunicación con routers / APs disponibles...")
    url = f"{BASE_URL}/api/v1/infrastructure/aps"
    async with session.get(url) as resp:
        assert resp.status == 200
        data = await resp.json()
        total_aps = data["total"]
        assert total_aps > 0, "No se encontraron APs registrados"
        print(f"  ✓ Catálogo de infraestructura consultado: {total_aps} Routers/APs detectados.")

    # Registrar un nuevo AP en el hospital
    new_ap = {
        "bssid": "00:11:22:33:44:55",
        "ssid": "HOSPITAL-LABORATORIO",
        "piso": "Piso 2",
        "area": "Laboratorio de Análisis Clínicos",
        "ip_address": "192.168.10.25",
        "channel": 36,
        "is_ingress_ap": False
    }
    async with session.post(url, json=new_ap) as resp:
        assert resp.status == 200
        data = await resp.json()
        assert data["access_point"]["bssid"] == "00:11:22:33:44:55"
        print("  ✓ Nuevo Router/AP hospitalario registrado y enlazado con la base de datos.")

async def test_requirement_3_access_events(session: aiohttp.ClientSession):
    """Req 3: Gestionar eventos de ingreso y salida cuando la infraestructura los proporcione."""
    print("\n[TEST 3] Verificando gestión de eventos de ingreso y salida de infraestructura...")
    url = f"{BASE_URL}/api/v1/infrastructure/events"
    
    # Evento 1: Ingreso por puerta principal
    ingress_payload = {
        "event_type": "associate",
        "mac_address": "aa:bb:cc:dd:ee:01",
        "ap_bssid": "fc:5a:1d:7e:0e:48",
        "piso": "Piso 1",
        "area": "Puerta Principal / Admisión"
    }
    async with session.post(url, json=ingress_payload) as resp:
        assert resp.status == 200
        data = await resp.json()
        assert data["event"]["event_type"] == "INGRESS"
        print(f"  ✓ Evento de INGRESO procesado. Event ID: {data['event']['event_id']}")

    # Evento 2: Salida del hospital
    egress_payload = {
        "event_type": "disassociate",
        "mac_address": "aa:bb:cc:dd:ee:01",
        "ap_bssid": "fc:5a:1d:7e:0e:48"
    }
    async with session.post(url, json=egress_payload) as resp:
        assert resp.status == 200
        data = await resp.json()
        assert data["event"]["event_type"] == "EGRESS"
        print(f"  ✓ Evento de SALIDA procesado. Event ID: {data['event']['event_id']}")

async def test_requirement_4_and_5_telemetry_and_distribution(session: aiohttp.ClientSession, session_id: str):
    """
    Req 4: Recibir las mediciones de múltiples celulares y enviarlas al módulo de procesamiento.
    Req 5: Distribuir los resultados de ubicación y los mensajes a cada dispositivo.
    """
    print("\n[TEST 4 & 5] Verificando recepción de RSSI, procesamiento y distribución de ubicación...")
    url = f"{BASE_URL}/api/v1/telemetry/rssi"
    # Lote de escaneo real basado en los datos txt proporcionados
    sample_scan = {
        "device_id": "test_device_001",
        "session_id": session_id,
        "measurements": [
            {"bssid": "a4:88:73:40:c8:cf", "ssid": "wifi-CsComputacion", "rssi_dbm": -88, "frecuencia_mhz": 5320, "timestamp_ms": int(time.time()*1000)},
            {"bssid": "a4:88:73:40:a4:00", "ssid": "wifi-CsComputacion", "rssi_dbm": -72, "frecuencia_mhz": 2412, "timestamp_ms": int(time.time()*1000)},
            {"bssid": "fc:5a:1d:7e:0e:48", "ssid": "MOVISTAR_0E40", "rssi_dbm": -43, "frecuencia_mhz": 2412, "timestamp_ms": int(time.time()*1000)},
            {"bssid": "70:0b:01:ef:f6:26", "ssid": "sc-f620", "rssi_dbm": -66, "frecuencia_mhz": 2437, "timestamp_ms": int(time.time()*1000)}
        ]
    }
    async with session.post(url, json=sample_scan) as resp:
        assert resp.status == 200
        data = await resp.json()
        loc = data["location"]
        assert "piso" in loc and loc["piso"], "No se calculó el piso"
        assert "area" in loc and loc["area"], "No se calculó el área"
        assert "confidence" in loc and loc["confidence"] > 0, "No se calculó confianza"
        assert "navigation_message" in loc, "No se generó mensaje de navegación"
        print(f"  ✓ Estimación devuelta al celular:")
        print(f"    - Piso calculado: {loc['piso']}")
        print(f"    - Área calculada: {loc['area']}")
        print(f"    - Confianza: {round(loc['confidence']*100, 1)}%")
        print(f"    - Mensaje distribuido: '{loc['navigation_message']}'")

async def test_requirement_6_concurrency_100_devices():
    """Req 6: Diseñar la gestión de conexiones para aproximadamente 100 dispositivos."""
    print("\n[TEST 6] Verificando gestión de conexiones para 100 dispositivos concurrentes...")
    devices_count = 100
    ws_url = f"{BASE_URL}/ws/device"

    async def connect_and_ping(dev_id: int):
        did = f"stress_dev_{dev_id:03d}"
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(ws_url, timeout=5) as ws:
                    await ws.send_json({"action": "connect", "device_id": did})
                    resp = await ws.receive_json(timeout=4)
                    assert resp.get("type") == "connected"
                    await ws.send_json({"action": "welcome_response", "accepted": True})
                    _ = await ws.receive_json(timeout=4)
                    await ws.send_json({"action": "ping", "timestamp": time.time()})
                    pong = await ws.receive_json(timeout=4)
                    assert pong.get("type") == "pong"
                    return True
        except Exception as e:
            return False

    t0 = time.time()
    results = await asyncio.gather(*[connect_and_ping(i) for i in range(1, devices_count + 1)])
    elapsed = time.time() - t0
    success_count = sum(1 for r in results if r is True)
    print(f"  ✓ {success_count}/{devices_count} dispositivos conectados y atendidos concurrentemente.")
    print(f"  ✓ Tiempo total de atención concurrente: {round(elapsed, 3)}s ({round(devices_count/elapsed, 1)} req/seg)")
    assert success_count >= 95, f"Demasiados fallos en prueba de concurrencia: {success_count}/{devices_count}"

async def main():
    print("=====================================================================")
    print("  SUITE DE VERIFICACIÓN AUTOMATIZADA: INTEGRANTE 2 (SERVIDOR CENTRAL)")
    print("=====================================================================")

    async with aiohttp.ClientSession() as session:
        try:
            # Comprobar salud del servidor
            async with session.get(f"{BASE_URL}/api/v1/dashboard/summary") as r:
                if r.status != 200:
                    print(f"El servidor no responde en {BASE_URL}. ¿Está encendido?")
                    sys.exit(1)
        except Exception as e:
            print(f"Error conectando al servidor en {BASE_URL}: {e}")
            print("Asegúrate de ejecutar 'python -m server.app' antes de correr este script.")
            sys.exit(1)

        # Ejecutar verificación de requerimientos
        session_id = await test_requirement_1_sessions(session)
        await test_requirement_2_ap_communication(session)
        await test_requirement_3_access_events(session)
        await test_requirement_4_and_5_telemetry_and_distribution(session, session_id)
        await test_requirement_6_concurrency_100_devices()

    print("\n=====================================================================")
    print("  RESULTADO: TODOS LOS REQUERIMIENTOS DEL INTEGRANTE 2 SUPERADOS (PASS) ✓")
    print("=====================================================================")

if __name__ == "__main__":
    asyncio.run(main())


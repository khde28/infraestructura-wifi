# scripts/simulate_ap_events.py - Simulador de Eventos de Routers / APs de Infraestructura
import socket
import asyncio
import aiohttp
import time
import argparse

# Eventos de ejemplo de Routers hostapd / OpenWrt / Mikrotik
SYSLOG_TEMPLATES = [
    "<134>Oct 23 11:26:08 AP-Admision-P1 hostapd: wlan0: STA {mac} IEEE 802.11: associated",
    "<134>Oct 23 11:28:15 AP-Consultorios-P2 hostapd: wlan0: STA {mac} IEEE 802.11: associated",
    "<134>Oct 23 11:32:40 AP-Pabellon302-P3 hostapd: wlan0: STA {mac} IEEE 802.11: associated",
    "<134>Oct 23 11:45:00 AP-Admision-P1 hostapd: wlan0: STA {mac} IEEE 802.11: disassociated"
]

def send_udp_syslog(message: str, host: str = "127.0.0.1", port: int = 1514):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(message.encode("utf-8"), (host, port))
    sock.close()

async def send_http_ap_event(session: aiohttp.ClientSession, base_url: str,
                             event_type: str, mac: str, ap_bssid: str, piso: str, area: str):
    url = f"{base_url}/api/v1/infrastructure/events"
    payload = {
        "event_type": event_type,
        "mac_address": mac,
        "ap_bssid": ap_bssid,
        "piso": piso,
        "area": area,
        "details": {
            "router_model": "Cisco Catalyst 9120 / OpenWrt 23.05",
            "rssi_at_ap_dbm": -55
        }
    }
    async with session.post(url, json=payload) as resp:
        res = await resp.json()
        print(f"[HTTP AP Event] {event_type.upper()} enviado para {mac} en {area} ({piso}) -> Status: {resp.status}")

async def main():
    parser = argparse.ArgumentParser(description="Simula eventos de infraestructura Wi-Fi (Routers/APs)")
    parser.add_argument("--mode", choices=["http", "syslog", "both"], default="both", help="Protocolo a usar")
    parser.add_argument("--url", default="http://localhost:8080", help="URL base del servidor")
    parser.add_argument("--syslog-port", type=int, default=1514, help="Puerto Syslog UDP")
    args = parser.parse_args()

    test_macs = [
        "a4:88:73:40:c8:cf",
        "18:48:59:5a:4d:c1",
        "fc:5a:1d:7e:0e:48",
        "98:97:d1:93:e6:52"
    ]

    print("============================================================")
    print("Iniciando simulación de eventos de Infraestructura Wi-Fi")
    print(f"Modo: {args.mode}")
    print("============================================================")

    async with aiohttp.ClientSession() as session:
        for idx, mac in enumerate(test_macs):
            # 1. Ingreso por Admisión (Piso 1)
            print(f"\n--- Simulación Dispositivo {mac} ---")
            if args.mode in ("syslog", "both"):
                syslog_msg = f"<134>Oct 23 11:26:08 AP-Admision hostapd: wlan0: STA {mac} IEEE 802.11: associated"
                send_udp_syslog(syslog_msg, port=args.syslog_port)
                print(f"[Syslog UDP] Enviado evento de asociación para {mac}")

            if args.mode in ("http", "both"):
                await send_http_ap_event(session, args.url, "associate", mac, "fc:5a:1d:7e:0e:48", "Piso 1", "Puerta Principal / Admisión")

            await asyncio.sleep(1.0)

            # 2. Roaming a Pabellón 302 (Piso 3)
            if args.mode in ("http", "both"):
                await send_http_ap_event(session, args.url, "associate", mac, "a4:88:73:40:c8:cf", "Piso 3", "Pabellón 302 - Ala Norte")

            await asyncio.sleep(1.0)

            # 3. Salida (disassociate)
            if idx == 0:
                if args.mode in ("syslog", "both"):
                    syslog_msg = f"<134>Oct 23 11:35:00 AP-Piso3 hostapd: wlan0: STA {mac} IEEE 802.11: disassociated"
                    send_udp_syslog(syslog_msg, port=args.syslog_port)
                    print(f"[Syslog UDP] Enviado evento de desasociación para {mac}")
                if args.mode in ("http", "both"):
                    await send_http_ap_event(session, args.url, "disassociate", mac, "a4:88:73:40:c8:cf", "Piso 3", "Pabellón 302")

    print("\nSimulación de eventos de infraestructura completada exitosamente.")

if __name__ == "__main__":
    asyncio.run(main())


# infrastructure/syslog_listener.py - Servidor UDP de Syslog para Routers/APs
import asyncio
import logging
import re
from typing import Optional
from server.infrastructure.access_events import AccessEventsManager
from server.infrastructure.ap_manager import APManager

logger = logging.getLogger("IPS.SyslogListener")

# Expresiones regulares para eventos comunes de routers (hostapd, OpenWrt, Mikrotik, Cisco)
REGEX_ASSOCIATED = re.compile(
    r"(?:STA|station|client)?\s*([0-9a-fA-F]{2}(?::[0-9a-fA-F]{2}){5})\s*(?:IEEE 802.11:)?\s*(?:associated|authenticated|joined|authorized)",
    re.IGNORECASE
)
REGEX_DISASSOCIATED = re.compile(
    r"(?:STA|station|client)?\s*([0-9a-fA-F]{2}(?::[0-9a-fA-F]{2}){5})\s*(?:IEEE 802.11:)?\s*(?:disassociated|deauthenticated|left|disconnected)",
    re.IGNORECASE
)

class SyslogProtocol(asyncio.DatagramProtocol):
    """Protocolo UDP asíncrono para recibir tramas Syslog de los Routers Wi-Fi."""
    def __init__(self, access_events: AccessEventsManager, ap_manager: APManager):
        self.access_events = access_events
        self.ap_manager = ap_manager
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.DatagramTransport):
        self.transport = transport
        logger.info("SyslogProtocol conectado y escuchando tramas UDP de routers.")

    def datagram_received(self, data: bytes, addr):
        try:
            line = data.decode("utf-8", errors="ignore").strip()
            asyncio.create_task(self._process_syslog_line(line, addr[0]))
        except Exception as e:
            logger.error(f"Error decodificando datagrama de {addr}: {e}")

    async def _process_syslog_line(self, line: str, source_ip: str):
        # 1. Verificar asociación (Ingreso/Roaming)
        match_assoc = REGEX_ASSOCIATED.search(line)
        if match_assoc:
            client_mac = match_assoc.group(1).lower()
            logger.info(f"[Syslog] Evento ASOCIACIÓN detectado de {source_ip}: MAC={client_mac}")
            
            # Buscar AP por IP de origen o BSSID
            ap = next((a for a in self.ap_manager.get_all_aps() if a.ip_address == source_ip), None)
            ap_bssid = ap.bssid if ap else f"ap_{source_ip}"
            piso = ap.piso if ap else "Piso Desconocido"
            area = ap.area if ap else "Área General"

            await self.access_events.handle_infrastructure_event(
                event_type="associate",
                mac_address=client_mac,
                ap_bssid=ap_bssid,
                ap_piso=piso,
                ap_area=area,
                details={"syslog_raw": line, "router_ip": source_ip}
            )
            return

        # 2. Verificar desasociación (Salida)
        match_disassoc = REGEX_DISASSOCIATED.search(line)
        if match_disassoc:
            client_mac = match_disassoc.group(1).lower()
            logger.info(f"[Syslog] Evento DESASOCIACIÓN detectado de {source_ip}: MAC={client_mac}")

            ap = next((a for a in self.ap_manager.get_all_aps() if a.ip_address == source_ip), None)
            ap_bssid = ap.bssid if ap else f"ap_{source_ip}"

            await self.access_events.handle_infrastructure_event(
                event_type="disassociate",
                mac_address=client_mac,
                ap_bssid=ap_bssid,
                details={"syslog_raw": line, "router_ip": source_ip}
            )

class SyslogServer:
    """Manejador del servicio Syslog."""
    def __init__(self, host: str, port: int, access_events: AccessEventsManager, ap_manager: APManager):
        self.host = host
        self.port = port
        self.access_events = access_events
        self.ap_manager = ap_manager
        self.transport = None

    async def start(self):
        loop = asyncio.get_running_loop()
        try:
            self.transport, _ = await loop.create_datagram_endpoint(
                lambda: SyslogProtocol(self.access_events, self.ap_manager),
                local_addr=(self.host, self.port)
            )
            logger.info(f"Servidor Syslog UDP para Routers/APs escuchando en {self.host}:{self.port}")
        except Exception as e:
            logger.warning(f"No se pudo enlazar puerto Syslog UDP {self.port} (puede requerir permisos): {e}")

    def stop(self):
        if self.transport:
            self.transport.close()
            logger.info("Servidor Syslog UDP detenido.")


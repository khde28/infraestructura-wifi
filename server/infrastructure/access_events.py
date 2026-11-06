# infrastructure/access_events.py - Detección y gestión de eventos de ingreso y salida
import asyncio
import logging
from typing import Dict, Optional, Any
from server.models import AccessEvent, AccessEventType, DeviceSession, AccessPoint
from server.database.db_adapter import DatabaseAdapter
from server.core.event_bus import BUS
from server.config import CONFIG

logger = logging.getLogger("IPS.AccessEvents")

class AccessEventsManager:
    """
    Gestiona y correlaciona eventos de ingreso (INGRESS), salida (EGRESS)
    y roaming entre puntos de acceso cuando la infraestructura los proporcione.
    """
    def __init__(self, db: DatabaseAdapter):
        self.db = db
        # Mapeo de último AP visto por dispositivo para detectar ROAMING: device_id -> bssid
        self._device_last_ap: Dict[str, str] = {}
        # Mapeo de dispositivos que ya fueron saludados: device_id -> bool
        self._welcomed_devices: set = set()

    async def initialize(self):
        # Suscribirse a eventos del sistema
        await BUS.subscribe("session_created", self._on_session_created)
        await BUS.subscribe("device_timeout_expired", self._on_device_timeout)
        logger.info("AccessEventsManager inicializado y suscrito a eventos del bus.")

    async def handle_infrastructure_event(self, event_type: str, mac_address: str,
                                          ap_bssid: str, ap_piso: str = "", ap_area: str = "",
                                          details: Optional[Dict[str, Any]] = None) -> AccessEvent:
        """
        Método invocado cuando un router, controlador Wi-Fi o Syslog reporta un evento de estación.
        Tipos de evento soportados:
        - 'associate', 'join', 'ingress' -> INGRESS o ROAMING
        - 'disassociate', 'leave', 'egress' -> EGRESS
        """
        mac_clean = mac_address.lower().strip()
        bssid_clean = ap_bssid.lower().strip()

        # Determinar tipo de evento normalizado
        if event_type.lower() in ("associate", "join", "ingress", "probe"):
            last_ap = self._device_last_ap.get(mac_clean)
            if last_ap and last_ap != bssid_clean:
                ev_type = AccessEventType.ROAMING
            else:
                ev_type = AccessEventType.INGRESS
            self._device_last_ap[mac_clean] = bssid_clean
        elif event_type.lower() in ("disassociate", "leave", "egress", "deauth"):
            ev_type = AccessEventType.EGRESS
            self._device_last_ap.pop(mac_clean, None)
            self._welcomed_devices.discard(mac_clean)
        else:
            ev_type = AccessEventType.INGRESS

        event = AccessEvent.create(
            event_type=ev_type,
            device_id=mac_clean,
            ap_bssid=bssid_clean,
            piso=ap_piso,
            area=ap_area,
            details=details
        )

        await self.db.record_access_event(event)
        logger.info(f"Evento de infraestructura registrado: {event.event_type.value} | MAC: {mac_clean} en AP: {bssid_clean}")

        # Publicar para dashboard y distribución
        await BUS.publish("access_event_generated", event)
        return event

    async def check_telemetry_access(self, device_id: str, session: DeviceSession, nearest_ap: Optional[AccessPoint]):
        """
        Analiza las mediciones recibidas de un celular para inferir ingreso o roaming si
        el router físico no emite syslog directamente.
        """
        if not nearest_ap:
            return

        last_ap_bssid = self._device_last_ap.get(device_id)
        current_bssid = nearest_ap.bssid.lower()

        # Si el dispositivo no había sido registrado en ningún AP: INGRESO
        if last_ap_bssid is None:
            self._device_last_ap[device_id] = current_bssid
            event = AccessEvent.create(
                event_type=AccessEventType.INGRESS,
                device_id=device_id,
                session_id=session.session_id,
                ap_bssid=current_bssid,
                ap_ssid=nearest_ap.ssid,
                piso=nearest_ap.piso,
                area=nearest_ap.area,
                details={"source": "telemetry_first_contact", "is_ingress_ap": nearest_ap.is_ingress_ap}
            )
            await self.db.record_access_event(event)
            await BUS.publish("access_event_generated", event)
            logger.info(f"Ingreso detectado por telemetría: Dispositivo {device_id} en {nearest_ap.area} ({nearest_ap.piso})")

        # Si cambió de AP: ROAMING
        elif last_ap_bssid != current_bssid:
            self._device_last_ap[device_id] = current_bssid
            event = AccessEvent.create(
                event_type=AccessEventType.ROAMING,
                device_id=device_id,
                session_id=session.session_id,
                ap_bssid=current_bssid,
                ap_ssid=nearest_ap.ssid,
                piso=nearest_ap.piso,
                area=nearest_ap.area,
                details={"previous_ap": last_ap_bssid, "source": "telemetry_roaming"}
            )
            await self.db.record_access_event(event)
            await BUS.publish("access_event_generated", event)
            logger.info(f"Roaming detectado: Dispositivo {device_id} se movió a {nearest_ap.area} ({nearest_ap.piso})")

    async def _on_session_created(self, session: DeviceSession):
        """Disparado cuando un celular se conecta por primera vez."""
        device_id = session.device_id
        if device_id not in self._welcomed_devices:
            self._welcomed_devices.add(device_id)
            # Notificar que se debe enviar mensaje de bienvenida
            await BUS.publish("send_welcome_message", {
                "device_id": device_id,
                "session_id": session.session_id,
                "title": CONFIG.WELCOME_MESSAGE_TITLE,
                "body": CONFIG.WELCOME_MESSAGE_BODY
            })

    async def _on_device_timeout(self, session: DeviceSession):
        """Disparado cuando un dispositivo pasa más de X segundos inactivo."""
        device_id = session.device_id
        last_bssid = self._device_last_ap.pop(device_id, "unknown")
        self._welcomed_devices.discard(device_id)

        event = AccessEvent.create(
            event_type=AccessEventType.EGRESS,
            device_id=device_id,
            session_id=session.session_id,
            ap_bssid=last_bssid,
            piso="",
            area="Fuera de cobertura / Salida",
            details={"reason": "connection_timeout"}
        )
        await self.db.record_access_event(event)
        await BUS.publish("access_event_generated", event)
        logger.warning(f"Salida (EGRESS) registrada para dispositivo {device_id} por inactividad.")


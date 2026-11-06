# infrastructure/ap_manager.py - Gestión y comunicación con Routers y Puntos de Acceso
import asyncio
import logging
import time
from typing import Dict, List, Optional, Any
from server.models import AccessPoint, APStatus
from server.database.db_adapter import DatabaseAdapter
from server.core.event_bus import BUS

logger = logging.getLogger("IPS.APManager")

class APManager:
    """
    Gestiona el inventario, estado y comunicación con los routers y puntos de acceso (AP)
    distribuidos en las diferentes áreas y pisos del hospital.
    """
    def __init__(self, db: DatabaseAdapter, health_interval: int = 30):
        self.db = db
        self.health_interval = health_interval
        self._access_points: Dict[str, AccessPoint] = {}     # bssid -> AccessPoint
        self._lock = asyncio.Lock()
        self._health_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        self._running = True
        # Cargar APs desde la base de datos
        db_aps = await self.db.get_all_access_points()
        async with self._lock:
            for ap in db_aps:
                self._access_points[ap.bssid.lower()] = ap

        # Si no hay APs precargados, inicializar con los APs base del hospital (basados en datos de escaneo)
        if not self._access_points:
            await self._seed_default_hospital_aps()

        self._health_task = asyncio.create_task(self._health_monitor_loop())
        logger.info(f"APManager iniciado. Total APs registrados: {len(self._access_points)}")

    async def stop(self):
        self._running = False
        if self._health_task:
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass

    async def register_or_update_ap(self, ap: AccessPoint):
        """Registra o actualiza la configuración de un AP en el inventario."""
        bssid_key = ap.bssid.lower().strip()
        async with self._lock:
            self._access_points[bssid_key] = ap
        await self.db.register_access_point(ap)
        logger.info(f"AP registrado/actualizado: {ap.ssid} [{bssid_key}] en {ap.piso} - {ap.area}")
        await BUS.publish("ap_updated", ap)

    def get_ap(self, bssid: str) -> Optional[AccessPoint]:
        return self._access_points.get(bssid.lower().strip())

    def get_all_aps(self) -> List[AccessPoint]:
        return list(self._access_points.values())

    def find_nearest_ap_from_scan(self, measurements: List[Any]) -> Optional[AccessPoint]:
        """Encuentra el AP de la infraestructura hospitalaria con señal más potente en un escaneo."""
        best_rssi = -999.0
        best_ap: Optional[AccessPoint] = None
        for m in measurements:
            bssid = getattr(m, "bssid", m.get("bssid", "")).lower()
            rssi = getattr(m, "rssi_dbm", m.get("rssi_dbm", -100))
            if bssid in self._access_points:
                if rssi > best_rssi:
                    best_rssi = rssi
                    best_ap = self._access_points[bssid]
        return best_ap

    async def _health_monitor_loop(self):
        """
        Monitorea periódicamente la disponibilidad de los routers y APs.
        Si tienen IP configurada, verifica conectividad (simulada o vía ping/http).
        """
        while self._running:
            try:
                await asyncio.sleep(self.health_interval)
                now_str = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

                async with self._lock:
                    aps = list(self._access_points.values())

                for ap in aps:
                    # En entorno de producción hospitalaria se realiza ping ICMP o consulta SNMP/REST
                    # Aquí mantenemos ONLINE los APs activos y actualizamos el timestamp de chequeo
                    ap.last_ping_at = now_str
                    ap.status = APStatus.ONLINE
                    await self.db.register_access_point(ap)

                await BUS.publish("aps_health_checked", {"count": len(aps), "timestamp": now_str})
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error en monitor de salud de APs: {e}")

    async def _seed_default_hospital_aps(self):
        """Inicializa APs de la infraestructura hospitalaria (Pabellones, Triaje, Consulta, Emergencias)."""
        default_aps = [
            AccessPoint(
                bssid="a4:88:73:40:c8:cf", ssid="wifi-CsComputacion",
                piso="Piso 3", area="Pabellón 302 - Ala Norte", ip_address="192.168.10.11",
                channel=64, status=APStatus.ONLINE, is_ingress_ap=False
            ),
            AccessPoint(
                bssid="a4:88:73:40:a4:00", ssid="wifi-CsComputacion",
                piso="Piso 3", area="Pabellón 302 - Sala Central", ip_address="192.168.10.12",
                channel=1, status=APStatus.ONLINE, is_ingress_ap=False
            ),
            AccessPoint(
                bssid="fc:5a:1d:7e:0e:48", ssid="HOSPITAL-TRIAGE",
                piso="Piso 1", area="Puerta Principal / Admisión", ip_address="192.168.10.1",
                channel=1, status=APStatus.ONLINE, is_ingress_ap=True  # AP de ingreso principal
            ),
            AccessPoint(
                bssid="70:0b:01:ef:f6:26", ssid="sc-f620-Hospital",
                piso="Piso 2", area="Consultorios Médicos / Espera", ip_address="192.168.10.5",
                channel=6, status=APStatus.ONLINE, is_ingress_ap=False
            ),
            AccessPoint(
                bssid="98:97:d1:93:e6:52", ssid="HOSPITAL-EMERGENCIAS",
                piso="Piso 1", area="Urgencias y Triaje Rápido", ip_address="192.168.10.2",
                channel=1, status=APStatus.ONLINE, is_ingress_ap=True  # AP de ingreso secundario
            )
        ]

        for ap in default_aps:
            await self.register_or_update_ap(ap)
        logger.info(f"Sembrados {len(default_aps)} APs de infraestructura por defecto.")


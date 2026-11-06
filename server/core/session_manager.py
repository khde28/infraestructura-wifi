# core/session_manager.py - Ciclo de vida y gestión de sesiones
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List, Any
from server.models import DeviceSession, SessionStatus, LocationResult
from server.database.db_adapter import DatabaseAdapter
from server.core.event_bus import BUS
from server.config import CONFIG

logger = logging.getLogger("IPS.SessionManager")

class SessionManager:
    """
    Gestiona el ciclo de vida de sesiones, autenticación/tokens de sesión,
    mensajes de bienvenida, respuesta de pacientes y timeouts.
    """
    def __init__(self, db: DatabaseAdapter, timeout_seconds: int = 45):
        self.db = db
        self.timeout_seconds = timeout_seconds
        self._sessions: Dict[str, DeviceSession] = {}           # session_id -> DeviceSession
        self._device_to_session: Dict[str, str] = {}            # device_id -> session_id
        self._mac_to_device: Dict[str, str] = {}                # mac_address -> device_id
        self._lock = asyncio.Lock()
        self._reaper_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        self._running = True
        self._reaper_task = asyncio.create_task(self._reap_expired_sessions_loop())
        logger.info(f"SessionManager iniciado. Timeout de inactividad: {self.timeout_seconds}s")

    async def stop(self):
        self._running = False
        if self._reaper_task:
            self._reaper_task.cancel()
            try:
                await self._reaper_task
            except asyncio.CancelledError:
                pass

    async def create_or_resume_session(self, device_id: str, mac_address: Optional[str] = None,
                                      patient_name: Optional[str] = None,
                                      device_model: Optional[str] = None,
                                      ip_address: Optional[str] = None) -> DeviceSession:
        """
        Crea una nueva sesión o reanuda una existente para el dispositivo móvil.
        """
        async with self._lock:
            existing_session_id = self._device_to_session.get(device_id)
            if existing_session_id and existing_session_id in self._sessions:
                session = self._sessions[existing_session_id]
                now_str = datetime.now(timezone.utc).isoformat()
                session.last_seen_at = now_str
                if ip_address:
                    session.ip_address = ip_address
                if mac_address:
                    session.mac_address = mac_address.lower()
                    self._mac_to_device[session.mac_address] = device_id
                
                # Si estaba desconectada, reactivar
                if session.status in (SessionStatus.DISCONNECTED, SessionStatus.EXPIRED):
                    session.status = SessionStatus.ACTIVE if session.welcome_accepted else SessionStatus.WELCOME_PENDING
                    await BUS.publish("device_reconnected", session)

                await self.db.save_session(session)
                return session

            # Crear nueva sesión
            session = DeviceSession.create_new(
                device_id=device_id,
                mac_address=mac_address,
                patient_name=patient_name,
                device_model=device_model,
                ip_address=ip_address
            )
            self._sessions[session.session_id] = session
            self._device_to_session[device_id] = session.session_id
            if mac_address:
                self._mac_to_device[mac_address.lower()] = device_id

            await self.db.save_session(session)
            logger.info(f"Nueva sesión creada: {session.session_id} para dispositivo: {device_id}")

            # Publicar evento de nueva sesión conectada
            await BUS.publish("session_created", session)
            return session

    async def handle_welcome_response(self, session_id: str, accepted: bool) -> Optional[DeviceSession]:
        """
        Registra la aceptación o rechazo del mensaje de bienvenida hospitalario.
        Cumple: 'Mostrar el mensaje de bienvenida y enviar la respuesta de aceptación o rechazo'.
        """
        async with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return None

            now_str = datetime.now(timezone.utc).isoformat()
            session.welcome_accepted = accepted
            session.welcome_response_at = now_str
            session.status = SessionStatus.ACTIVE if accepted else SessionStatus.OPTED_OUT
            session.last_seen_at = now_str

            await self.db.update_session_welcome(session_id, accepted, now_str)
            logger.info(f"Respuesta de bienvenida para sesión {session_id}: {'ACEPTADO' if accepted else 'RECHAZADO'}")

            await BUS.publish("welcome_answered", {
                "session_id": session_id,
                "device_id": session.device_id,
                "accepted": accepted,
                "status": session.status.value
            })
            return session

    async def update_activity(self, device_id: str, ap_bssid: Optional[str] = None):
        """Actualiza el timestamp de última actividad al recibir RSSI o heartbeat."""
        async with self._lock:
            session_id = self._device_to_session.get(device_id)
            if session_id and session_id in self._sessions:
                session = self._sessions[session_id]
                session.last_seen_at = datetime.now(timezone.utc).isoformat()
                session.measurements_count += 1
                if ap_bssid:
                    session.last_ap_bssid = ap_bssid.lower()

    async def update_device_location(self, device_id: str, location: LocationResult):
        """Asocia la última ubicación calculada a la sesión activa."""
        async with self._lock:
            session_id = self._device_to_session.get(device_id)
            if session_id and session_id in self._sessions:
                self._sessions[session_id].last_location = location

    def get_session_by_id(self, session_id: str) -> Optional[DeviceSession]:
        return self._sessions.get(session_id)

    def get_session_by_device(self, device_id: str) -> Optional[DeviceSession]:
        session_id = self._device_to_session.get(device_id)
        if session_id:
            return self._sessions.get(session_id)
        return None

    def get_device_by_mac(self, mac_address: str) -> Optional[str]:
        return self._mac_to_device.get(mac_address.lower())

    def get_all_active_sessions(self) -> List[DeviceSession]:
        return list(self._sessions.values())

    async def close_session(self, session_id: str, reason: str = "Client disconnect"):
        async with self._lock:
            session = self._sessions.get(session_id)
            if session:
                session.status = SessionStatus.DISCONNECTED
                session.last_seen_at = datetime.now(timezone.utc).isoformat()
                await self.db.save_session(session)
                await BUS.publish("session_closed", {"session": session, "reason": reason})

    async def _reap_expired_sessions_loop(self):
        """
        Monitorea periódicamente las sesiones inactivas. Si no hay mediciones
        o heartbeat en `timeout_seconds`, la marca como EXPIRED y genera evento de salida.
        """
        while self._running:
            try:
                await asyncio.sleep(CONFIG.CLEANUP_INTERVAL_SECONDS)
                now = datetime.now(timezone.utc)
                expired_sessions: List[DeviceSession] = []

                async with self._lock:
                    for s in list(self._sessions.values()):
                        if s.status in (SessionStatus.DISCONNECTED, SessionStatus.EXPIRED):
                            continue
                        last_seen = datetime.fromisoformat(s.last_seen_at)
                        if (now - last_seen).total_seconds() > self.timeout_seconds:
                            s.status = SessionStatus.EXPIRED
                            expired_sessions.append(s)

                for s in expired_sessions:
                    logger.warning(f"Sesión expirada por inactividad: {s.session_id} (device: {s.device_id})")
                    await self.db.save_session(s)
                    # Publicar evento de salida
                    await BUS.publish("device_timeout_expired", s)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error en _reap_expired_sessions_loop: {e}", exc_info=True)


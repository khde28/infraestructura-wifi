# core/connection_manager.py - Gestor de conexiones concurrentes (~100 dispositivos)
import asyncio
import json
import logging
import time
from typing import Dict, Set, Optional, Any
from aiohttp import web, WSMsgType

logger = logging.getLogger("IPS.ConnectionManager")

class ConnectionManager:
    """
    Gestiona el pool de conexiones WebSocket de dispositivos móviles (~100 dispositivos)
    y conexiones del Dashboard Administrativo en tiempo real.
    """
    def __init__(self, max_devices: int = 100, heartbeat_interval: int = 15):
        self.max_devices = max_devices
        self.heartbeat_interval = heartbeat_interval
        
        # Mapeos activos
        self._device_ws: Dict[str, web.WebSocketResponse] = {}      # device_id -> WebSocket
        self._ws_device: Dict[web.WebSocketResponse, str] = {}      # WebSocket -> device_id
        self._dashboard_ws: Set[web.WebSocketResponse] = set()       # WebSockets del Dashboard
        
        # Sincronización y control de concurrencia
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(max_devices)
        
        # Métricas de rendimiento
        self.total_connected_ever: int = 0
        self.messages_sent: int = 0
        self.messages_received: int = 0
        self._last_heartbeat_time = time.time()
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None

    async def start(self):
        self._running = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info(f"ConnectionManager iniciado. Capacidad máxima configurada: {self.max_devices} dispositivos.")

    async def stop(self):
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        async with self._lock:
            # Cerrar sockets móviles
            for ws in list(self._device_ws.values()):
                await ws.close(code=1000, message=b"Server shutting down")
            # Cerrar sockets dashboard
            for ws in list(self._dashboard_ws):
                await ws.close(code=1000, message=b"Server shutting down")
            self._device_ws.clear()
            self._ws_device.clear()
            self._dashboard_ws.clear()

    # ==================== REGISTRO Y GESTIÓN DE CLIENTES MÓVILES ====================

    async def register_device(self, device_id: str, ws: web.WebSocketResponse) -> bool:
        """
        Registra una conexión WebSocket para un dispositivo móvil verificando
        el límite de concurrencia (~100 dispositivos).
        """
        async with self._lock:
            if len(self._device_ws) >= self.max_devices and device_id not in self._device_ws:
                logger.warning(
                    f"Capacidad máxima alcanzada ({self.max_devices}). Rechazando conexión de device_id: {device_id}"
                )
                return False

            # Si ya existía una conexión previa para este device_id, cerrar la anterior
            if device_id in self._device_ws:
                old_ws = self._device_ws[device_id]
                try:
                    await old_ws.close(code=1001, message=b"New connection established")
                except Exception:
                    pass
                self._ws_device.pop(old_ws, None)

            self._device_ws[device_id] = ws
            self._ws_device[ws] = device_id
            self.total_connected_ever += 1
            logger.info(f"Dispositivo registrado en WS: {device_id}. Conectados activos: {len(self._device_ws)}/{self.max_devices}")
            return True

    async def unregister_device(self, ws: web.WebSocketResponse) -> Optional[str]:
        """Elimina un socket móvil cuando se cierra la conexión."""
        async with self._lock:
            device_id = self._ws_device.pop(ws, None)
            if device_id and device_id in self._device_ws:
                self._device_ws.pop(device_id, None)
                logger.info(f"Dispositivo desregistrado: {device_id}. Conectados activos: {len(self._device_ws)}")
            return device_id

    def is_device_connected(self, device_id: str) -> bool:
        return device_id in self._device_ws

    @property
    def active_devices_count(self) -> int:
        return len(self._device_ws)

    # ==================== REGISTRO Y GESTIÓN DEL DASHBOARD ====================

    async def register_dashboard(self, ws: web.WebSocketResponse):
        async with self._lock:
            self._dashboard_ws.add(ws)
            logger.info(f"Dashboard Web conectado vía WS. Total dashboards activos: {len(self._dashboard_ws)}")

    async def unregister_dashboard(self, ws: web.WebSocketResponse):
        async with self._lock:
            self._dashboard_ws.discard(ws)
            logger.info(f"Dashboard Web desconectado. Total dashboards activos: {len(self._dashboard_ws)}")

    # ==================== ENVÍO Y DISTRIBUCIÓN DE MENSAJES ====================

    async def send_to_device(self, device_id: str, message: Dict[str, Any]) -> bool:
        """
        Envía un mensaje JSON en tiempo real a un dispositivo móvil específico.
        Cumple: 'Distribuir los resultados de ubicación y los mensajes a cada dispositivo'.
        """
        ws = self._device_ws.get(device_id)
        if not ws or ws.closed:
            return False

        try:
            payload = json.dumps(message)
            await ws.send_str(payload)
            self.messages_sent += 1
            return True
        except Exception as e:
            logger.error(f"Error enviando mensaje a device {device_id}: {e}")
            return False

    async def broadcast_to_dashboard(self, event: Dict[str, Any]):
        """Envía actualizaciones en tiempo real a todos los dashboards conectados."""
        if not self._dashboard_ws:
            return

        payload = json.dumps(event)
        dead_sockets = []
        for ws in self._dashboard_ws:
            if ws.closed:
                dead_sockets.append(ws)
                continue
            try:
                await ws.send_str(payload)
                self.messages_sent += 1
            except Exception:
                dead_sockets.append(ws)

        if dead_sockets:
            async with self._lock:
                for ws in dead_sockets:
                    self._dashboard_ws.discard(ws)

    # ==================== HEARTBEAT Y DETECCIÓN DE ZOMBIES ====================

    async def _heartbeat_loop(self):
        """Envía ping periódicamente a todos los sockets activos para mantener viva la conexión."""
        while self._running:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                ping_payload = json.dumps({"type": "ping", "timestamp": time.time()})

                # Ping a dispositivos
                async with self._lock:
                    sockets = list(self._device_ws.values()) + list(self._dashboard_ws)

                for ws in sockets:
                    if not ws.closed:
                        try:
                            await ws.send_str(ping_payload)
                        except Exception:
                            pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error en heartbeat loop: {e}")

    def get_metrics(self) -> Dict[str, Any]:
        """Métricas para el Dashboard Administrativo (Integrante 6)."""
        return {
            "max_capacity": self.max_devices,
            "connected_devices": len(self._device_ws),
            "connected_dashboards": len(self._dashboard_ws),
            "total_connected_ever": self.total_connected_ever,
            "messages_sent": self.messages_sent,
            "messages_received": self.messages_received,
            "utilization_percent": round((len(self._device_ws) / self.max_devices) * 100, 1)
        }


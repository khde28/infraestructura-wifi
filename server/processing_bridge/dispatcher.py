# processing_bridge/dispatcher.py - Despachador de telemetría a módulos de procesamiento
import asyncio
import logging
import json
import time
from typing import Optional, Dict, Any
import aiohttp
from server.models import RSSIBatch, LocationResult
from server.processing_bridge.mock_engine import FingerprintProcessingEngine
from server.core.connection_manager import ConnectionManager
from server.core.session_manager import SessionManager
from server.database.db_adapter import DatabaseAdapter
from server.core.event_bus import BUS
from server.config import CONFIG

logger = logging.getLogger("IPS.Dispatcher")

class ProcessingDispatcher:
    """
    Encola, procesa y distribuye las mediciones de múltiples dispositivos móviles.
    Coordina la entrega de resultados de ubicación a cada celular.
    """
    def __init__(self, engine: FingerprintProcessingEngine,
                 conn_manager: ConnectionManager,
                 session_manager: SessionManager,
                 db: DatabaseAdapter,
                 worker_count: int = 4):
        self.engine = engine
        self.conn_manager = conn_manager
        self.session_manager = session_manager
        self.db = db
        self.worker_count = worker_count
        
        # Cola de procesamiento no bloqueante
        self._queue: asyncio.Queue[RSSIBatch] = asyncio.Queue(maxsize=1000)
        self._workers: list[asyncio.Task] = []
        self._running = False
        self._http_client: Optional[aiohttp.ClientSession] = None

        # Estadísticas de procesamiento
        self.batches_processed = 0
        self.total_measurements_processed = 0
        self.avg_processing_time_ms = 0.0

    async def start(self):
        self._running = True
        self._http_client = aiohttp.ClientSession()
        for i in range(self.worker_count):
            task = asyncio.create_task(self._worker_loop(i))
            self._workers.append(task)
        logger.info(f"ProcessingDispatcher iniciado con {self.worker_count} workers concurrentes.")

    async def stop(self):
        self._running = False
        for task in self._workers:
            task.cancel()
        if self._http_client:
            await self._http_client.close()

    async def submit_batch(self, batch: RSSIBatch):
        """Pone en cola un lote de mediciones RSSI para procesamiento asíncrono."""
        try:
            self._queue.put_nowait(batch)
        except asyncio.QueueFull:
            logger.warning("Cola de procesamiento llena. Descartando lote más antiguo para priorizar tiempo real.")
            try:
                _ = self._queue.get_nowait()
                self._queue.put_nowait(batch)
            except Exception:
                pass

    async def process_sync(self, batch: RSSIBatch) -> LocationResult:
        """Procesa un lote de forma síncrona/inmediata (usado por peticiones HTTP REST POST)."""
        t0 = time.time()
        location = await self._execute_processing(batch)
        elapsed_ms = (time.time() - t0) * 1000
        self._update_stats(len(batch.measurements), elapsed_ms)
        
        # Guardar y distribuir
        await self._persist_and_distribute(batch, location)
        return location

    async def _worker_loop(self, worker_id: int):
        while self._running:
            try:
                batch = await self._queue.get()
                t0 = time.time()
                location = await self._execute_processing(batch)
                elapsed_ms = (time.time() - t0) * 1000
                self._update_stats(len(batch.measurements), elapsed_ms)

                await self._persist_and_distribute(batch, location)
                self._queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error en worker {worker_id} de procesamiento: {e}", exc_info=True)

    async def _execute_processing(self, batch: RSSIBatch) -> LocationResult:
        """
        Envía al módulo externo de los Integrantes 4 y 5 si está configurado,
        o utiliza el motor de fingerprinting local.
        """
        if CONFIG.EXTERNAL_PROCESSING_URL and self._http_client:
            try:
                async with self._http_client.post(
                    CONFIG.EXTERNAL_PROCESSING_URL,
                    json=batch.to_dict(),
                    timeout=aiohttp.ClientTimeout(total=2.0)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return LocationResult(
                            device_id=batch.device_id,
                            session_id=batch.session_id,
                            piso=data.get("piso", "Piso 1"),
                            area=data.get("area", "Área Desconocida"),
                            lugar_id=data.get("lugar_id", "external"),
                            confidence=float(data.get("confidence", 0.8)),
                            timestamp_ms=batch.client_timestamp_ms,
                            coordinates=data.get("coordinates"),
                            algorithm_info=data.get("algorithm_info", {"engine": "external_service"}),
                            navigation_message=data.get("navigation_message")
                        )
            except Exception as e:
                logger.warning(f"Fallo en servicio externo de procesamiento ({e}), usando motor interno de fallback.")

        # Procesamiento interno con FingerprintProcessingEngine
        return self.engine.estimate_location(batch.device_id, batch.session_id, batch.measurements)

    async def _persist_and_distribute(self, batch: RSSIBatch, location: LocationResult):
        """Guarda en BD y distribuye la ubicación calculada al dispositivo y al dashboard."""
        # 1. Actualizar sesión en memoria y base de datos
        await self.session_manager.update_device_location(batch.device_id, location)
        await self.session_manager.update_activity(batch.device_id)
        
        # 2. Guardar en historial de base de datos
        await self.db.save_location_history(location)
        await self.db.save_rssi_batch_log(batch.device_id, batch.session_id, batch.measurements)

        # 3. Distribuir al dispositivo móvil vía WebSocket (Cumple: 'Distribuir los resultados a cada dispositivo')
        loc_msg = {
            "type": "location_update",
            "device_id": location.device_id,
            "session_id": location.session_id,
            "piso": location.piso,
            "area": location.area,
            "lugar_id": location.lugar_id,
            "confidence": location.confidence,
            "navigation_message": location.navigation_message,
            "timestamp_ms": location.timestamp_ms
        }
        await self.conn_manager.send_to_device(batch.device_id, loc_msg)

        # 4. Transmitir en tiempo real al Dashboard Administrativo (Integrante 6)
        dash_payload = {
            "event": "device_location",
            "data": location.to_dict()
        }
        await self.conn_manager.broadcast_to_dashboard(dash_payload)

        # 5. Publicar en bus interno
        await BUS.publish("location_calculated", location)

    def _update_stats(self, count: int, elapsed_ms: float):
        self.batches_processed += 1
        self.total_measurements_processed += count
        if self.avg_processing_time_ms == 0.0:
            self.avg_processing_time_ms = elapsed_ms
        else:
            self.avg_processing_time_ms = round((self.avg_processing_time_ms * 0.9) + (elapsed_ms * 0.1), 2)


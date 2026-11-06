# core/event_bus.py - Bus de eventos asíncrono para desacoplamiento interno
import asyncio
import logging
from typing import Callable, Dict, List, Any

logger = logging.getLogger("IPS.EventBus")

class EventBus:
    """
    Bus de eventos en memoria usando colas asíncronas para comunicación
    desacoplada y concurrente entre los componentes del servidor.
    """
    def __init__(self):
        self._subscribers: Dict[str, List[Callable[[Any], Any]]] = {}
        self._lock = asyncio.Lock()

    async def subscribe(self, event_name: str, callback: Callable[[Any], Any]):
        async with self._lock:
            if event_name not in self._subscribers:
                self._subscribers[event_name] = []
            self._subscribers[event_name].append(callback)
            logger.debug(f"Suscrito a evento: '{event_name}'")

    async def publish(self, event_name: str, payload: Any):
        """Publica un evento llamando a los callbacks registrados de forma asíncrona."""
        callbacks = self._subscribers.get(event_name, [])
        if not callbacks:
            return

        for cb in callbacks:
            try:
                if asyncio.iscoroutinefunction(cb):
                    asyncio.create_task(cb(payload))
                else:
                    cb(payload)
            except Exception as e:
                logger.error(f"Error procesando evento '{event_name}': {e}", exc_info=True)

# Instancia global del bus de eventos
BUS = EventBus()


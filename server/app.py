# app.py - Servidor Central e Infraestructura Wi-Fi (Integrante 2)
import asyncio
import logging
import os
import glob
from pathlib import Path
from aiohttp import web
import aiohttp_cors

from server.config import CONFIG
from server.database.db_adapter import DatabaseAdapter
from server.core.connection_manager import ConnectionManager
from server.core.session_manager import SessionManager
from server.core.event_bus import BUS
from server.infrastructure.ap_manager import APManager
from server.infrastructure.access_events import AccessEventsManager
from server.infrastructure.syslog_listener import SyslogServer
from server.processing_bridge.mock_engine import FingerprintProcessingEngine
from server.processing_bridge.dispatcher import ProcessingDispatcher

# Rutas de la API
from server.api.routes_devices import routes as routes_devices
from server.api.routes_infrastructure import routes as routes_infra
from server.api.routes_dashboard import routes as routes_dashboard
from server.api.websocket_handlers import routes as routes_ws

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("IPS.Server")

async def init_app() -> web.Application:
    """Inicializa la aplicación aiohttp con todos sus servicios y dependencias."""
    app = web.Application()

    # 1. Base de datos
    db = DatabaseAdapter(CONFIG.DATABASE_PATH)
    db.init_schema()
    app["db"] = db

    # 2. Cargar datos de mapeo iniciales de los archivos txt del proyecto
    data_files = glob.glob(str(Path(__file__).parent.parent / "rssi_*.txt"))
    for df in data_files:
        try:
            count = await db.load_calibration_from_txt(df)
            logger.info(f"Cargadas {count} mediciones de mapeo desde {os.path.basename(df)}")
        except Exception as e:
            logger.warning(f"No se pudo cargar {df}: {e}")

    # 3. Gestores Core
    conn_manager = ConnectionManager(
        max_devices=CONFIG.MAX_CONCURRENT_DEVICES,
        heartbeat_interval=CONFIG.HEARTBEAT_INTERVAL_SECONDS
    )
    session_manager = SessionManager(db, timeout_seconds=CONFIG.CONNECTION_TIMEOUT_SECONDS)
    ap_manager = APManager(db, health_interval=CONFIG.AP_HEALTH_CHECK_INTERVAL)
    access_events = AccessEventsManager(db)

    # 4. Motor de Procesamiento y Despachador de Mediciones
    processing_engine = FingerprintProcessingEngine(db)
    await processing_engine.initialize_from_db()

    dispatcher = ProcessingDispatcher(
        engine=processing_engine,
        conn_manager=conn_manager,
        session_manager=session_manager,
        db=db,
        worker_count=4
    )

    # Inyectar dependencias en el contexto de la aplicación
    app["conn_manager"] = conn_manager
    app["session_manager"] = session_manager
    app["ap_manager"] = ap_manager
    app["access_events"] = access_events
    app["processing_engine"] = processing_engine
    app["dispatcher"] = dispatcher

    # 5. Suscripciones al EventBus para sincronización y notificación al Dashboard
    async def _on_access_event(event):
        await conn_manager.broadcast_to_dashboard({
            "event": "access_event",
            "data": event.to_dict()
        })

    async def _on_welcome_sent(payload):
        device_id = payload["device_id"]
        # Enviar notificación de bienvenida al celular por WebSocket
        await conn_manager.send_to_device(device_id, {
            "type": "welcome_prompt",
            "title": payload["title"],
            "body": payload["body"],
            "session_id": payload["session_id"]
        })

    async def _on_welcome_answered(payload):
        await conn_manager.broadcast_to_dashboard({
            "event": "welcome_answered",
            "data": payload
        })

    await BUS.subscribe("access_event_generated", _on_access_event)
    await BUS.subscribe("send_welcome_message", _on_welcome_sent)
    await BUS.subscribe("welcome_answered", _on_welcome_answered)

    # 6. Registrar ciclo de vida de la aplicación (Startup / Cleanup)
    syslog_server = SyslogServer(
        host=CONFIG.SYSLOG_HOST,
        port=CONFIG.SYSLOG_PORT,
        access_events=access_events,
        ap_manager=ap_manager
    )

    async def on_startup(app: web.Application):
        await conn_manager.start()
        await session_manager.start()
        await ap_manager.start()
        await access_events.initialize()
        await dispatcher.start()
        if CONFIG.SYSLOG_ENABLED:
            await syslog_server.start()
        logger.info(f"=== Servidor Central IPS Hospitalario iniciado en http://{CONFIG.HOST}:{CONFIG.PORT} ===")

    async def on_cleanup(app: web.Application):
        logger.info("Deteniendo servicios del servidor central...")
        if CONFIG.SYSLOG_ENABLED:
            syslog_server.stop()
        await dispatcher.stop()
        await ap_manager.stop()
        await session_manager.stop()
        await conn_manager.stop()

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    # 7. Registrar Rutas
    app.add_routes(routes_devices)
    app.add_routes(routes_infra)
    app.add_routes(routes_dashboard)
    app.add_routes(routes_ws)

    # Servir archivos estáticos del Dashboard Web
    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        app.router.add_static("/static/", path=str(static_dir), name="static")
        
        async def index_handler(request):
            return web.FileResponse(static_dir / "index.html")
            
        app.router.add_get("/", index_handler)

    # 8. Configurar CORS para permitir que cualquier app web o móvil consuma la API
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*"
        )
    })
    for route in list(app.router.routes()):
        cors.add(route)

    return app

if __name__ == "__main__":
    app = init_app()
    web.run_app(app, host=CONFIG.HOST, port=CONFIG.PORT)


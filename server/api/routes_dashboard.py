# api/routes_dashboard.py - Endpoints para el Dashboard Administrativo (Integrante 6)
import logging
from aiohttp import web
from server.core.session_manager import SessionManager
from server.core.connection_manager import ConnectionManager
from server.infrastructure.ap_manager import APManager
from server.database.db_adapter import DatabaseAdapter
from server.processing_bridge.dispatcher import ProcessingDispatcher
from server.models import SessionStatus

logger = logging.getLogger("IPS.ApiDashboard")

routes = web.RouteTableDef()

@routes.get("/api/v1/dashboard/summary")
async def dashboard_summary(request: web.Request):
    """Resumen ejecutivo para el dashboard: concurrencia, distribución por piso y estado de pacientes."""
    session_manager: SessionManager = request.app["session_manager"]
    conn_manager: ConnectionManager = request.app["conn_manager"]
    ap_manager: APManager = request.app["ap_manager"]

    sessions = session_manager.get_all_active_sessions()
    
    # Distribución por piso y área
    floor_dist = {}
    area_dist = {}
    active_count = 0
    welcome_accepted_count = 0
    welcome_pending_count = 0
    welcome_opted_out_count = 0

    for s in sessions:
        if s.status in (SessionStatus.ACTIVE, SessionStatus.WELCOME_PENDING, SessionStatus.CONNECTED):
            active_count += 1
            if s.last_location:
                piso = s.last_location.piso or "Sin ubicar"
                area = s.last_location.area or "Sin ubicar"
                floor_dist[piso] = floor_dist.get(piso, 0) + 1
                area_dist[area] = area_dist.get(area, 0) + 1

        if s.welcome_accepted is True:
            welcome_accepted_count += 1
        elif s.welcome_accepted is False:
            welcome_opted_out_count += 1
        else:
            welcome_pending_count += 1

    return web.json_response({
        "status": "success",
        "summary": {
            "capacity_max": conn_manager.max_devices,
            "active_devices": active_count,
            "utilization_percent": round((active_count / conn_manager.max_devices) * 100, 1),
            "welcome_stats": {
                "accepted": welcome_accepted_count,
                "pending": welcome_pending_count,
                "opted_out": welcome_opted_out_count
            },
            "floor_distribution": floor_dist,
            "area_distribution": area_dist,
            "total_infrastructure_aps": len(ap_manager.get_all_aps())
        }
    })

@routes.get("/api/v1/dashboard/devices")
async def list_dashboard_devices(request: web.Request):
    """Lista detallada de todos los dispositivos registrados y sus ubicaciones en tiempo real."""
    session_manager: SessionManager = request.app["session_manager"]
    sessions = session_manager.get_all_active_sessions()

    return web.json_response({
        "status": "success",
        "total": len(sessions),
        "devices": [s.to_dict() for s in sessions]
    })

@routes.get("/api/v1/dashboard/events")
async def list_dashboard_events(request: web.Request):
    """Historial de eventos de ingreso y salida provistos por la infraestructura."""
    db: DatabaseAdapter = request.app["db"]
    limit = int(request.query.get("limit", "50"))
    events = await db.get_recent_access_events(limit=limit)

    return web.json_response({
        "status": "success",
        "count": len(events),
        "events": events
    })

@routes.get("/api/v1/dashboard/metrics")
async def get_dashboard_metrics(request: web.Request):
    """Métricas técnicas de concurrencia y velocidad del servidor central."""
    conn_manager: ConnectionManager = request.app["conn_manager"]
    dispatcher: ProcessingDispatcher = request.app["dispatcher"]

    return web.json_response({
        "status": "success",
        "connection_metrics": conn_manager.get_metrics(),
        "processing_metrics": {
            "batches_processed": dispatcher.batches_processed,
            "total_measurements": dispatcher.total_measurements_processed,
            "avg_processing_time_ms": dispatcher.avg_processing_time_ms,
            "queue_pending_items": dispatcher._queue.qsize()
        }
    })


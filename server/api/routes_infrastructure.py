# api/routes_infrastructure.py - Endpoints para comunicación con APs e Infraestructura Wi-Fi
import logging
from aiohttp import web
from server.models import AccessPoint, APStatus
from server.infrastructure.ap_manager import APManager
from server.infrastructure.access_events import AccessEventsManager
from server.database.db_adapter import DatabaseAdapter
from server.processing_bridge.mock_engine import FingerprintProcessingEngine

logger = logging.getLogger("IPS.ApiInfra")

routes = web.RouteTableDef()

@routes.get("/api/v1/infrastructure/aps")
async def list_aps(request: web.Request):
    """Lista todos los Routers y APs configurados en la infraestructura hospitalaria."""
    ap_manager: APManager = request.app["ap_manager"]
    aps = ap_manager.get_all_aps()
    return web.json_response({
        "status": "success",
        "total": len(aps),
        "access_points": [ap.to_dict() for ap in aps]
    })

@routes.post("/api/v1/infrastructure/aps")
async def add_or_update_ap(request: web.Request):
    """Registra o actualiza la información de un router / AP hospitalario."""
    ap_manager: APManager = request.app["ap_manager"]
    try:
        data = await request.json()
        ap = AccessPoint(
            bssid=data["bssid"],
            ssid=data.get("ssid", "HOSPITAL-WIFI"),
            piso=data.get("piso", "Piso 1"),
            area=data.get("area", "Área General"),
            ip_address=data.get("ip_address"),
            channel=data.get("channel"),
            status=APStatus(data.get("status", "ONLINE")),
            is_ingress_ap=bool(data.get("is_ingress_ap", False))
        )
    except Exception as e:
        return web.json_response({"status": "error", "message": f"Datos incompletos o inválidos: {e}"}, status=400)

    await ap_manager.register_or_update_ap(ap)
    return web.json_response({"status": "success", "access_point": ap.to_dict()})

@routes.post("/api/v1/infrastructure/events")
async def receive_ap_event(request: web.Request):
    """
    Recibe eventos provistos por routers / controladores Wi-Fi vía Webhook HTTP.
    Cumple: 'Gestionar eventos de ingreso y salida cuando la infraestructura los proporcione'.
    """
    access_events: AccessEventsManager = request.app["access_events"]
    ap_manager: APManager = request.app["ap_manager"]

    try:
        data = await request.json()
        event_type = data.get("event_type", "associate")
        mac_address = data.get("mac_address")
        ap_bssid = data.get("ap_bssid")
    except Exception:
        return web.json_response({"status": "error", "message": "JSON inválido"}, status=400)

    if not mac_address or not ap_bssid:
        return web.json_response({
            "status": "error",
            "message": "mac_address y ap_bssid son obligatorios"
        }, status=400)

    ap = ap_manager.get_ap(ap_bssid)
    piso = ap.piso if ap else data.get("piso", "")
    area = ap.area if ap else data.get("area", "")

    event = await access_events.handle_infrastructure_event(
        event_type=event_type,
        mac_address=mac_address,
        ap_bssid=ap_bssid,
        ap_piso=piso,
        ap_area=area,
        details=data.get("details", {"source": "webhook"})
    )

    return web.json_response({
        "status": "success",
        "event": event.to_dict()
    })

@routes.post("/api/v1/infrastructure/calibration/import-txt")
async def import_calibration(request: web.Request):
    """
    Importa un archivo TXT de mediciones para alimentar la base de calibración/fingerprinting.
    """
    db: DatabaseAdapter = request.app["db"]
    engine: FingerprintProcessingEngine = request.app["processing_engine"]

    try:
        data = await request.json()
        file_path = data.get("file_path")
    except Exception:
        return web.json_response({"status": "error", "message": "JSON inválido"}, status=400)

    if not file_path:
        return web.json_response({"status": "error", "message": "file_path es requerido"}, status=400)

    try:
        count = await db.load_calibration_from_txt(file_path)
        # Recargar huellas en el motor de procesamiento
        await engine.initialize_from_db()
        return web.json_response({
            "status": "success",
            "message": f"Se importaron {count} mediciones de calibración desde {file_path}",
            "reference_points_count": len(engine.reference_fingerprints)
        })
    except Exception as e:
        return web.json_response({"status": "error", "message": str(e)}, status=500)


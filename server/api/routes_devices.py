# api/routes_devices.py - Endpoints REST para celulares Android (Integrante 1)
import json
import logging
from aiohttp import web
from server.models import RSSIBatch, SessionStatus
from server.core.session_manager import SessionManager
from server.core.connection_manager import ConnectionManager
from server.processing_bridge.dispatcher import ProcessingDispatcher
from server.infrastructure.access_events import AccessEventsManager
from server.infrastructure.ap_manager import APManager
from server.config import CONFIG

logger = logging.getLogger("IPS.ApiDevices")

routes = web.RouteTableDef()

@routes.post("/api/v1/devices/register")
async def register_device(request: web.Request):
    """
    Registra un dispositivo móvil Android y crea/reanuda su sesión.
    Devuelve los datos de la sesión y el mensaje de bienvenida hospitalario.
    """
    session_manager: SessionManager = request.app["session_manager"]
    conn_manager: ConnectionManager = request.app["conn_manager"]

    # Verificar capacidad de 100 dispositivos
    if conn_manager.active_devices_count >= CONFIG.MAX_CONCURRENT_DEVICES:
        return web.json_response({
            "status": "error",
            "message": f"Servidor a capacidad máxima ({CONFIG.MAX_CONCURRENT_DEVICES} dispositivos)."
        }, status=503)

    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON inválido"}, status=400)

    device_id = data.get("device_id")
    if not device_id:
        return web.json_response({"status": "error", "message": "El campo 'device_id' es obligatorio"}, status=400)

    client_ip = request.remote
    session = await session_manager.create_or_resume_session(
        device_id=device_id,
        mac_address=data.get("mac_address"),
        patient_name=data.get("patient_name"),
        device_model=data.get("device_model"),
        ip_address=client_ip
    )

    return web.json_response({
        "status": "success",
        "session": session.to_dict(),
        "welcome_prompt": {
            "title": CONFIG.WELCOME_MESSAGE_TITLE,
            "message": CONFIG.WELCOME_MESSAGE_BODY,
            "requires_action": session.welcome_accepted is None,
            "current_status": session.status.value
        }
    })

@routes.post("/api/v1/sessions/{session_id}/welcome")
async def answer_welcome(request: web.Request):
    """
    Registra la aceptación o rechazo del paciente al mensaje de bienvenida.
    Cumple: 'Mostrar el mensaje de bienvenida y enviar la respuesta de aceptación o rechazo'.
    """
    session_id = request.match_info["session_id"]
    session_manager: SessionManager = request.app["session_manager"]

    try:
        data = await request.json()
        accepted = bool(data.get("accepted", True))
    except Exception:
        return web.json_response({"status": "error", "message": "JSON inválido"}, status=400)

    updated_session = await session_manager.handle_welcome_response(session_id, accepted)
    if not updated_session:
        return web.json_response({"status": "error", "message": "Sesión no encontrada"}, status=404)

    return web.json_response({
        "status": "success",
        "session_id": session_id,
        "welcome_accepted": accepted,
        "session_status": updated_session.status.value,
        "message": "Servicio de orientación activado" if accepted else "Orientación desactivada por el usuario"
    })

@routes.post("/api/v1/telemetry/rssi")
async def receive_rssi(request: web.Request):
    """
    Recibe un paquete de mediciones RSSI de un celular Android.
    Calcula de inmediato la ubicación y actualiza los eventos de presencia/roaming.
    """
    dispatcher: ProcessingDispatcher = request.app["dispatcher"]
    session_manager: SessionManager = request.app["session_manager"]
    ap_manager: APManager = request.app["ap_manager"]
    access_events: AccessEventsManager = request.app["access_events"]

    try:
        data = await request.json()
        batch = RSSIBatch.from_dict(data)
    except Exception as e:
        return web.json_response({"status": "error", "message": f"Formato inválido: {e}"}, status=400)

    # Buscar o asegurar sesión
    session = session_manager.get_session_by_device(batch.device_id)
    if not session:
        session = await session_manager.create_or_resume_session(batch.device_id)
    batch.session_id = session.session_id

    # Detectar AP más cercano para correlación de infraestructura (ingreso/roaming)
    nearest_ap = ap_manager.find_nearest_ap_from_scan(batch.measurements)
    await access_events.check_telemetry_access(batch.device_id, session, nearest_ap)

    # Procesar ubicación (k-NN / Integrantes 4 y 5)
    location = await dispatcher.process_sync(batch)

    return web.json_response({
        "status": "success",
        "location": location.to_dict()
    })

@routes.get("/api/v1/devices/{device_id}/location")
async def get_device_location(request: web.Request):
    """Consulta la última ubicación conocida del dispositivo."""
    device_id = request.match_info["device_id"]
    session_manager: SessionManager = request.app["session_manager"]
    session = session_manager.get_session_by_device(device_id)

    if not session or not session.last_location:
        return web.json_response({"status": "error", "message": "Ubicación aún no disponible"}, status=404)

    return web.json_response({
        "status": "success",
        "device_id": device_id,
        "location": session.last_location.to_dict()
    })

@routes.post("/api/v1/sessions/heartbeat")
async def device_heartbeat(request: web.Request):
    """Mantiene la sesión activa para evitar timeout por inactividad."""
    session_manager: SessionManager = request.app["session_manager"]
    try:
        data = await request.json()
        device_id = data.get("device_id")
    except Exception:
        return web.json_response({"status": "error", "message": "JSON inválido"}, status=400)

    if not device_id:
        return web.json_response({"status": "error", "message": "device_id requerido"}, status=400)

    await session_manager.update_activity(device_id)
    return web.json_response({"status": "success", "device_id": device_id, "alive": True})


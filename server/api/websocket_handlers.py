# api/websocket_handlers.py - Handlers de WebSocket para Móviles y Dashboard
import asyncio
import json
import logging
from aiohttp import web, WSMsgType
from server.core.connection_manager import ConnectionManager
from server.core.session_manager import SessionManager
from server.processing_bridge.dispatcher import ProcessingDispatcher
from server.infrastructure.access_events import AccessEventsManager
from server.infrastructure.ap_manager import APManager
from server.models import RSSIBatch
from server.config import CONFIG

logger = logging.getLogger("IPS.WebSockets")

routes = web.RouteTableDef()

@routes.get("/ws/device")
async def ws_device_handler(request: web.Request):
    """
    WebSocket para clientes móviles Android (Integrante 1).
    Soporta autenticación de sesión, mensaje de bienvenida interactivo,
    recepción masiva de escaneos RSSI y distribución instantánea de ubicación.
    """
    ws = web.WebSocketResponse(heartbeat=CONFIG.HEARTBEAT_INTERVAL_SECONDS)
    await ws.prepare(request)

    conn_manager: ConnectionManager = request.app["conn_manager"]
    session_manager: SessionManager = request.app["session_manager"]
    dispatcher: ProcessingDispatcher = request.app["dispatcher"]
    access_events: AccessEventsManager = request.app["access_events"]
    ap_manager: APManager = request.app["ap_manager"]

    current_device_id = None
    current_session = None

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                conn_manager.messages_received += 1
                try:
                    data = json.loads(msg.data)
                except Exception:
                    await ws.send_json({"type": "error", "message": "JSON inválido"})
                    continue

                action = data.get("action") or data.get("type")

                # ================= 1. REGISTRO / CONEXIÓN DEL MÓVIL =================
                if action == "connect" or action == "register":
                    device_id = data.get("device_id")
                    if not device_id:
                        await ws.send_json({"type": "error", "message": "device_id requerido"})
                        continue

                    # Registrar en el gestor de concurrencia
                    ok = await conn_manager.register_device(device_id, ws)
                    if not ok:
                        await ws.send_json({
                            "type": "error",
                            "message": f"Servidor a capacidad máxima ({CONFIG.MAX_CONCURRENT_DEVICES} dispositivos)"
                        })
                        await ws.close(code=1008, message=b"Max capacity reached")
                        break

                    current_device_id = device_id
                    current_session = await session_manager.create_or_resume_session(
                        device_id=device_id,
                        mac_address=data.get("mac_address"),
                        patient_name=data.get("patient_name"),
                        device_model=data.get("device_model"),
                        ip_address=request.remote
                    )

                    # Enviar acuse de conexión y el MENSAJE DE BIENVENIDA
                    await ws.send_json({
                        "type": "connected",
                        "device_id": current_device_id,
                        "session_id": current_session.session_id,
                        "status": current_session.status.value,
                        "welcome_prompt": {
                            "title": CONFIG.WELCOME_MESSAGE_TITLE,
                            "body": CONFIG.WELCOME_MESSAGE_BODY,
                            "pending_response": current_session.welcome_accepted is None
                        }
                    })

                # ================= 2. RESPUESTA AL MENSAJE DE BIENVENIDA =================
                elif action == "welcome_response":
                    if not current_session:
                        await ws.send_json({"type": "error", "message": "Debe registrarse primero"})
                        continue

                    accepted = bool(data.get("accepted", True))
                    current_session = await session_manager.handle_welcome_response(
                        current_session.session_id, accepted
                    )
                    await ws.send_json({
                        "type": "welcome_acknowledged",
                        "accepted": accepted,
                        "session_status": current_session.status.value,
                        "message": "Servicio de posicionamiento habilitado" if accepted else "Posicionamiento en espera"
                    })

                # ================= 3. RECEPCIÓN DE MEDICIONES RSSI =================
                elif action in ("rssi_batch", "telemetry"):
                    if not current_device_id or not current_session:
                        await ws.send_json({"type": "error", "message": "Dispositivo no autenticado"})
                        continue

                    data["device_id"] = current_device_id
                    data["session_id"] = current_session.session_id
                    batch = RSSIBatch.from_dict(data)

                    # Verificar presencia en APs para eventos de ingreso/roaming
                    nearest_ap = ap_manager.find_nearest_ap_from_scan(batch.measurements)
                    await access_events.check_telemetry_access(current_device_id, current_session, nearest_ap)

                    # Enviar a la cola de procesamiento concurrente
                    await dispatcher.submit_batch(batch)

                # ================= 4. HEARTBEAT / PING =================
                elif action in ("ping", "heartbeat"):
                    if current_device_id:
                        await session_manager.update_activity(current_device_id)
                    await ws.send_json({"type": "pong", "timestamp": data.get("timestamp")})

                else:
                    await ws.send_json({"type": "warning", "message": f"Acción desconocida: {action}"})

            elif msg.type == WSMsgType.ERROR:
                logger.warning(f"Error en WebSocket de dispositivo {current_device_id}: {ws.exception()}")

    finally:
        await conn_manager.unregister_device(ws)
        if current_session:
            await session_manager.close_session(current_session.session_id, reason="WS client disconnected")

    return ws

@routes.get("/ws/dashboard")
async def ws_dashboard_handler(request: web.Request):
    """
    WebSocket para el Dashboard Administrativo (Integrante 6).
    Transmite métricas, eventos de ingreso/salida y ubicaciones en tiempo real.
    """
    ws = web.WebSocketResponse(heartbeat=15)
    await ws.prepare(request)

    conn_manager: ConnectionManager = request.app["conn_manager"]
    session_manager: SessionManager = request.app["session_manager"]
    await conn_manager.register_dashboard(ws)

    # Enviar estado inicial inmediato al dashboard
    sessions = session_manager.get_all_active_sessions()
    await ws.send_json({
        "event": "init_state",
        "data": {
            "active_devices": [s.to_dict() for s in sessions],
            "metrics": conn_manager.get_metrics()
        }
    })

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                # Dashboard puede solicitar refresco o enviar comandos
                data = json.loads(msg.data)
                if data.get("action") == "refresh":
                    sessions = session_manager.get_all_active_sessions()
                    await ws.send_json({
                        "event": "refresh_data",
                        "data": {
                            "active_devices": [s.to_dict() for s in sessions],
                            "metrics": conn_manager.get_metrics()
                        }
                    })
            elif msg.type == WSMsgType.ERROR:
                pass
    finally:
        await conn_manager.unregister_dashboard(ws)

    return ws


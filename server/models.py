# models.py - Modelos de datos del Servidor Central IPS
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional, Dict, Any
import uuid

class SessionStatus(str, Enum):
    CONNECTED = "CONNECTED"
    WELCOME_PENDING = "WELCOME_PENDING"
    ACTIVE = "ACTIVE"          # El usuario aceptó el guiado
    OPTED_OUT = "OPTED_OUT"    # El usuario rechazó el guiado
    DISCONNECTED = "DISCONNECTED"
    EXPIRED = "EXPIRED"

class AccessEventType(str, Enum):
    INGRESS = "INGRESS"        # Ingreso a la infraestructura hospitalaria
    EGRESS = "EGRESS"          # Salida de la infraestructura hospitalaria
    ROAMING = "ROAMING"        # Cambio de Punto de Acceso (AP)
    RECONNECT = "RECONNECT"    # Reconexión tras desconexión temporal

class APStatus(str, Enum):
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"
    DEGRADED = "DEGRADED"

@dataclass
class RSSIMeasurement:
    bssid: str
    ssid: str
    rssi_dbm: float
    frecuencia_mhz: int
    timestamp_ms: int
    edad_s: float = 0.0
    fuente: str = "scan"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RSSIMeasurement":
        return cls(
            bssid=data.get("bssid", "").lower().strip(),
            ssid=data.get("ssid", "").strip(),
            rssi_dbm=float(data.get("rssi_dbm", data.get("rssi", -100))),
            frecuencia_mhz=int(data.get("frecuencia_mhz", data.get("frequency", 2412))),
            timestamp_ms=int(data.get("timestamp_ms", int(datetime.now(timezone.utc).timestamp() * 1000))),
            edad_s=float(data.get("edad_s", 0.0)),
            fuente=data.get("fuente", "scan")
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class RSSIBatch:
    device_id: str
    session_id: str
    measurements: List[RSSIMeasurement]
    client_timestamp_ms: int
    server_timestamp_ms: int = field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000))

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RSSIBatch":
        measurements = [
            RSSIMeasurement.from_dict(m) for m in data.get("measurements", [])
        ]
        return cls(
            device_id=data["device_id"],
            session_id=data.get("session_id", ""),
            measurements=measurements,
            client_timestamp_ms=int(data.get("client_timestamp_ms", int(datetime.now(timezone.utc).timestamp() * 1000))),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "device_id": self.device_id,
            "session_id": self.session_id,
            "client_timestamp_ms": self.client_timestamp_ms,
            "server_timestamp_ms": self.server_timestamp_ms,
            "count": len(self.measurements),
            "measurements": [m.to_dict() for m in self.measurements]
        }

@dataclass
class LocationResult:
    device_id: str
    session_id: str
    piso: str                       # Ej. "Piso 3" o "3"
    area: str                       # Ej. "Triaje", "Sala 302", "Pabellón A"
    lugar_id: str                   # Código de calibración, ej. "302_8"
    confidence: float               # Nivel de confianza [0.0 - 1.0]
    timestamp_ms: int
    coordinates: Optional[Dict[str, float]] = None  # {"x": 12.5, "y": 8.0}
    algorithm_info: Optional[Dict[str, Any]] = None # {"k": 3, "method": "knn-weighted"}
    navigation_message: Optional[str] = None       # Mensaje orientativo para el paciente

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class DeviceSession:
    session_id: str
    device_id: str                  # Identificador único de hardware o UUID del móvil
    mac_address: Optional[str]      # MAC Wi-Fi del dispositivo
    patient_name: Optional[str]     # Nombre o código de paciente (ej. "Paciente #1042")
    device_model: Optional[str]     # Ej. "Samsung Galaxy A54", "Redmi Note 14"
    status: SessionStatus
    connected_at: str
    last_seen_at: str
    last_ap_bssid: Optional[str] = None
    last_location: Optional[LocationResult] = None
    welcome_accepted: Optional[bool] = None
    welcome_response_at: Optional[str] = None
    ip_address: Optional[str] = None
    measurements_count: int = 0

    @classmethod
    def create_new(cls, device_id: str, mac_address: Optional[str] = None,
                   patient_name: Optional[str] = None, device_model: Optional[str] = None,
                   ip_address: Optional[str] = None) -> "DeviceSession":
        now_str = datetime.now(timezone.utc).isoformat()
        return cls(
            session_id=str(uuid.uuid4()),
            device_id=device_id,
            mac_address=mac_address.lower() if mac_address else None,
            patient_name=patient_name or f"Paciente_{device_id[:6]}",
            device_model=device_model or "Android Device",
            status=SessionStatus.WELCOME_PENDING,
            connected_at=now_str,
            last_seen_at=now_str,
            ip_address=ip_address
        )

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if self.last_location:
            d["last_location"] = self.last_location.to_dict()
        return d

@dataclass
class AccessPoint:
    bssid: str                      # MAC del AP (clave primaria)
    ssid: str
    piso: str                       # "Piso 1", "Piso 2", "Piso 3"
    area: str                       # "Entrada Principal", "Triaje", "Sala 302"
    ip_address: Optional[str] = None
    channel: Optional[int] = None
    status: APStatus = APStatus.ONLINE
    last_ping_at: Optional[str] = None
    connected_clients_count: int = 0
    is_ingress_ap: bool = False     # ¿Es un AP ubicado en puertas/entradas del hospital?

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class AccessEvent:
    event_id: str
    event_type: AccessEventType
    device_id: str
    session_id: Optional[str]
    ap_bssid: str
    ap_ssid: Optional[str]
    piso: str
    area: str
    timestamp: str
    details: Optional[Dict[str, Any]] = None

    @classmethod
    def create(cls, event_type: AccessEventType, device_id: str, ap_bssid: str,
               piso: str = "", area: str = "", session_id: Optional[str] = None,
               ap_ssid: Optional[str] = None, details: Optional[Dict[str, Any]] = None) -> "AccessEvent":
        return cls(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            device_id=device_id,
            session_id=session_id,
            ap_bssid=ap_bssid.lower(),
            ap_ssid=ap_ssid,
            piso=piso,
            area=area,
            timestamp=datetime.now(timezone.utc).isoformat(),
            details=details or {}
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


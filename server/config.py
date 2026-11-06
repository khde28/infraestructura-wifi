# config.py - Configuración central del Servidor IPS (Integrante 2)
import os
from dataclasses import dataclass

@dataclass
class ServerConfig:
    # Parámetros del servidor HTTP y WebSocket
    HOST: str = os.getenv("IPS_HOST", "0.0.0.0")
    PORT: int = int(os.getenv("IPS_PORT", "8080"))
    
    # Capacidad y Concurrencia (Requisito: ~100 dispositivos)
    MAX_CONCURRENT_DEVICES: int = int(os.getenv("IPS_MAX_DEVICES", "100"))
    CONNECTION_TIMEOUT_SECONDS: int = int(os.getenv("IPS_TIMEOUT_SEC", "45"))
    HEARTBEAT_INTERVAL_SECONDS: int = int(os.getenv("IPS_HEARTBEAT_SEC", "15"))
    CLEANUP_INTERVAL_SECONDS: int = int(os.getenv("IPS_CLEANUP_SEC", "10"))
    
    # Infraestructura Wi-Fi
    SYSLOG_ENABLED: bool = os.getenv("IPS_SYSLOG_ENABLED", "true").lower() == "true"
    SYSLOG_HOST: str = os.getenv("IPS_SYSLOG_HOST", "0.0.0.0")
    SYSLOG_PORT: int = int(os.getenv("IPS_SYSLOG_PORT", "1514"))  # Puerto UDP no privilegiado
    AP_HEALTH_CHECK_INTERVAL: int = int(os.getenv("IPS_AP_CHECK_SEC", "30"))
    
    # Base de datos (SQLite por defecto para integración con Integrante 3)
    DATABASE_PATH: str = os.getenv("IPS_DB_PATH", "hospital_ips.db")
    
    # Módulos de Procesamiento (Integrantes 4 y 5)
    EXTERNAL_PROCESSING_URL: str = os.getenv("IPS_PROCESSING_URL", "")  # URL externa si está activo
    ENABLE_INTERNAL_PROCESSING: bool = True  # Motor de procesamiento integrado con fingerprinting
    
    # Mensaje de bienvenida hospitalario para nuevos ingresos
    WELCOME_MESSAGE_TITLE: str = "Bienvenido al Hospital San Juan de Dios"
    WELCOME_MESSAGE_BODY: str = (
        "Se ha detectado su ingreso a las instalaciones. El sistema de guiado en interiores "
        "le ayudará a llegar a su destino (Triaje, Consultorios, Laboratorio). ¿Desea activar el guiado?"
    )

CONFIG = ServerConfig()


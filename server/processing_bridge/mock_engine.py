# processing_bridge/mock_engine.py - Motor de Procesamiento y Estimación IPS
import math
import logging
from collections import deque
from typing import Dict, List, Optional, Tuple, Any
from server.models import RSSIMeasurement, LocationResult
from server.database.db_adapter import DatabaseAdapter

logger = logging.getLogger("IPS.ProcessingEngine")

class FingerprintProcessingEngine:
    """
    Motor de procesamiento inicial (k-NN) y estabilización temporal.
    Cumple con el procesamiento de referencia mientras los Integrantes 4 y 5
    despliegan sus módulos finales, o actúa como motor autónomo del servidor.
    """
    def __init__(self, db: DatabaseAdapter, k: int = 3, default_penalty_rssi: float = -100.0):
        self.db = db
        self.k = k
        self.default_penalty_rssi = default_penalty_rssi
        
        # Huellas de referencia cargadas en memoria: {lugar_id: {"piso": str, "area": str, "signals": {bssid: avg_rssi}}}
        self.reference_fingerprints: Dict[str, Dict[str, Any]] = {}
        
        # Historial de estimaciones recientes para filtro temporal de saltos: device_id -> deque([LocationResult])
        self._location_history_window: Dict[str, deque] = {}
        self.window_size = 4

    async def initialize_from_db(self):
        """Carga las huellas de calibración almacenadas en base de datos."""
        self.reference_fingerprints = await self.db.get_calibration_fingerprints()
        logger.info(f"Motor de procesamiento cargó {len(self.reference_fingerprints)} puntos de referencia de fingerprinting.")

    def set_reference_fingerprints(self, fingerprints: Dict[str, Dict[str, Any]]):
        self.reference_fingerprints = fingerprints

    def estimate_location(self, device_id: str, session_id: str,
                          measurements: List[RSSIMeasurement]) -> LocationResult:
        """
        Ejecuta el algoritmo k-NN sobre las mediciones RSSI recibidas del celular.
        """
        if not measurements or not self.reference_fingerprints:
            # Estimación por defecto si no hay datos suficientes
            return LocationResult(
                device_id=device_id,
                session_id=session_id,
                piso="Piso 1",
                area="Recepción / Entrada Principal",
                lugar_id="default_entry",
                confidence=0.3,
                timestamp_ms=measurements[0].timestamp_ms if measurements else 0,
                navigation_message="Señal baja. Espere mientras se calibra su ubicación."
            )

        # Vector de escaneo actual del dispositivo: {bssid: rssi}
        current_vector: Dict[str, float] = {
            m.bssid.lower(): m.rssi_dbm for m in measurements
        }

        # Calcular distancias euclidianas a cada punto de referencia
        distances: List[Tuple[float, str, Dict[str, Any]]] = []

        for lugar_id, fp in self.reference_fingerprints.items():
            ref_signals = fp["signals"]
            # Cojunto de todos los BSSIDs entre el punto de referencia y el escaneo actual
            all_bssids = set(ref_signals.keys()).union(set(current_vector.keys()))
            
            sq_sum = 0.0
            matched_bssids = 0
            for bssid in all_bssids:
                curr_val = current_vector.get(bssid, self.default_penalty_rssi)
                ref_val = ref_signals.get(bssid, self.default_penalty_rssi)
                if bssid in current_vector and bssid in ref_signals:
                    matched_bssids += 1
                sq_sum += (curr_val - ref_val) ** 2

            # Penalización si pocos BSSIDs coinciden
            euclidean_dist = math.sqrt(sq_sum)
            if matched_bssids < 2:
                euclidean_dist += 20.0

            distances.append((euclidean_dist, lugar_id, fp))

        # Ordenar por menor distancia euclidiana
        distances.sort(key=lambda x: x[0])
        top_k = distances[:self.k]

        # Votación ponderada o el más cercano
        best_match = top_k[0]
        min_dist = best_match[0]
        fp_data = best_match[2]

        lugar_id = best_match[1]
        piso = fp_data.get("piso", "Piso 1")
        area = fp_data.get("area", "Área Hospitalaria")

        # Cálculo de confianza [0.0, 1.0] inversamente proporcional a la distancia
        confidence = max(0.2, min(0.98, round(1.0 - (min_dist / 150.0), 2)))

        # Generar mensaje de orientación amigable para el paciente
        nav_msg = self._generate_navigation_message(piso, area)

        result = LocationResult(
            device_id=device_id,
            session_id=session_id,
            piso=piso,
            area=area,
            lugar_id=lugar_id,
            confidence=confidence,
            timestamp_ms=measurements[0].timestamp_ms,
            algorithm_info={
                "method": "weighted-knn",
                "k": len(top_k),
                "min_distance": round(min_dist, 2),
                "matched_points": [d[1] for d in top_k]
            },
            navigation_message=nav_msg
        )

        # Aplicar filtro temporal de estabilización
        stabilized = self._apply_temporal_filter(device_id, result)
        return stabilized

    def _apply_temporal_filter(self, device_id: str, current: LocationResult) -> LocationResult:
        """Filtro de ventana deslizante para evitar oscilaciones o saltos bruscos de piso/área."""
        if device_id not in self._location_history_window:
            self._location_history_window[device_id] = deque(maxlen=self.window_size)

        window = self._location_history_window[device_id]
        window.append(current)

        if len(window) < 2:
            return current

        # Frecuencia de áreas en la ventana reciente
        area_counts: Dict[str, int] = {}
        for item in window:
            area_counts[item.area] = area_counts.get(item.area, 0) + 1

        # Encontrar área predominante
        dominant_area = max(area_counts.items(), key=lambda x: x[1])[0]
        if dominant_area != current.area and area_counts[dominant_area] >= 2:
            # Mantener el área predominante reciente para estabilizar
            for item in reversed(window):
                if item.area == dominant_area:
                    current.area = dominant_area
                    current.piso = item.piso
                    current.confidence = round(current.confidence * 0.9, 2)
                    break

        return current

    def _generate_navigation_message(self, piso: str, area: str) -> str:
        if "3" in piso:
            return f"Usted se encuentra en {area} ({piso}). Triaje se encuentra en Piso 1. Para bajar use el elevador central."
        elif "2" in piso:
            return f"Usted se encuentra en {area} ({piso}). Cerca a Consultorios de Especialidades."
        else:
            return f"Usted se encuentra en {area} ({piso}). Siga las señales azules para Admisión y Triaje."


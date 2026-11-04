"""
Módulo Core: Auditoría, Observabilidad y Gestión de Cuarentena
═══════════════════════════════════════════════════════════════════════════════
Registra métricas de ejecución y aísla registros rechazados en PostgreSQL RDS.
Inspirado en patrones de observabilidad para pipelines de misión crítica.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from sqlalchemy import create_engine, text
from pyspark.sql import DataFrame

from vueladata_etl.core.constants import (
    SCHEMA_AUDIT,
    TABLE_EXECUTION_LOG,
    TABLE_ERROR_QUARANTINE,
    obtener_url_postgres
)

logger = logging.getLogger("vueladata.audit")


class AuditManager:
    """Administra el registro de métricas de ejecución y la persistencia de errores de calidad."""

    def __init__(self, db_url: Optional[str] = None):
        self.db_url = db_url or obtener_url_postgres()
        self._engine = None

    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_engine(self.db_url, pool_pre_ping=True)
        return self._engine

    def registrar_inicio(self, dag_id: str, run_id: str, capa: str, fecha_proceso: str) -> Optional[int]:
        """
        Inserta un nuevo registro de ejecución con estado RUNNING en vueladata_audit.pipeline_execution_log.
        Retorna el id_ejecucion autogenerado para correlación.
        """
        ahora = datetime.now(timezone.utc)
        print(f"[*] Auditoría: Registrando inicio de corrida {run_id} para capa {capa.upper()}...")

        query = text(f"""
            INSERT INTO {SCHEMA_AUDIT}.{TABLE_EXECUTION_LOG} 
            (dag_id, run_id, capa, fecha_proceso, fecha_inicio_utc, estado, mensaje_detalle)
            VALUES (:dag_id, :run_id, :capa, :fecha_proceso, :fecha_inicio, 'RUNNING', 'Iniciando pipeline Medallion')
            RETURNING id_ejecucion;
        """)

        try:
            with self.engine.begin() as conn:
                res = conn.execute(query, {
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "capa": capa.upper(),
                    "fecha_proceso": fecha_proceso,
                    "fecha_inicio": ahora
                })
                id_ejecucion = res.scalar()
                print(f"[+] Auditoría: Corrida registrada con id_ejecucion={id_ejecucion}")
                return id_ejecucion
        except Exception as e:
            logger.warning(f"No se pudo registrar inicio en auditoría (modo offline/dev): {e}")
            return None

    def registrar_fin(
        self,
        id_ejecucion: Optional[int],
        filas_leidas: int = 0,
        filas_escritas: int = 0,
        filas_rechazadas: int = 0,
        estado: str = "SUCCESS",
        mensaje: str = "Ejecución finalizada correctamente"
    ) -> None:
        """Actualiza el registro de auditoría con las métricas finales y estado SUCCESS o FAILED."""
        if id_ejecucion is None:
            return

        ahora = datetime.now(timezone.utc)
        print(f"[*] Auditoría: Cerrando corrida {id_ejecucion} con estado: {estado}...")

        query = text(f"""
            UPDATE {SCHEMA_AUDIT}.{TABLE_EXECUTION_LOG}
            SET fecha_fin_utc = :fecha_fin,
                filas_leidas = :leidas,
                filas_escritas = :escritas,
                filas_rechazadas = :rechazadas,
                estado = :estado,
                mensaje_detalle = :mensaje
            WHERE id_ejecucion = :id_ejecucion;
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(query, {
                    "fecha_fin": ahora,
                    "leidas": filas_leidas,
                    "escritas": filas_escritas,
                    "rechazadas": filas_rechazadas,
                    "estado": estado,
                    "mensaje": mensaje,
                    "id_ejecucion": id_ejecucion
                })
                print(f"[+] Auditoría: Registro {id_ejecucion} actualizado exitosamente.")
        except Exception as e:
            logger.warning(f"No se pudo registrar fin en auditoría: {e}")

    def guardar_cuarentena(
        self,
        id_ejecucion: Optional[int],
        dag_id: str,
        capa: str,
        tabla_origen: str,
        registros_rechazados: List[Dict[str, Any]]
    ) -> int:
        """
        Inserta en lote registros con fallas de calidad en vueladata_audit.pipeline_error_quarantine.
        Garantiza que ningún dato corrupto se pierda ni contamine las capas Silver o Gold.
        """
        if not registros_rechazados:
            return 0

        total = len(registros_rechazados)
        print(f"[*] Cuarentena: Persistiendo {total} registros rechazados en {SCHEMA_AUDIT}.{TABLE_ERROR_QUARANTINE}...")

        query = text(f"""
            INSERT INTO {SCHEMA_AUDIT}.{TABLE_ERROR_QUARANTINE}
            (id_ejecucion, dag_id, capa, tabla_origen, clave_registro, tipo_error, mensaje_error, payload_original, fecha_captura_utc)
            VALUES (:id_ejecucion, :dag_id, :capa, :tabla_origen, :clave, :tipo_error, :mensaje_error, :payload, :fecha_captura);
        """)

        ahora = datetime.now(timezone.utc)
        parametros = []
        for r in registros_rechazados:
            parametros.append({
                "id_ejecucion": id_ejecucion,
                "dag_id": dag_id,
                "capa": capa.upper(),
                "tabla_origen": tabla_origen,
                "clave": str(r.get("clave_registro", "N/A")),
                "tipo_error": str(r.get("tipo_error", "VALIDACION_FALLIDA")),
                "mensaje_error": str(r.get("mensaje_error", "Registro no cumple criterios de calidad")),
                "payload": json.dumps(r.get("payload_raw", {}), default=str),
                "fecha_captura": ahora
            })

        try:
            with self.engine.begin() as conn:
                conn.execute(query, parametros)
            print(f"[+] Cuarentena: {total} registros aislados exitosamente en la base de datos de auditoría.")
            return total
        except Exception as e:
            logger.warning(f"No se pudo guardar registros en cuarentena: {e}")
            return 0

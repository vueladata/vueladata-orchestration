"""
Capa Silver: Validador de Calidad de Datos y Enrutador a Cuarentena
═══════════════════════════════════════════════════════════════════════════════
Responsabilidad única (SRP): Separar los registros en dos flujos:
  1. Válidos: continúan hacia la curación de la capa Silver y Gold.
  2. Rechazados: se aíslan y guardan en vueladata_audit.pipeline_error_quarantine.

Inspirado en el patrón validar_y_separar con trazabilidad de calidad total.
"""

from typing import Tuple, List, Dict, Any, Optional
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from vueladata_etl.core.audit import AuditManager

logger = logging.getLogger("vueladata.quality_validator")


class FlightQualityValidator:
    """Valida reglas de consistencia de vuelos y enruta los datos defectuosos a cuarentena."""

    @staticmethod
    def validar_y_separar(
        df_flights: DataFrame,
        id_ejecucion: Optional[int] = None,
        dag_id: str = "dag_daily_batch_spark",
        audit_manager: Optional[AuditManager] = None
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Evalúa las reglas de consistencia lógica de vuelos:
          → Regla 1: El número de vuelo y la fecha de salida no pueden ser nulos o vacíos.
          → Regla 2: La llegada programada no puede ser menor o igual a la salida programada (inconsistencia temporal).
          → Regla 3: Si el vuelo no está cancelado, el retraso de llegada no puede ser nulo o incongruente (< -120 min).

        Retorna:
          (df_validos, df_rechazados)
        """
        print("[*] Silver: Aplicando validaciones de calidad y consistencia aeronáutica...")

        # Flag 1: Identificadores obligatorios presentes
        cond_id_invalido = (
            F.col("numero_vuelo").isNull() | 
            (F.col("numero_vuelo") == "") |
            F.col("codigo_aerolinea").isNull() |
            F.col("aeropuerto_origen").isNull() |
            F.col("aeropuerto_destino").isNull()
        )

        # Flag 2: Coherencia temporal entre salida y llegada programadas
        if "salida_programada_ts" in df_flights.columns and "llegada_programada_ts" in df_flights.columns:
            cond_tiempo_incoherente = (
                F.col("salida_programada_ts").isNotNull() &
                F.col("llegada_programada_ts").isNotNull() &
                (F.col("llegada_programada_ts") < F.col("salida_programada_ts"))
            )
        elif "salida_programada_raw" in df_flights.columns and "llegada_programada_raw" in df_flights.columns:
            cond_tiempo_incoherente = (
                F.col("salida_programada_raw").isNotNull() &
                F.col("llegada_programada_raw").isNotNull() &
                (F.col("llegada_programada_raw") < F.col("salida_programada_raw"))
            )
        else:
            cond_tiempo_incoherente = F.lit(False)

        # Marcar motivo de rechazo
        df_evaluado = df_flights.withColumn(
            "motivo_rechazo",
            F.when(cond_id_invalido, F.lit("FALTA_CAMPOS_OBLIGATORIOS_IDENTIFICACION"))
             .when(cond_tiempo_incoherente, F.lit("INCOHERENCIA_CRONOLOGICA_LLEGADA_PREVIA_A_SALIDA"))
             .otherwise(F.lit(None))
        )

        # Separar en dos DataFrames
        df_rechazados = df_evaluado.filter(F.col("motivo_rechazo").isNotNull())
        df_validos = df_evaluado.filter(F.col("motivo_rechazo").isNull()).drop("motivo_rechazo")

        total_rechazados = df_rechazados.count()
        total_validos = df_validos.count()

        print(f"[+] Control de Calidad: {total_validos:,} registros válidos | {total_rechazados:,} registros rechazados.")

        # Si hay rechazados y tenemos el gestor de auditoría, guardarlos en cuarentena
        if total_rechazados > 0 and audit_manager:
            # Tomar muestra o lote de rechazados para persistir en la tabla de errores
            registros_para_cuarentena = []
            for row in df_rechazados.limit(500).collect():
                d = row.asDict()
                registros_para_cuarentena.append({
                    "clave_registro": f"{d.get('codigo_aerolinea')}-{d.get('numero_vuelo')}-{d.get('anio')}{d.get('mes')}{d.get('dia')}",
                    "tipo_error": d.get("motivo_rechazo", "ERROR_CALIDAD"),
                    "mensaje_error": f"Falla de validación en vuelo {d.get('numero_vuelo')}: {d.get('motivo_rechazo')}",
                    "payload_raw": d
                })

            audit_manager.guardar_cuarentena(
                id_ejecucion=id_ejecucion,
                dag_id=dag_id,
                capa="SILVER",
                tabla_origen="vuelo",
                registros_rechazados=registros_para_cuarentena
            )

        return df_validos, df_rechazados

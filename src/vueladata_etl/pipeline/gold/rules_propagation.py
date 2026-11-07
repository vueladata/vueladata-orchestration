"""
Capa Gold: Motor de Reglas de Negocio - RN-08 (Propagación de Retrasos en Flota)
Responsabilidad única (SRP): Secuenciar itinerarios por aeronave y calcular efecto dominó restando tiempo de rotación.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from vueladata_etl.core.constants import (
    TURNAROUND_MINUTES_NACIONAL,
    TURNAROUND_MINUTES_INTERNACIONAL
)


class DelayPropagationEngine:
    """Implementa el análisis de propagación en cadena de retrasos aeronáuticos por matrícula."""

    @staticmethod
    def calculate_propagation(df_flights: DataFrame) -> DataFrame:
        """
        Aplica la regla RN-08:
        Evalúa vuelos consecutivos de una misma aeronave considerando el tiempo mínimo de rotación
        (45 min vuelos nacionales, 60 min vuelos internacionales).
        """
        df_flota = df_flights
        if "matricula_aeronave" not in df_flota.columns:
            return df_flota

        # Filtrar solo vuelos con matrícula asignada
        df_flota = df_flota.filter(
            F.col("matricula_aeronave").isNotNull() & (F.col("matricula_aeronave") != "")
        )

        if "tipo_vuelo" not in df_flota.columns:
            df_flota = df_flota.withColumn("tipo_vuelo", F.lit("NACIONAL"))

        if "retraso_llegada_min" not in df_flota.columns:
            df_flota = df_flota.withColumn("retraso_llegada_min", F.lit(0.0))

        if "salida_programada_raw" not in df_flota.columns:
            df_flota = df_flota.withColumn("salida_programada_raw", F.lit("0000"))

        # Asignar tiempo de rotación requerido según tipo de vuelo
        df_flota = df_flota.withColumn(
            "minutos_minimos_rotacion",
            F.when(
                F.col("tipo_vuelo") == "INTERNACIONAL",
                TURNAROUND_MINUTES_INTERNACIONAL
            ).otherwise(TURNAROUND_MINUTES_NACIONAL)
        )

        # Ventana analítica ordenada por aeronave y cronología
        w_aircraft = Window.partitionBy("matricula_aeronave", "anio", "mes", "dia") \
            .orderBy("salida_programada_raw", "numero_vuelo")

        df_secuenciado = df_flota \
            .withColumn("prev_numero_vuelo", F.lag("numero_vuelo", 1).over(w_aircraft)) \
            .withColumn("prev_retraso_llegada", F.lag("retraso_llegada_min", 1).over(w_aircraft))

        # Cálculo del retraso propagado restando buffer de rotación
        df_propagado = df_secuenciado.withColumn(
            "retraso_heredado_previo",
            F.coalesce(F.col("prev_retraso_llegada"), F.lit(0.0))
        ).withColumn(
            "retraso_propagado_estimado",
            F.when(
                F.col("retraso_heredado_previo") > 0,
                F.greatest(
                    F.lit(0.0),
                    F.col("retraso_heredado_previo") - (F.col("minutos_minimos_rotacion") * 0.25)
                )
            ).otherwise(0.0)
        ).withColumn(
            "es_retraso_propagado",
            F.when(F.col("retraso_propagado_estimado") > 15.0, 1).otherwise(0)
        )

        return df_propagado

    @staticmethod
    def aggregate_propagation_kpi(df_propagado: DataFrame) -> DataFrame:
        """Genera métricas analíticas Gold de vulnerabilidad y efecto dominó por aeronave."""
        return df_propagado.groupBy("anio", "mes", "matricula_aeronave") \
            .agg(
                F.count("*").alias("total_vuelos_operados"),
                F.sum("es_retraso_propagado").alias("total_vuelos_con_retraso_propagado"),
                F.avg("retraso_propagado_estimado").alias("retraso_propagado_promedio_min")
            ) \
            .withColumn(
                "tasa_vulnerabilidad_rotacion_pct",
                F.round((F.col("total_vuelos_con_retraso_propagado") / F.when(F.col("total_vuelos_operados") > 0, F.col("total_vuelos_operados")).otherwise(1)) * 100, 2)
            ) \
            .withColumn("fecha_actualizacion_utc", F.current_timestamp())

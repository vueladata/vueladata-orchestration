"""
Capa Gold: Motor de Reglas de Negocio - Conexiones Críticas en Hubs
═══════════════════════════════════════════════════════════════════════════════
Responsabilidad única (SRP): Evaluar el impacto de las demoras de llegada sobre
la ventana mínima de conexión (MCT) en aeropuertos centro de conexiones (HU-31).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from vueladata_etl.core.constants import HUBS_AEROPUERTOS


class ConnectionsRuleEngine:
    """Modela y cuantifica las conexiones de pasajeros perdidas en aeropuertos Hub."""

    @staticmethod
    def aggregate_kpi_conexiones(df_flights: DataFrame) -> DataFrame:
        """
        Evalúa vuelos que alimentan un aeropuerto Hub:
        Si el retraso de llegada supera el tiempo mínimo de conexión (45 min) o se cancela,
        la conexión se clasifica como PERDIDA.
        """
        hubs_keys = list(HUBS_AEROPUERTOS.keys())

        df = df_flights
        if "retraso_llegada_min" not in df.columns:
            df = df.withColumn("retraso_llegada_min", F.lit(0.0).cast(DoubleType()))
        if "cancelado" not in df.columns:
            df = df.withColumn("cancelado", F.lit(0))

        # Filtrar vuelos que arriban a un Hub
        df_hubs = df.filter(F.col("aeropuerto_destino").isin(hubs_keys)) \
            .withColumn("retraso_llegada_val", F.coalesce(F.col("retraso_llegada_min").cast(DoubleType()), F.lit(0.0)))

        # Clasificación de la conexión
        df_hubs = df_hubs.withColumn(
            "aeropuerto_hub", F.col("aeropuerto_destino")
        ).withColumn(
            "es_conexion_perdida",
            F.when((F.col("cancelado") == 1) | (F.col("retraso_llegada_val") > 45.0), 1).otherwise(0)
        ).withColumn(
            "minutos_escala_disponible",
            F.greatest(F.lit(0.0), F.lit(90.0) - F.col("retraso_llegada_val"))  # 90 min de escala estándar programada
        )

        kpi = df_hubs.groupBy("anio", "mes", "aeropuerto_hub") \
            .agg(
                F.count("*").alias("total_conexiones_programadas"),
                F.sum(F.when(F.col("es_conexion_perdida") == 0, 1).otherwise(0)).alias("total_conexiones_exitosas"),
                F.sum("es_conexion_perdida").alias("total_conexiones_perdidas"),
                F.round(F.avg("minutos_escala_disponible"), 2).alias("promedio_minutos_escala_disponible")
            ) \
            .withColumn(
                "tasa_perdida_conexiones_pct",
                F.when(
                    F.col("total_conexiones_programadas") > 0,
                    F.round((F.col("total_conexiones_perdidas") / F.col("total_conexiones_programadas")) * 100, 2)
                ).otherwise(0.0)
            ) \
            .withColumn("fecha_actualizacion_utc", F.current_timestamp())

        return kpi

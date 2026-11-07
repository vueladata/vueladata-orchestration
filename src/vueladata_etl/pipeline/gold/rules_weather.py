"""
Capa Gold: Motor de Reglas de Negocio - Impacto Meteorológico por Aeropuerto
═══════════════════════════════════════════════════════════════════════════════
Responsabilidad única (SRP): Consolidar los retrasos y afectaciones por condiciones
climáticas adversas a nivel de aeropuerto de origen (HU-31).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


class WeatherRuleEngine:
    """Modela el impacto de eventos meteorológicos adversos en la puntualidad aérea."""

    @staticmethod
    def aggregate_kpi_clima(df_flights: DataFrame) -> DataFrame:
        """
        Agrega las afectaciones meteorológicas por aeropuerto de origen:
        Evalúa minutos de demora por clima y determina los aeropuertos más vulnerables.
        """
        df = df_flights
        if "retraso_clima_min" in df.columns:
            df = df.withColumn("retraso_clima_val", F.coalesce(F.col("retraso_clima_min").cast(DoubleType()), F.lit(0.0)))
        else:
            df = df.withColumn("retraso_clima_val", F.lit(0.0).cast(DoubleType()))

        df = df.withColumn(
            "es_afectado_clima",
            F.when(F.col("retraso_clima_val") > 0.0, 1).otherwise(0)
        )

        kpi = df.groupBy("anio", "mes", "aeropuerto_origen") \
            .agg(
                F.count("*").alias("total_vuelos_programados"),
                F.sum("es_afectado_clima").alias("total_vuelos_afectados_clima"),
                F.round(F.sum("retraso_clima_val"), 2).alias("total_minutos_retraso_clima"),
                F.round(F.avg(F.when(F.col("es_afectado_clima") == 1, F.col("retraso_clima_val")).otherwise(None)), 2).alias("promedio_minutos_retraso_clima")
            ) \
            .filter(F.col("total_vuelos_afectados_clima") > 0) \
            .withColumn(
                "tasa_afectacion_clima_pct",
                F.round((F.col("total_vuelos_afectados_clima") / F.col("total_vuelos_programados")) * 100, 2)
            ) \
            .withColumn(
                "condicion_climatica_predominante",
                F.when(F.col("promedio_minutos_retraso_clima") >= 60.0, F.lit("Tormenta / Cierre Temporal"))
                .when(F.col("promedio_minutos_retraso_clima") >= 30.0, F.lit("Niebla / Baja Visibilidad"))
                .otherwise(F.lit("Lluvia / Vientos Cruzados"))
            ) \
            .withColumn("fecha_actualizacion_utc", F.current_timestamp())

        return kpi

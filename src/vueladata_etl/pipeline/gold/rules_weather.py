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
    def apply_weather_audit(df_flights, observations):
        from vueladata_etl.pipeline.gold.business_rules import optional
        df = df_flights
        adverse = F.lit(False)
        if observations is not None:
            hourly = (observations.withColumn("hora", F.date_trunc("hour", F.to_timestamp("fecha_hora_observacion_utc")))
                      .groupBy("codigo_iata", "hora")
                      .agg(F.max(F.when((F.col("visibilidad_km") < 1) | (F.col("velocidad_viento_kmh") > 50), 1).otherwise(0)).alias("adverso")))
            for side, moment in (("origen", "salida_real_ts"), ("destino", "llegada_real_ts")):
                fallback = "salida_programada_ts" if side == "origen" else "llegada_programada_ts"
                hour = F.date_trunc("hour", F.coalesce(optional(df, "fecha_hora_evento_utc", "timestamp"), optional(df, moment, "timestamp"), optional(df, fallback, "timestamp")))
                wx = hourly.select(F.col("codigo_iata").alias("_wx_airport"), F.col("hora").alias("_wx_hour"), F.col("adverso").alias("_wx_" + side))
                df = df.join(wx, (F.col("aeropuerto_" + side) == F.col("_wx_airport")) & (hour == F.col("_wx_hour")), "left").drop("_wx_airport", "_wx_hour")
            adverse = (F.coalesce(F.col("_wx_origen"), F.lit(0)) == 1) | (F.coalesce(F.col("_wx_destino"), F.lit(0)) == 1)
        technical = F.upper(optional(df, "codigo_causa_original")).isin("TECNICA", "TÉCNICA")
        previous = F.coalesce(optional(df, "requiere_auditoria", "boolean"), F.lit(False))
        return df.withColumn("requiere_auditoria", previous | F.coalesce(technical & adverse, F.lit(False))).drop("_wx_origen", "_wx_destino")

    @staticmethod
    def aggregate_kpi_clima(df_flights: DataFrame) -> DataFrame:
        """
        Agrega las afectaciones meteorológicas por aeropuerto de origen:
        Evalúa minutos de demora por clima y determina los aeropuertos más vulnerables.
        """
        df = df_flights
        if "requiere_auditoria" not in df.columns:
            df = df.withColumn("requiere_auditoria", F.lit(False))
        if "retraso_clima_min" in df.columns:
            df = df.withColumn("retraso_clima_val", F.coalesce(F.col("retraso_clima_min").cast(DoubleType()), F.lit(0.0)))
        else:
            df = df.withColumn("retraso_clima_val", F.lit(0.0).cast(DoubleType()))

        df = df.withColumn(
            "es_afectado_clima",
            F.when((F.col("retraso_clima_val") > 0.0) | F.col("requiere_auditoria"), 1).otherwise(0)
        )

        kpi = df.groupBy("anio", "mes", "aeropuerto_origen") \
            .agg(
                F.count("*").alias("total_vuelos_programados"),
                F.sum(F.when(F.col("requiere_auditoria"), 1).otherwise(0)).alias("total_vuelos_requieren_auditoria"),
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
                F.lit("NO_DETERMINADA")  # Delay duration does not identify a meteorological phenomenon.
            ) \
            .withColumn("fecha_actualizacion_utc", F.current_timestamp())

        return kpi

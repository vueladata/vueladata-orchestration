"""
Capa Gold: Motor de Reglas de Negocio - RN-01 (Puntualidad OTP)
Responsabilidad única (SRP): Implementar la lógica y métricas analíticas de On-Time Performance.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from vueladata_etl.core.constants import OTP_DELAY_THRESHOLD_MINUTES


class OtpRuleEngine:
    """Calcula las métricas de puntualidad aeronáutica bajo el estándar internacional OTP-15."""

    @staticmethod
    def apply_otp_rule(df_flights: DataFrame) -> DataFrame:
        """
        Aplica la regla RN-01 a nivel de registro:
        Un vuelo es puntual si es operado (no cancelado ni desviado) y su retraso de llegada <= 15 min.
        """
        return df_flights.withColumn(
            "retraso_llegada_val",
            F.coalesce(F.col("retraso_llegada_min").cast(DoubleType()), F.lit(0.0))
        ).withColumn(
            "es_cancelado",
            F.when(F.col("cancelado") == 1, 1).otherwise(0)
        ).withColumn(
            "es_desviado",
            F.when(F.col("desviado") == 1, 1).otherwise(0)
        ).withColumn(
            "es_operado",
            F.when((F.col("es_cancelado") == 0) & (F.col("es_desviado") == 0), 1).otherwise(0)
        ).withColumn(
            "es_puntual_otp",
            F.when(
                (F.col("es_operado") == 1) & (F.col("retraso_llegada_val") <= OTP_DELAY_THRESHOLD_MINUTES),
                1
            ).otherwise(0)
        )

    @staticmethod
    def aggregate_kpi_otp(df_with_otp: DataFrame) -> DataFrame:
        """Genera el cubo analítico agregado de puntualidad por aerolínea, período y aeropuerto de origen."""
        return df_with_otp.groupBy("anio", "mes", "codigo_aerolinea", "aeropuerto_origen") \
            .agg(
                F.count("*").alias("total_vuelos_programados"),
                F.sum("es_cancelado").alias("total_vuelos_cancelados"),
                F.sum("es_desviado").alias("total_vuelos_desviados"),
                F.sum("es_operado").alias("total_vuelos_operados"),
                F.sum("es_puntual_otp").alias("total_vuelos_puntuales"),
                F.avg("retraso_llegada_val").alias("promedio_retraso_llegada_min"),
                F.avg("retraso_salida_min").alias("promedio_retraso_salida_min")
            ) \
            .withColumn(
                "tasa_otp_porcentaje",
                F.round(
                    (F.col("total_vuelos_puntuales") / F.when(F.col("total_vuelos_operados") > 0, F.col("total_vuelos_operados")).otherwise(1)) * 100,
                    2
                )
            ) \
            .withColumn("fecha_actualizacion_utc", F.current_timestamp())

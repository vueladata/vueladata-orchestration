"""
Capa Gold: Motor de Reglas de Negocio - Costo Financiero de Compensaciones
═══════════════════════════════════════════════════════════════════════════════
Responsabilidad única (SRP): Cuantificar el costo económico y los pasajeros afectados
por cancelaciones y demoras prolongadas (>3 horas) por causa y ruta (HU-31).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from vueladata_etl.core.constants import (
    COMPENSATION_THRESHOLD_DELAY_MINUTES,
    COMPENSATION_BASE_COP_PER_PASSENGER,
    AVERAGE_PASSENGERS_PER_FLIGHT
)


class CompensationRuleEngine:
    """Modela y agrega las compensaciones económicas según normativas de aviación comercial."""

    @staticmethod
    def aggregate_kpi_compensaciones(df_flights: DataFrame) -> DataFrame:
        """
        Calcula las compensaciones por causa, ruta y mes:
        Aplica a vuelos cancelados o con retraso de llegada > 180 min (3 horas).
        """
        df = df_flights
        if "ruta" not in df.columns:
            df = df.withColumn("ruta", F.concat(F.col("aeropuerto_origen"), F.lit(" - "), F.col("aeropuerto_destino")))

        # Asegurar columnas numéricas necesarias de forma defensiva
        for col_name in ["retraso_clima_min", "retraso_aerolinea_min", "retraso_aeronave_tardia_min", "retraso_llegada_min"]:
            if col_name not in df.columns:
                df = df.withColumn(col_name, F.lit(0.0).cast(DoubleType()))
            else:
                df = df.withColumn(col_name, F.coalesce(F.col(col_name).cast(DoubleType()), F.lit(0.0)))

        if "cancelado" not in df.columns:
            df = df.withColumn("cancelado", F.lit(0))
        else:
            df = df.withColumn("cancelado", F.coalesce(F.col("cancelado"), F.lit(0)))

        # Filtrar vuelos que ameritan compensación obligatoria
        df_comp = df.filter(
            (F.col("cancelado") == 1) | (F.col("retraso_llegada_min") >= COMPENSATION_THRESHOLD_DELAY_MINUTES)
        )

        # Identificar la causa predominante de la compensación
        df_comp = df_comp.withColumn(
            "codigo_causa",
            F.when(F.col("cancelado") == 1, F.lit("CANCELACION_OPERACIONAL"))
            .when(F.col("retraso_clima_min") > 60.0, F.lit("CLIMA_EXTREMO"))
            .when(F.col("retraso_aerolinea_min") > 60.0, F.lit("FALLA_MANTENIMIENTO"))
            .when(F.col("retraso_aeronave_tardia_min") > 60.0, F.lit("ROTACION_FLOTA"))
            .otherwise(F.lit("DEMORA_PROLONGADA_GENERAL"))
        ).withColumn(
            "nombre_causa",
            F.when(F.col("codigo_causa") == "CANCELACION_OPERACIONAL", F.lit("Cancelación de Vuelo"))
            .when(F.col("codigo_causa") == "CLIMA_EXTREMO", F.lit("Tormenta / Meteorología Severa"))
            .when(F.col("codigo_causa") == "FALLA_MANTENIMIENTO", F.lit("Mantenimiento No Programado"))
            .when(F.col("codigo_causa") == "ROTACION_FLOTA", F.lit("Efecto Dominó en Rotación"))
            .otherwise(F.lit("Demora Prolongada (>3 Horas)"))
        ).withColumn(
            "es_imputable",
            F.when(F.col("codigo_causa").isin("CLIMA_EXTREMO"), False).otherwise(True)
        ).withColumn(
            "pasajeros_afectados_estimados",
            F.lit(AVERAGE_PASSENGERS_PER_FLIGHT)
        ).withColumn(
            "monto_compensado_estimado_cop",
            F.col("pasajeros_afectados_estimados") * F.lit(COMPENSATION_BASE_COP_PER_PASSENGER)
        )

        kpi = df_comp.groupBy("anio", "mes", "codigo_causa", "nombre_causa", "es_imputable", "aeropuerto_origen", "aeropuerto_destino", "ruta") \
            .agg(
                F.count("*").alias("total_vuelos_con_compensacion"),
                F.sum("pasajeros_afectados_estimados").alias("total_pasajeros_afectados"),
                F.round(F.sum("monto_compensado_estimado_cop"), 2).alias("monto_total_compensado_cop"),
                F.round(F.avg("monto_compensado_estimado_cop"), 2).alias("monto_promedio_vuelo_cop")
            ) \
            .withColumn("fecha_actualizacion_utc", F.current_timestamp())

        return kpi

"""RN-07: connections require actual paired reservation segments and hub MCT."""
from pyspark.sql import functions as F
from vueladata_etl.pipeline.gold.business_rules import optional


class ConnectionsRuleEngine:
    @staticmethod
    def evaluate_connections(df):
        arrival = optional(df, "llegada_conexion_utc", "timestamp")
        departure = optional(df, "salida_conexion_utc", "timestamp")
        available = (departure.cast("long") - arrival.cast("long")) / 60.0
        minimum = optional(df, "tiempo_min_conexion_min", "double")
        return (df.withColumn("minutos_escala_disponible", available)
                .withColumn("es_conexion_en_riesgo", (available < minimum))
                .withColumn("es_conexion_perdida", F.when(optional(df, "conexion_cancelada", "boolean"), 1).when(optional(df, "estado_conexion") == "PERDIDA", 1)
                            .when(available < 0, 1).when(available.isNotNull(), 0)))

    @staticmethod
    def aggregate_kpi_conexiones(df_connections):
        df = ConnectionsRuleEngine.evaluate_connections(df_connections)
        if "aeropuerto_hub" not in df.columns:
            df = df.withColumn("aeropuerto_hub", F.col("aeropuerto_destino"))
        # Flights without an actual connection are not synthetic passenger connections.
        df = df.filter(optional(df, "id_conexion", "long").isNotNull())
        return (df.groupBy("anio", "mes", "aeropuerto_hub")
                .agg(F.count("*").alias("total_conexiones_programadas"),
                     F.first(optional(df, "nombre_hub"), ignorenulls=True).alias("nombre_hub"),
                     F.sum(F.when(~F.col("es_conexion_en_riesgo") & (F.col("es_conexion_perdida") == 0), 1).otherwise(0)).alias("total_conexiones_exitosas"),
                     F.sum("es_conexion_perdida").alias("total_conexiones_perdidas"),
                     F.round(F.avg("minutos_escala_disponible"), 2).alias("promedio_minutos_escala_disponible"),
                     F.sum(F.when(F.col("es_conexion_en_riesgo") & (F.col("es_conexion_perdida") == 0), 1).otherwise(0)).alias("total_conexiones_en_riesgo"))
                .withColumn("tasa_perdida_conexiones_pct", F.when(F.col("total_conexiones_programadas") > 0,
                    F.round(F.col("total_conexiones_perdidas") / F.col("total_conexiones_programadas") * 100, 2)).otherwise(0.0))
                .withColumn("fecha_actualizacion_utc", F.current_timestamp()))

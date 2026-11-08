"""RN-05/06: actual segment fares; preserve the existing BI aggregation schema."""
from pyspark.sql import functions as F
from vueladata_etl.pipeline.gold.business_rules import compensation_rows, optional


class CompensationRuleEngine:
    apply_compensation_rule = staticmethod(compensation_rows)

    @staticmethod
    def aggregate_kpi_compensaciones(df_flights):
        df = compensation_rows(df_flights)
        if "ruta" not in df.columns:
            df = df.withColumn("ruta", F.concat_ws(" - ", "aeropuerto_origen", "aeropuerto_destino"))
        df = df.filter((F.col("cancelado") == 1) | (F.col("retraso_llegada_min") >= 120))
        df = (df.withColumn("nombre_causa", F.coalesce(optional(df, "nombre_causa"), F.col("codigo_causa")))
              .withColumn("pasajeros_afectados_estimados", optional(df, "pasajeros_afectados", "long"))
              .withColumn("monto_compensado_estimado_cop", F.when(
                  optional(df, "moneda") == "COP", F.col("monto_compensacion"))))
        return (df.groupBy("anio", "mes", "codigo_causa", "nombre_causa", "es_imputable",
                           "aeropuerto_origen", "aeropuerto_destino", "ruta")
                .agg(F.sum(F.when(F.col("porcentaje_compensacion") > 0, 1).otherwise(0)).alias("total_vuelos_con_compensacion"),
                     F.sum("pasajeros_afectados_estimados").alias("total_pasajeros_afectados"),
                     F.round(F.sum("monto_compensado_estimado_cop"), 2).alias("monto_total_compensado_cop"),
                     F.round(F.avg("monto_compensado_estimado_cop"), 2).alias("monto_promedio_vuelo_cop"),
                     F.sum(F.when(F.col("monto_compensado_estimado_cop").isNull(), 1).otherwise(0)).alias("total_vuelos_sin_valoracion"))
                .withColumn("fecha_actualizacion_utc", F.current_timestamp()))

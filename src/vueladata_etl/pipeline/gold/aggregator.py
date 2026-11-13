"""Publish the seven immutable BI datasets plus additive star-schema tables."""
from pyspark import StorageLevel
from pyspark.sql import functions as F
from vueladata_etl.pipeline.gold.business_rules import previous_cause
from pyspark.errors import AnalysisException
from vueladata_etl.pipeline.gold.rules_otp import OtpRuleEngine
from vueladata_etl.pipeline.gold.rules_propagation import DelayPropagationEngine
from vueladata_etl.pipeline.gold.rules_causes import CausesRuleEngine
from vueladata_etl.pipeline.gold.rules_compensations import CompensationRuleEngine
from vueladata_etl.pipeline.gold.rules_connections import ConnectionsRuleEngine
from vueladata_etl.pipeline.gold.rules_weather import WeatherRuleEngine
from vueladata_etl.pipeline.gold.star import build_star


class GoldAggregator:
    def __init__(self, spark, path_config, mode: str = "all"):
        self.spark, self.paths, self.mode = spark, path_config, mode

    def process_gold_metrics(self, df_flights=None, *, only_star_model=False):
        if df_flights is None:
            df_flights = self.spark.read.parquet(f"{self.paths.silver_path}/vuelos_silver")
        if self.mode == "daily":
            df_flights = df_flights.filter(F.col("fuente") != "F1_KAGGLE")
        elif self.mode == "historical":
            df_flights = df_flights.filter(F.col("fuente") == "F1_KAGGLE")
        df = previous_cause(df_flights)
        df = DelayPropagationEngine.calculate_propagation(OtpRuleEngine.apply_otp_rule(df))
        # Keep reusable rows in memory with RAM storage (backed by disk if needed)
        # to maximize throughput across all 16 analytical and dimensional tables.
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            try:
                connections = self.spark.read.parquet(f"{self.paths.silver_path}/conexiones")
            except AnalysisException as exc:
                if exc.getErrorClass() != "PATH_NOT_FOUND":
                    raise
                connections = df.limit(0)
            tables = {}
            if not only_star_model:
                if "nombre_hub" not in connections.columns and not connections.isEmpty():
                    raise ValueError("Silver conexiones lacks nombre_hub: rebuild Silver with the airport catalog before publishing BI cubes")
                tables = {
                    "puntualidad_aerolineas": OtpRuleEngine.aggregate_kpi_otp(df),
                    "puntualidad_rutas": OtpRuleEngine.aggregate_kpi_otp_rutas_franjas(df),
                    "causas_retraso": CausesRuleEngine.aggregate_kpi_causas(df),
                    "costos_compensacion": CompensationRuleEngine.aggregate_kpi_compensaciones(df),
                    "conexiones_hubs": ConnectionsRuleEngine.aggregate_kpi_conexiones(connections),
                    "propagacion_flota": DelayPropagationEngine.aggregate_propagation_kpi(df),
                    "impacto_clima": WeatherRuleEngine.aggregate_kpi_clima(df),
                }
            tables.update(build_star(df, connections))
            overwrite_mode = "dynamic" if self.mode == "daily" else "static"
            for name, table in tables.items():
                if {"anio", "mes"} <= set(table.columns) and not table.isEmpty():
                    writer = (table.write.mode("overwrite").option("partitionOverwriteMode", overwrite_mode)
                              .option("compression", "snappy").partitionBy("anio", "mes"))
                    writer.parquet(f"{self.paths.gold_path}/{name}")
                else:
                    if self.mode == "daily":
                        try:
                            existing = self.spark.read.parquet(f"{self.paths.gold_path}/{name}")
                            table = existing.unionByName(table).distinct()
                        except (AnalysisException, Exception):
                            pass
                    table.write.mode("overwrite").option("compression", "snappy").parquet(f"{self.paths.gold_path}/{name}")
            return df
        finally:
            df.unpersist()

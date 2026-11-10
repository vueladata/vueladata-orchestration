"""Publish the seven immutable BI datasets plus additive star-schema tables."""
from pyspark import StorageLevel
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
    def __init__(self, spark, path_config):
        self.spark, self.paths = spark, path_config

    def process_gold_metrics(self, df_flights=None, *, only_star_model=False):
        if df_flights is None:
            df_flights = self.spark.read.parquet(f"{self.paths.silver_path}/vuelos_silver")
        df = previous_cause(df_flights)
        df = DelayPropagationEngine.calculate_propagation(OtpRuleEngine.apply_otp_rule(df))
        # The full historical snapshot exceeds the 4 GiB Fargate container.
        # Keep reusable rows on local disk so window sorts retain their heap.
        df = df.persist(StorageLevel.DISK_ONLY)
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
            for name, table in tables.items():
                writer = (table.write.mode("overwrite").option("partitionOverwriteMode", "static")
                          .option("compression", "snappy"))
                if {"anio", "mes"} <= set(table.columns) and not table.isEmpty():
                    writer = writer.partitionBy("anio", "mes")
                # Input is the COMPLETE Silver snapshot, so empty/new months clear
                # stale metrics without losing other days or historical sources.
                writer.parquet(f"{self.paths.gold_path}/{name}")
            return df
        finally:
            df.unpersist()

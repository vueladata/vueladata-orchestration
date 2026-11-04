"""
Capa Gold: Agregador y Persistencia de Datasets Analíticos
Responsabilidad única (SRP): Orquestar el cómputo de métricas analíticas finales y persistir en la capa Gold.
"""

from typing import Optional
from pyspark.sql import DataFrame, SparkSession

from vueladata_etl.core.config import PathConfig
from vueladata_etl.pipeline.gold.rules_otp import OtpRuleEngine
from vueladata_etl.pipeline.gold.rules_propagation import DelayPropagationEngine


class GoldAggregator:
    """Orquestador de la capa analítica Gold para entrega a dashboards y consumidores."""

    def __init__(self, spark: SparkSession, path_config: PathConfig):
        self.spark = spark
        self.paths = path_config

    def process_gold_metrics(self, df_flights: Optional[DataFrame] = None) -> None:
        """Calcula y persiste las tablas analíticas Gold de OTP y propagación de retrasos."""
        if df_flights is None:
            silver_input = f"{self.paths.silver_path}/vuelos_silver"
            print(f"[*] Gold: Cargando vuelos desde capa Silver en {silver_input}...")
            try:
                df_flights = self.spark.read.parquet(silver_input)
            except Exception as e:
                print(f"[!] Error leyendo Silver Vuelos para Gold: {e}")
                return

        # 1. Aplicación de Regla RN-01: Puntualidad OTP
        print("[*] Gold: Aplicando Regla RN-01 (Puntualidad OTP)...")
        df_with_otp = OtpRuleEngine.apply_otp_rule(df_flights)
        kpi_otp = OtpRuleEngine.aggregate_kpi_otp(df_with_otp)

        output_otp = f"{self.paths.gold_path}/kpi_otp_aerolineas"
        print(f"[*] Escribiendo Gold OTP en Parquet en {output_otp}...")
        kpi_otp.write.mode("overwrite") \
            .partitionBy("anio", "mes") \
            .parquet(output_otp)
        print("[+] Gold OTP: Persistido exitosamente.")

        # 2. Aplicación de Regla RN-08: Propagación de Retrasos en Flota
        print("[*] Gold: Aplicando Regla RN-08 (Propagación de Retrasos por Aeronave)...")
        df_propagacion = DelayPropagationEngine.calculate_propagation(df_with_otp)
        kpi_propagacion = DelayPropagationEngine.aggregate_propagation_kpi(df_propagacion)

        output_prop = f"{self.paths.gold_path}/kpi_propagacion_flota"
        print(f"[*] Escribiendo Gold Propagación en Parquet en {output_prop}...")
        kpi_propagacion.write.mode("overwrite") \
            .partitionBy("anio", "mes") \
            .parquet(output_prop)
        print("[+] Gold Propagación: Persistido exitosamente.")

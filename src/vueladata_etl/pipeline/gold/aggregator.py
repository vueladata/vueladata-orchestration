"""
Capa Gold: Agregador y Persistencia de Datasets Analíticos (HU-31)
═══════════════════════════════════════════════════════════════════════════════
Responsabilidad única (SRP): Orquestar el cómputo de métricas analíticas finales
y persistir en la capa Gold todos los cubos necesarios para Power BI y Athena.
"""

from typing import Optional
from pyspark.sql import DataFrame, SparkSession

from vueladata_etl.core.config import PathConfig
from vueladata_etl.pipeline.gold.rules_otp import OtpRuleEngine
from vueladata_etl.pipeline.gold.rules_propagation import DelayPropagationEngine
from vueladata_etl.pipeline.gold.rules_causes import CausesRuleEngine
from vueladata_etl.pipeline.gold.rules_compensations import CompensationRuleEngine
from vueladata_etl.pipeline.gold.rules_connections import ConnectionsRuleEngine
from vueladata_etl.pipeline.gold.rules_weather import WeatherRuleEngine


class GoldAggregator:
    """Orquestador integral de la capa analítica Gold para entrega a Power BI y Athena."""

    def __init__(self, spark: SparkSession, path_config: PathConfig):
        self.spark = spark
        self.paths = path_config

    def process_gold_metrics(self, df_flights: Optional[DataFrame] = None) -> None:
        """Calcula y persiste todos los modelos analíticos Gold que responden las 6 preguntas de negocio."""
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

        # 1.1 Cubo OTP por Aerolínea y Mes
        kpi_otp = OtpRuleEngine.aggregate_kpi_otp(df_with_otp)
        output_otp = f"{self.paths.gold_path}/puntualidad_aerolineas"
        print(f"[*] Escribiendo Gold OTP en Parquet en {output_otp}...")
        kpi_otp.write.mode("overwrite").partitionBy("anio", "mes").parquet(output_otp)

        # 1.2 Pregunta 1: Cubo OTP por Ruta, Aeropuerto, Franja y Mes
        print("[*] Gold [Pregunta 1]: Aplicando OTP por Ruta y Franja Horaria...")
        kpi_rutas = OtpRuleEngine.aggregate_kpi_otp_rutas_franjas(df_with_otp)
        output_rutas = f"{self.paths.gold_path}/puntualidad_rutas"
        print(f"[*] Escribiendo Gold OTP Rutas y Franjas en {output_rutas}...")
        kpi_rutas.write.mode("overwrite").partitionBy("anio", "mes").parquet(output_rutas)

        # 2. Pregunta 2: Causas de Retraso e Imputabilidad
        print("[*] Gold [Pregunta 2]: Aplicando Participación Imputable vs No Imputable...")
        kpi_causas = CausesRuleEngine.aggregate_kpi_causas(df_with_otp)
        output_causas = f"{self.paths.gold_path}/causas_retraso"
        print(f"[*] Escribiendo Gold Causas en {output_causas}...")
        kpi_causas.write.mode("overwrite").partitionBy("anio", "mes").parquet(output_causas)

        # 3. Pregunta 3: Costo de Compensaciones
        print("[*] Gold [Pregunta 3]: Calculando Costo Financiero de Compensaciones...")
        kpi_comp = CompensationRuleEngine.aggregate_kpi_compensaciones(df_with_otp)
        output_comp = f"{self.paths.gold_path}/costos_compensacion"
        print(f"[*] Escribiendo Gold Compensaciones en {output_comp}...")
        kpi_comp.write.mode("overwrite").partitionBy("anio", "mes").parquet(output_comp)

        # 4. Pregunta 4: Conexiones Perdidas en Hubs
        print("[*] Gold [Pregunta 4]: Evaluando Pérdida de Conexiones en Aeropuertos Hub...")
        kpi_con = ConnectionsRuleEngine.aggregate_kpi_conexiones(df_with_otp)
        output_con = f"{self.paths.gold_path}/conexiones_hubs"
        print(f"[*] Escribiendo Gold Conexiones Hubs en {output_con}...")
        kpi_con.write.mode("overwrite").partitionBy("anio", "mes").parquet(output_con)

        # 5. Pregunta 5: Propagación de Retrasos en Flota (RN-08)
        print("[*] Gold [Pregunta 5]: Aplicando Regla RN-08 (Propagación de Retrasos por Aeronave)...")
        df_propagacion = DelayPropagationEngine.calculate_propagation(df_with_otp)
        kpi_propagacion = DelayPropagationEngine.aggregate_propagation_kpi(df_propagacion)
        output_prop = f"{self.paths.gold_path}/propagacion_flota"
        print(f"[*] Escribiendo Gold Propagación en Parquet en {output_prop}...")
        kpi_propagacion.write.mode("overwrite").partitionBy("anio", "mes").parquet(output_prop)

        # 6. Pregunta 6: Retrasos Asociados a Clima por Aeropuerto
        print("[*] Gold [Pregunta 6]: Calculando Impacto Meteorológico por Aeropuerto...")
        kpi_clima = WeatherRuleEngine.aggregate_kpi_clima(df_with_otp)
        output_clima = f"{self.paths.gold_path}/impacto_clima"
        print(f"[*] Escribiendo Gold Clima Aeropuertos en {output_clima}...")
        kpi_clima.write.mode("overwrite").partitionBy("anio", "mes").parquet(output_clima)

        print("[+] Todas las capas Gold analíticas fueron generadas y persistidas exitosamente.")

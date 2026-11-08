"""
Capa Silver: Transformador y Orquestador de la Capa Curada
═══════════════════════════════════════════════════════════════════════════════
Garantiza calidad total mediante validación y aislamiento de errores a cuarentena,
deduplicación por clave de negocio y sobrescritura dinámica de particiones.
"""

from typing import Optional
from pyspark.sql import DataFrame, functions as F
from pyspark.errors import AnalysisException
from vueladata_etl.pipeline.silver.catalogs import airport_timezones, historical_timestamps
from vueladata_etl.pipeline.silver.operational import enrich_flights, connection_rows
from vueladata_etl.pipeline.gold.rules_weather import WeatherRuleEngine
from vueladata_etl.core.persistence import replace_sources

from vueladata_etl.core.config import PathConfig
from vueladata_etl.core.audit import AuditManager
from vueladata_etl.pipeline.bronze.reader import BronzeReader
from vueladata_etl.pipeline.silver.cleaner_flights import FlightsCleaner
from vueladata_etl.pipeline.silver.cleaner_events import EventsCleaner
from vueladata_etl.pipeline.silver.validator_quarantine import FlightQualityValidator


class SilverTransformer:
    """Orquestador de la transformación Medallion Bronze -> Silver con garantía de calidad e idempotencia."""

    def __init__(self, reader: BronzeReader, path_config: PathConfig, audit_manager: Optional[AuditManager] = None):
        self.reader = reader
        self.paths = path_config
        self.audit = audit_manager

    def process_flights(
        self,
        mode: str,
        filter_date: Optional[str] = None,
        id_ejecucion: Optional[int] = None,
        dag_id: str = "dag_daily_batch_spark"
    ) -> Optional[DataFrame]:
        """
        Procesa, valida y escribe el dataset unificado de vuelos en la capa Silver:
        1. Limpia y estandariza los registros de Kaggle (F1), Rotación (F6) y RDS.
        2. Aplica control de calidad: separa válidos y enruta rechazados a la BD de auditoría.
        3. Persiste únicamente los registros limpios en formato Parquet particionado.
        """
        df_flights = None

        # 1. Fuente F1 Kaggle (histórico 5.8M)
        if mode in ["historical", "all"]:
            f1_path = f"{self.paths.bronze_path}/f1_kaggle"
            print(f"[*] Silver: Leyendo y estandarizando F1 Kaggle desde {f1_path}...")
            df_f1_raw = self.reader.read_f1_flights(f1_path)
            df_flights = FlightsCleaner.clean_f1_flights(df_f1_raw)
            airports = self.reader.read_f1_airports(f1_path)
            airlines = self.reader.read_f1_airlines(f1_path).select(F.col("IATA_CODE").alias("codigo_aerolinea"), F.col("AIRLINE").alias("nombre_aerolinea"))
            df_flights = historical_timestamps(df_flights, airport_timezones(airports))
            df_flights = df_flights.join(F.broadcast(airlines), "codigo_aerolinea", "left")

        if mode in ["daily", "all"]:
            rds_path = f"{self.paths.bronze_path}/rds_operacional"
            read = lambda table: self.reader.read_rds_table(rds_path, table, filter_date)
            operational = FlightsCleaner.clean_rds_flights(enrich_flights(read))
            weather = read("observacion_clima").join(F.broadcast(read("aeropuerto").select("id_aeropuerto", "codigo_iata")), "id_aeropuerto")
            json_weather = self.reader.read_weather_observations(f"{self.paths.bronze_path}/events")
            if json_weather is not None:
                airport_codes = read("aeropuerto").select(F.col("id_aeropuerto").cast("string").alias("_airport_id"), "codigo_iata")
                json_weather = json_weather.join(F.broadcast(airport_codes), F.col("id_aeropuerto") == F.col("_airport_id"), "left")
                json_weather = json_weather.withColumn("codigo_iata", F.coalesce("codigo_iata", F.when(F.col("id_aeropuerto").rlike("^[A-Z]{3}$"), F.col("id_aeropuerto"))))
                cols = ["codigo_iata", "fecha_hora_observacion_utc", "visibilidad_km", "velocidad_viento_kmh"]
                weather = weather.select(*cols).unionByName(json_weather.select(*cols))
            operational = WeatherRuleEngine.apply_weather_audit(operational, weather)
            connections = connection_rows(read)
            connections.write.mode("overwrite").option("compression", "snappy").parquet(f"{self.paths.silver_path}/conexiones")
            f6 = FlightsCleaner.clean_f6_itinerary(self.reader.read_f6_itinerary(f"{self.paths.bronze_path}/f6_rotacion"))
            # F6 is a schedule, not evidence that a flight arrived on time. Prefer
            # the operational record if both refer to the same scheduled flight.
            keys = ["numero_vuelo", "salida_programada_ts"]
            f6 = f6.join(operational.select(*keys), keys, "left_anti")
            current = operational.unionByName(f6, allowMissingColumns=True)
            df_flights = current if df_flights is None else df_flights.unionByName(current, allowMissingColumns=True)

        # 4. Control de Calidad y Cuarentena de Errores
        if df_flights is not None:
            df_validos, rejected = FlightQualityValidator.validar_y_separar(
                df_flights=df_flights,
                id_ejecucion=id_ejecucion,
                dag_id=dag_id,
                audit_manager=self.audit
            )

            # Full rejects in S3; the RDS audit only holds a bounded sample.
            rejected.write.mode("overwrite").option("compression", "snappy").parquet(f"{self.paths.silver_path}/cuarentena_vuelos/{mode}")
            keys = ["fuente", "codigo_aerolinea", "numero_vuelo", "fecha_vuelo", "aeropuerto_origen", "aeropuerto_destino", "salida_programada_raw"]
            df_validos = df_validos.dropDuplicates(keys)
            sources = (["F1_KAGGLE"] if mode == "historical" else
                       ["F6_ROTACION", "RDS_OPERACIONAL"] if mode == "daily" else
                       ["F1_KAGGLE", "F6_ROTACION", "RDS_OPERACIONAL"])
            return replace_sources(df_validos, f"{self.paths.silver_path}/vuelos_silver", sources, self.paths.base_path)

        return None

    def process_events(self) -> None:
        events_path = f"{self.paths.bronze_path}/events"
        sources = [
            (self.reader.read_oooi_events, EventsCleaner.clean_oooi_events, "eventos_oooi"),
            (self.reader.read_payment_events, EventsCleaner.clean_payment_events, "eventos_pagos"),
            (self.reader.read_weather_observations, EventsCleaner.clean_weather_observations, "observaciones_clima"),
        ]
        for read, clean, target in sources:
            df = clean(read(events_path))
            if df is not None:
                df.write.mode("overwrite").option("partitionOverwriteMode", "static").option("compression", "snappy").partitionBy("fecha_particion").parquet(f"{self.paths.silver_path}/{target}")
        positions = self.reader.read_opensky_events(events_path)
        if positions is not None:
            positions.dropDuplicates(["id_externo_posicion"]).write.mode("overwrite").option("compression", "snappy").parquet(f"{self.paths.silver_path}/posiciones_aeronaves")

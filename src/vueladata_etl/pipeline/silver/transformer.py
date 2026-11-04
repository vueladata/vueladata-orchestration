"""
Capa Silver: Transformador y Orquestador de la Capa Curada
═══════════════════════════════════════════════════════════════════════════════
Garantiza calidad total mediante validación y aislamiento de errores a cuarentena,
deduplicación por clave de negocio y sobrescritura dinámica de particiones.
"""

from typing import Optional
from pyspark.sql import DataFrame

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

        # 2. Fuente F6 Rotación de Flota (sintético semanal)
        if mode in ["daily", "all"]:
            f6_path = f"{self.paths.bronze_path}/f6_rotacion"
            print(f"[*] Silver: Leyendo y estandarizando F6 Rotación desde {f6_path}...")
            try:
                df_f6_raw = self.reader.read_f6_itinerary(f6_path)
                df_f6_clean = FlightsCleaner.clean_f6_itinerary(df_f6_raw, filter_date)
                if df_flights is None:
                    df_flights = df_f6_clean
                else:
                    df_flights = df_flights.unionByName(df_f6_clean, allowMissingColumns=True)
            except Exception as e:
                print(f"[!] Advertencia F6: {e}")

        # 3. Fuente RDS Operacional (extraída desde PostgreSQL operacional a S3 Bronze)
        if mode in ["daily", "all"]:
            rds_path = f"{self.paths.bronze_path}/rds_operacional"
            print(f"[*] Silver: Leyendo y estandarizando Vuelos RDS desde {rds_path}...")
            try:
                df_rds_raw = self.reader.read_rds_flights(rds_path)
                if df_rds_raw is not None and df_rds_raw.count() > 0:
                    df_rds_clean = FlightsCleaner.clean_rds_flights(df_rds_raw, filter_date)
                    if df_flights is None:
                        df_flights = df_rds_clean
                    else:
                        df_flights = df_flights.unionByName(df_rds_clean, allowMissingColumns=True)
            except Exception as e:
                print(f"[!] Advertencia RDS Vuelos: {e}")

        # 4. Control de Calidad y Cuarentena de Errores
        if df_flights is not None:
            df_validos, _ = FlightQualityValidator.validar_y_separar(
                df_flights=df_flights,
                id_ejecucion=id_ejecucion,
                dag_id=dag_id,
                audit_manager=self.audit
            )

            output_dir = f"{self.paths.silver_path}/vuelos_silver"
            print(f"[*] Persistiendo Vuelos Silver en Parquet de forma idempotente en {output_dir}...")
            # Idempotencia: sobrescritura por partición (anio, mes)
            df_validos.write.mode("overwrite") \
                .partitionBy("anio", "mes") \
                .parquet(output_dir)
            print("[+] Silver Vuelos: Persistido exitosamente con control de calidad.")

            return df_validos

        return None

    def process_events(self) -> None:
        """
        Procesa y persiste los eventos JSON en Silver garantizando idempotencia:
        1. Deduplica por id_externo_evento.
        2. Sobrescribe únicamente la partición de la fecha afectada.
        """
        events_path = f"{self.paths.bronze_path}/events"

        # 1. Eventos OOOI
        try:
            df_oooi_raw = self.reader.read_oooi_events(events_path)
            df_oooi_clean = EventsCleaner.clean_oooi_events(df_oooi_raw)
            if df_oooi_clean is not None and df_oooi_clean.count() > 0:
                df_oooi_clean.write.mode("overwrite") \
                    .partitionBy("fecha_particion") \
                    .parquet(f"{self.paths.silver_path}/eventos_oooi")
        except Exception as e:
            print(f"[!] Aviso eventos OOOI: {e}")

        # 2. Eventos de Compensaciones y Pagos
        try:
            df_pagos_raw = self.reader.read_payment_events(events_path)
            df_pagos_clean = EventsCleaner.clean_payment_events(df_pagos_raw)
            if df_pagos_clean is not None and df_pagos_clean.count() > 0:
                df_pagos_clean.write.mode("overwrite") \
                    .partitionBy("fecha_particion") \
                    .parquet(f"{self.paths.silver_path}/eventos_pagos")
        except Exception as e:
            print(f"[!] Aviso eventos Pagos: {e}")

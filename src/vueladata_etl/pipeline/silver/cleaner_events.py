"""
Capa Silver: Limpiador y Estandarizador de Eventos de Streaming / Batch
Responsabilidad única (SRP): Tipar, validar y estructurar los eventos operacionales OOOI, clima y pagos.
"""

from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from vueladata_etl.core.constants import VALID_OOOI_EVENTS


class EventsCleaner:
    """Procesa y valida eventos de streaming acumulados por Worker Lake en Bronze."""

    @staticmethod
    def clean_oooi_events(df_raw: Optional[DataFrame]) -> Optional[DataFrame]:
        """Estandariza fechas ISO 8601 a Timestamps y valida códigos de eventos OOOI."""
        if df_raw is None:
            return None

        return df_raw \
            .filter(F.col("codigo_evento").isin(VALID_OOOI_EVENTS)) \
            .withColumn("fecha_hora_evento_ts", F.to_timestamp(F.col("fecha_hora_evento_utc"))) \
            .withColumn("fecha_particion", F.to_date(F.col("fecha_hora_evento_ts"))) \
            .dropDuplicates(["id_externo_evento"])

    @staticmethod
    def clean_payment_events(df_raw: Optional[DataFrame]) -> Optional[DataFrame]:
        """Estandariza los eventos asíncronos de compensaciones y pagos."""
        if df_raw is None:
            return None

        return df_raw \
            .withColumn("fecha_hora_evento_ts", F.to_timestamp(F.col("fecha_hora_evento_utc"))) \
            .withColumn("fecha_particion", F.to_date(F.col("fecha_hora_evento_ts"))) \
            .dropDuplicates(["id_externo_evento"])

    @staticmethod
    def clean_weather_observations(df_raw: Optional[DataFrame]) -> Optional[DataFrame]:
        """Estandariza observaciones meteorológicas de aeropuertos."""
        if df_raw is None:
            return None

        return df_raw \
            .withColumn("fecha_hora_observacion_ts", F.to_timestamp(F.col("fecha_hora_observacion_utc"))) \
            .withColumn("fecha_particion", F.to_date(F.col("fecha_hora_observacion_ts")))

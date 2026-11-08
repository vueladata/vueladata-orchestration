"""
Capa Bronze: Lector de Datos en Bruto con Esquemas Explícitos
Responsabilidad única (SRP): Cargar fuentes de datos sin inferencia de tipos desde S3/Local.
"""

from typing import Optional
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from vueladata_etl.core.schemas import (
    SCHEMA_F1_FLIGHTS,
    SCHEMA_F1_AIRLINES,
    SCHEMA_F1_AIRPORTS,
    SCHEMA_F6_ITINERARY,
    SCHEMA_F6_FLEET,
    SCHEMA_F6_ROUTES,
    SCHEMA_EVENTO_OOOI,
    SCHEMA_EVENTO_PAGO,
    SCHEMA_OBSERVACION_CLIMA,
    SCHEMA_POSICION_AERONAVE
)


class BronzeReader:
    """Lector especializado para la capa Bronze utilizando esquemas explícitos StructType."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_f1_flights(self, f1_dir: str) -> DataFrame:
        """Lee el dataset histórico de vuelos Kaggle (5.8M) con esquema explícito."""
        return self.spark.read \
            .option("header", "true") \
            .option("enforceSchema", "false") \
            .option("mode", "FAILFAST") \
            .schema(SCHEMA_F1_FLIGHTS) \
            .csv(f"{f1_dir}/flights.csv")

    def read_f1_airlines(self, f1_dir: str) -> DataFrame:
        """Lee el catálogo de aerolíneas IATA."""
        return self.spark.read \
            .option("header", "true") \
            .option("enforceSchema", "false") \
            .option("mode", "FAILFAST") \
            .schema(SCHEMA_F1_AIRLINES) \
            .csv(f"{f1_dir}/airlines.csv")

    def read_f1_airports(self, f1_dir: str) -> DataFrame:
        """Lee el catálogo de aeropuertos de Kaggle."""
        return self.spark.read \
            .option("header", "true") \
            .option("enforceSchema", "false") \
            .option("mode", "FAILFAST") \
            .schema(SCHEMA_F1_AIRPORTS) \
            .csv(f"{f1_dir}/airports.csv")

    def read_f6_itinerary(self, f6_dir: str) -> DataFrame:
        """Lee el itinerario semanal sintético de rotación de flota."""
        return self.spark.read \
            .option("header", "true") \
            .option("enforceSchema", "false") \
            .option("mode", "FAILFAST") \
            .schema(SCHEMA_F6_ITINERARY) \
            .csv(f"{f6_dir}/synthetic_weekly_itinerary.csv")

    def read_f6_fleet(self, f6_dir: str) -> DataFrame:
        """Lee el catálogo de aeronaves y capacidades."""
        return self.spark.read \
            .option("header", "true") \
            .option("enforceSchema", "false") \
            .option("mode", "FAILFAST") \
            .schema(SCHEMA_F6_FLEET) \
            .csv(f"{f6_dir}/synthetic_fleet.csv")

    def read_f6_routes(self, f6_dir: str) -> DataFrame:
        """Lee el catálogo de rutas programadas."""
        return self.spark.read \
            .option("header", "true") \
            .option("enforceSchema", "false") \
            .option("mode", "FAILFAST") \
            .schema(SCHEMA_F6_ROUTES) \
            .csv(f"{f6_dir}/synthetic_routes.csv")

    def read_oooi_events(self, events_dir: str) -> Optional[DataFrame]:
        """Lee los eventos operacionales OOOI depositados por Worker Lake en Bronze."""
        try:
            return self.spark.read \
                .option("mode", "FAILFAST") \
                .schema(SCHEMA_EVENTO_OOOI) \
                .json(f"{events_dir}/oooi")
        except AnalysisException as exc:
            if exc.getErrorClass() == "PATH_NOT_FOUND":
                return None
            raise

    def read_pago_events(self, events_dir: str) -> Optional[DataFrame]:
        """Lee los eventos de pagos asíncronos depositados en Bronze."""
        try:
            return self.spark.read \
                .option("mode", "FAILFAST") \
                .schema(SCHEMA_EVENTO_PAGO) \
                .json(f"{events_dir}/pagos")
        except AnalysisException as exc:
            if exc.getErrorClass() == "PATH_NOT_FOUND":
                return None
            raise

    def read_payment_events(self, events_dir: str) -> Optional[DataFrame]:
        """Alias en inglés para lectura de eventos de compensaciones y pagos."""
        return self.read_pago_events(events_dir)

    def read_clima_events(self, events_dir: str) -> Optional[DataFrame]:
        """Lee las observaciones meteorológicas de aeropuertos."""
        try:
            return self.spark.read \
                .option("mode", "FAILFAST") \
                .schema(SCHEMA_OBSERVACION_CLIMA) \
                .json(f"{events_dir}/clima")
        except AnalysisException as exc:
            if exc.getErrorClass() == "PATH_NOT_FOUND":
                return None
            raise

    def read_weather_observations(self, events_dir: str) -> Optional[DataFrame]:
        """Alias en inglés para observaciones meteorológicas."""
        return self.read_clima_events(events_dir)

    def read_opensky_events(self, events_dir: str) -> Optional[DataFrame]:
        """Lee las trazas de radar OpenSky depositadas en Bronze."""
        try:
            return self.spark.read \
                .option("mode", "FAILFAST") \
                .schema(SCHEMA_POSICION_AERONAVE) \
                .json(f"{events_dir}/opensky")
        except AnalysisException as exc:
            if exc.getErrorClass() == "PATH_NOT_FOUND":
                return None
            raise

    def read_rds_flights(self, rds_bronze_dir: str) -> Optional[DataFrame]:
        """Lee los vuelos transaccionales extraídos desde PostgreSQL RDS en formato Parquet."""
        try:
            return self.spark.read.parquet(f"{rds_bronze_dir}/vuelo")
        except AnalysisException as exc:
            if exc.getErrorClass() == "PATH_NOT_FOUND":
                return None
            raise

    def read_rds_table(self, directory, table, snapshot_date):
        # All operational inputs belong to the same snapshot, not all historical copies.
        return self.spark.read.parquet(f"{directory}/{table}/fecha_proceso={snapshot_date}")

"""
Configuración centralizada de Apache Spark y gestión de rutas S3/Local.
Configura sobrescritura dinámica de particiones para garantizar idempotencia en S3.
"""

import os
from dataclasses import dataclass
from pyspark.sql import SparkSession


@dataclass(frozen=True)
class PathConfig:
    base_path: str

    @property
    def bronze_path(self) -> str:
        return f"{self.base_path}/bronze"

    @property
    def silver_path(self) -> str:
        return f"{self.base_path}/silver"

    @property
    def gold_path(self) -> str:
        return f"{self.base_path}/gold"

    @classmethod
    def from_bucket(cls, bucket_or_path: str) -> "PathConfig":
        if bucket_or_path.startswith("s3://") or bucket_or_path.startswith("s3a://"):
            normalized = bucket_or_path.replace("s3://", "s3a://")
        elif bucket_or_path.startswith("file://") or "/" in bucket_or_path or "\\" in bucket_or_path:
            normalized = bucket_or_path
        else:
            normalized = f"s3a://{bucket_or_path}"
        return cls(base_path=normalized.rstrip("/"))


class SparkSessionFactory:
    """Fábrica de sesiones Spark optimizada para Fargate Spot y almacenamiento idempotente en S3."""

    @staticmethod
    def create(app_name: str = "VuelaData-Medallion-ETL") -> SparkSession:
        builder = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

        # Configuración de endpoint personalizado (LocalStack / MinIO en pruebas)
        endpoint = os.getenv("AWS_ENDPOINT_URL")
        if endpoint:
            builder = builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)

        return builder.getOrCreate()

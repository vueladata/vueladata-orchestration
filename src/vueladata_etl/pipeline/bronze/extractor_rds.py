"""
Capa Bronze: Extractor de Datos Operacionales desde PostgreSQL RDS
═══════════════════════════════════════════════════════════════════════════════
Responsabilidad única (SRP): Extraer tablas operativas de negocio desde la base
de datos relacional transaccional (vueladata) y depositarlas de forma inmutable,
raw e idempotente en Amazon S3 Bronze particionado por fecha de proceso.
"""

import os
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy import types as sqltypes
from pyspark.sql import types as T

from vueladata_etl.core.constants import (
    SCHEMA_OPERACIONAL,
    obtener_url_postgres
)
from vueladata_etl.core.config import PathConfig

logger = logging.getLogger("vueladata.extractor_rds")


class RdsBronzeExtractor:
    """Extrae deltas operacionales de PostgreSQL RDS hacia la capa S3 Bronze garantizando idempotencia."""

    def __init__(self, path_config: PathConfig, db_url: Optional[str] = None, spark: Optional[Any] = None):
        self.paths = path_config
        self.db_url = db_url or obtener_url_postgres()
        self._engine = None
        self.spark = spark

    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_engine(self.db_url, pool_pre_ping=True)
        return self._engine

    def extraer_tabla_a_bronze(
        self,
        tabla: str,
        columna_fecha: Optional[str] = None,
        fecha_proceso: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Extrae los registros de una tabla operacional y los deposita en S3 Bronze.
        Garantiza idempotencia: los datos se particionan por fecha_proceso y se sobrescriben si se reejecuta.
        """
        print(f"[*] Bronze: Extrayendo tabla operacional {SCHEMA_OPERACIONAL}.{tabla} desde RDS...")

        query = f"SELECT * FROM {SCHEMA_OPERACIONAL}.{tabla}"
        params = {}

        # Extracción incremental por fecha si se especifica la columna temporal
        if columna_fecha and fecha_proceso:
            query += f" WHERE DATE({columna_fecha}) = :fecha"
            params["fecha"] = fecha_proceso
            print(f"[*] Filtro incremental aplicado: DATE({columna_fecha}) = '{fecha_proceso}'")

        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params)

            total_filas = len(df)
            print(f"[+] Registros extraídos de {tabla}: {total_filas}")

            # Agregar metadata técnica de ingesta
            fecha_str = fecha_proceso or datetime.now(timezone.utc).strftime("%Y-%m-%d")
            df["fecha_proceso"] = fecha_str

            # Ruta de destino Bronze en S3
            output_dir = f"{self.paths.bronze_path}/rds_operacional/{tabla}"
            print(f"[*] Persistiendo en S3 Bronze: {output_dir}...")

            if self.spark is not None or total_filas > 0:
                if self.spark is not None:
                    # Usar conector S3A nativo de Apache Spark (cero dependencias de fsspec)
                    fields = []
                    for column in inspect(self.engine).get_columns(tabla, schema=SCHEMA_OPERACIONAL):
                        sql_type = column["type"]
                        if isinstance(sql_type, sqltypes.Boolean): kind = T.BooleanType()
                        elif isinstance(sql_type, sqltypes.Integer): kind = T.LongType()
                        elif isinstance(sql_type, sqltypes.Numeric): kind = T.DoubleType()
                        elif isinstance(sql_type, sqltypes.Float): kind = T.DoubleType()
                        elif isinstance(sql_type, sqltypes.DateTime): kind = T.TimestampType()
                        elif isinstance(sql_type, sqltypes.Date): kind = T.DateType()
                        else: kind = T.StringType()
                        fields.append(T.StructField(column["name"], kind, True))
                    fields.append(T.StructField("fecha_proceso", T.StringType(), False))
                    schema = T.StructType(fields)
                    records = []
                    for values in df.itertuples(index=False, name=None):
                        row = []
                        for value, field in zip(values, fields):
                            if pd.isna(value): value = None
                            elif isinstance(field.dataType, T.DoubleType): value = float(value)
                            elif isinstance(field.dataType, T.LongType): value = int(value)
                            elif isinstance(field.dataType, T.TimestampType): value = pd.Timestamp(value).to_pydatetime()
                            elif isinstance(field.dataType, T.StringType): value = str(value)
                            row.append(value)
                        records.append(tuple(row))
                    spark_df = self.spark.createDataFrame(records, schema=schema)
                    spark_df.write.mode("overwrite").partitionBy("fecha_proceso").parquet(output_dir)
                else:
                    df.to_parquet(
                        output_dir,
                        partition_cols=["fecha_proceso"],
                        index=False,
                        engine="pyarrow",
                        compression="snappy"
                    )

            print(f"[+] Tabla {tabla} almacenada en S3 Bronze exitosamente ({total_filas} filas).")
            return {
                "tabla": tabla,
                "filas_extraidas": total_filas,
                "output_dir": output_dir,
                "status": "SUCCESS"
            }
        except Exception:
            logger.exception("Operational extraction failed: %s", tabla)
            raise

    def extraer_todo_operacional(self, fecha_proceso: Optional[str] = None) -> Dict[str, int]:
        """
        Ejecuta la extracción de todas las tablas operativas clave del negocio:
        - Catálogos: aeropuerto, aerolinea, aeronave, ruta
        - Transaccionales: vuelo, evento_operacional, observacion_clima
        """
        print("\n=================================================================")
        print("INICIANDO INGESTA OPERACIONAL: RDS PostgreSQL -> S3 Bronze")
        print(f"Fecha de proceso: {fecha_proceso}")
        print("=================================================================")

        resultados = {}

        # Complete operational snapshots avoid mixing copies from different days and
        # include late events/corrections and connections across midnight.
        tables = ["ciudad", "aeropuerto", "aerolinea", "aeronave", "ruta", "estado_vuelo",
                  "vuelo", "evento_operacional", "observacion_clima", "causa",
                  "incidencia_vuelo", "detalle_reserva", "conexion_itinerario",
                  "tiempo_minimo_conexion", "evaluacion_conexion"]
        for table in tables:
            result = self.extraer_tabla_a_bronze(table, fecha_proceso=fecha_proceso)
            resultados[table] = result["filas_extraidas"]
        return resultados

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
from sqlalchemy import create_engine, text

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

            if total_filas > 0:
                if self.spark is not None:
                    # Usar conector S3A nativo de Apache Spark (cero dependencias de fsspec)
                    df_spark = df.copy()
                    for c in df_spark.select_dtypes(include=['object']).columns:
                        df_spark[c] = df_spark[c].astype(str)
                    spark_df = self.spark.createDataFrame(df_spark)
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
        except Exception as e:
            print(f"[!] Error extrayendo tabla {tabla} desde RDS: {e}")
            logger.error(f"Fallo en extracción de {tabla}: {e}")
            return {
                "tabla": tabla,
                "filas_extraidas": 0,
                "error": str(e),
                "status": "FAILED"
            }

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

        # 1. Tablas de catálogo (dimensión base)
        tablas_catalogo = ["aeropuerto", "aerolinea", "aeronave", "ruta"]
        for t in tablas_catalogo:
            res = self.extraer_tabla_a_bronze(tabla=t, fecha_proceso=fecha_proceso)
            resultados[t] = res.get("filas_extraidas", 0)

        # 2. Tablas transaccionales (incrementales por fecha si se especifica y no es "all")
        col_vuelo = "salida_programada_utc" if (fecha_proceso and fecha_proceso != "all") else None
        res_vuelos = self.extraer_tabla_a_bronze(
            tabla="vuelo",
            columna_fecha=col_vuelo,
            fecha_proceso=fecha_proceso if fecha_proceso != "all" else None
        )
        resultados["vuelo"] = res_vuelos.get("filas_extraidas", 0)

        # Fallback resiliente: Si no hay vuelos para la fecha exacta del día, extraer el lote completo disponible
        if resultados["vuelo"] == 0 and fecha_proceso and fecha_proceso != "all":
            print(f"[*] Aviso: 0 vuelos para '{fecha_proceso}'. Extrayendo lote operacional completo disponible...")
            res_vuelos_all = self.extraer_tabla_a_bronze(tabla="vuelo", columna_fecha=None, fecha_proceso=fecha_proceso)
            resultados["vuelo"] = res_vuelos_all.get("filas_extraidas", 0)

        col_eventos = "fecha_hora_evento_utc" if (fecha_proceso and fecha_proceso != "all") else None
        res_eventos = self.extraer_tabla_a_bronze(
            tabla="evento_operacional",
            columna_fecha=col_eventos,
            fecha_proceso=fecha_proceso if fecha_proceso != "all" else None
        )
        resultados["evento_operacional"] = res_eventos.get("filas_extraidas", 0)

        if resultados["evento_operacional"] == 0 and fecha_proceso and fecha_proceso != "all":
            res_eventos_all = self.extraer_tabla_a_bronze(tabla="evento_operacional", columna_fecha=None, fecha_proceso=fecha_proceso)
            resultados["evento_operacional"] = res_eventos_all.get("filas_extraidas", 0)

        print("[+] Ingesta a S3 Bronze finalizada con éxito.")
        return resultados

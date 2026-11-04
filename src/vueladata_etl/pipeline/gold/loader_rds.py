"""
Capa Gold: Cargador hacia la Base de Datos Analítica (Serving Layer)
═══════════════════════════════════════════════════════════════════════════════
Responsabilidad única (SRP): Sincronizar los cubos analíticos y tablas de hechos
calculados por Spark hacia el esquema relacional vueladata_gold en PostgreSQL RDS.
Garantiza idempotencia total mediante borrado previo por partición temporal en transacción.

Inspirado en el patrón de carga analítica con SQLAlchemy y pandas.
"""

from typing import Optional, Dict
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from pyspark.sql import DataFrame

from vueladata_etl.core.constants import (
    SCHEMA_GOLD,
    TABLE_FACT_VUELO,
    TABLE_KPI_OTP,
    TABLE_KPI_PROPAGACION,
    obtener_url_postgres
)

logger = logging.getLogger("vueladata.gold_loader")


class GoldRdsLoader:
    """Carga los modelos analíticos de la capa Gold hacia PostgreSQL RDS de forma idempotente."""

    def __init__(self, db_url: Optional[str] = None):
        self.db_url = db_url or obtener_url_postgres()
        self._engine = None

    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_engine(self.db_url, pool_pre_ping=True)
        return self._engine

    def cargar_fact_vuelos(self, df_fact_spark: DataFrame, limit: int = 10000) -> int:
        """
        Carga la tabla de hechos individual enriquecida en vueladata_gold.fact_vuelo_analitico.
        Idempotente: borra por fecha antes de insertar.
        """
        print(f"[*] Gold Loader: Preparando carga de hechos hacia {SCHEMA_GOLD}.{TABLE_FACT_VUELO}...")

        # Convertir a Pandas para carga bulk con SQLAlchemy
        df_pdf = df_fact_spark.limit(limit).toPandas()
        if df_pdf.empty:
            print("[!] Gold Loader: No hay registros de hechos para cargar.")
            return 0

        # Mapeo y preparación de columnas
        if "salida_programada_ts" in df_pdf.columns and df_pdf["salida_programada_ts"].notna().any():
            fecha_salida_series = pd.to_datetime(df_pdf["salida_programada_ts"]).dt.date
        elif "fecha_vuelo" in df_pdf.columns and df_pdf["fecha_vuelo"].notna().any():
            fecha_salida_series = pd.to_datetime(df_pdf["fecha_vuelo"]).dt.date
        elif "anio" in df_pdf.columns and "mes" in df_pdf.columns and "dia" in df_pdf.columns:
            fecha_salida_series = pd.to_datetime(
                df_pdf["anio"].astype(str) + "-" + 
                df_pdf["mes"].astype(str).str.zfill(2) + "-" + 
                df_pdf["dia"].astype(str).str.zfill(2)
            ).dt.date
        else:
            fecha_salida_series = pd.to_datetime("2026-01-01").date()

        df_cargar = pd.DataFrame({
            "id_vuelo": df_pdf.get("id_vuelo", df_pdf.index.astype(str)).astype(str),
            "numero_vuelo": df_pdf["numero_vuelo"].astype(str),
            "codigo_aerolinea": df_pdf["codigo_aerolinea"].astype(str),
            "matricula_aeronave": df_pdf.get("matricula_aeronave", None),
            "aeropuerto_origen": df_pdf["aeropuerto_origen"].astype(str),
            "aeropuerto_destino": df_pdf["aeropuerto_destino"].astype(str),
            "fecha_salida": fecha_salida_series,
            "retraso_salida_min": df_pdf.get("retraso_salida_min", 0.0),
            "retraso_llegada_min": df_pdf.get("retraso_llegada_min", 0.0),
            "es_cancelado": df_pdf.get("cancelado", 0),
            "es_desviado": df_pdf.get("desviado", 0),
            "es_operado": df_pdf.get("es_operado", 1),
            "es_puntual_otp": df_pdf.get("es_puntual_otp", 0),
            "categoria_retraso": df_pdf.get("categoria_retraso", "NORMAL"),
            "retraso_propagado_estimado": df_pdf.get("retraso_propagado_estimado", 0.0),
            "es_retraso_propagado": df_pdf.get("es_retraso_propagado", 0)
        })

        fechas_unicas = df_cargar["fecha_salida"].dropna().unique().tolist()

        try:
            with self.engine.begin() as conn:
                for f in fechas_unicas:
                    conn.execute(
                        text(f"DELETE FROM {SCHEMA_GOLD}.{TABLE_FACT_VUELO} WHERE fecha_salida = :fecha"),
                        {"fecha": f}
                    )
                df_cargar.to_sql(
                    name=TABLE_FACT_VUELO,
                    con=conn,
                    schema=SCHEMA_GOLD,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=2000
                )
            total = len(df_cargar)
            print(f"[+] Gold Loader: {total:,} hechos analíticos cargados exitosamente.")
            return total
        except Exception as e:
            print(f"[!] Error cargando fact_vuelo_analitico: {e}")
            logger.error(f"Fallo en carga de fact_vuelo_analitico: {e}")
            return 0

    def cargar_kpi_otp(self, df_kpi_spark: DataFrame) -> int:
        """
        Carga el cubo analítico de puntualidad en vueladata_gold.kpi_otp_aerolinea_mensual.
        Idempotente: sobrescribe las combinaciones de (anio, mes) en transacción.
        """
        print(f"[*] Gold Loader: Sincronizando métricas OTP hacia {SCHEMA_GOLD}.{TABLE_KPI_OTP}...")

        df_pdf = df_kpi_spark.toPandas()
        if df_pdf.empty:
            print("[!] Gold Loader: Cubo OTP vacío.")
            return 0

        anios_meses = df_pdf[["anio", "mes"]].drop_duplicates().to_dict(orient="records")

        try:
            with self.engine.begin() as conn:
                for am in anios_meses:
                    conn.execute(
                        text(f"DELETE FROM {SCHEMA_GOLD}.{TABLE_KPI_OTP} WHERE anio = :anio AND mes = :mes"),
                        {"anio": int(am["anio"]), "mes": int(am["mes"])}
                    )
                cols_otp = [
                    "anio", "mes", "codigo_aerolinea", "aeropuerto_origen",
                    "total_vuelos_programados", "total_vuelos_cancelados", "total_vuelos_desviados",
                    "total_vuelos_operados", "total_vuelos_puntuales",
                    "promedio_retraso_llegada_min", "promedio_retraso_salida_min",
                    "tasa_otp_porcentaje", "fecha_actualizacion_utc"
                ]
                df_cargar = df_pdf[[c for c in cols_otp if c in df_pdf.columns]]
                df_cargar.to_sql(
                    name=TABLE_KPI_OTP,
                    con=conn,
                    schema=SCHEMA_GOLD,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=2000
                )
            total = len(df_cargar)
            print(f"[+] Gold Loader: {total:,} registros de métricas OTP sincronizados.")
            return total
        except Exception as e:
            print(f"[!] Error cargando kpi_otp: {e}")
            logger.error(f"Fallo en carga de kpi_otp: {e}")
            return 0

    def cargar_kpi_propagacion(self, df_prop_spark: DataFrame) -> int:
        """
        Carga el cubo analítico de propagación de flota en vueladata_gold.kpi_propagacion_flota.
        """
        print(f"[*] Gold Loader: Sincronizando métricas de efecto dominó hacia {SCHEMA_GOLD}.{TABLE_KPI_PROPAGACION}...")

        df_pdf = df_prop_spark.toPandas()
        if df_pdf.empty:
            print("[!] Gold Loader: Cubo de propagación vacío.")
            return 0

        anios_meses = df_pdf[["anio", "mes"]].drop_duplicates().to_dict(orient="records")

        try:
            with self.engine.begin() as conn:
                for am in anios_meses:
                    conn.execute(
                        text(f"DELETE FROM {SCHEMA_GOLD}.{TABLE_KPI_PROPAGACION} WHERE anio = :anio AND mes = :mes"),
                        {"anio": int(am["anio"]), "mes": int(am["mes"])}
                    )
                cols_destino = [
                    "anio", "mes", "matricula_aeronave",
                    "total_vuelos_operados", "total_vuelos_con_retraso_propagado",
                    "retraso_propagado_promedio_min", "tasa_vulnerabilidad_rotacion_pct",
                    "fecha_actualizacion_utc"
                ]
                df_cargar = df_pdf[[c for c in cols_destino if c in df_pdf.columns]]
                df_cargar.to_sql(
                    name=TABLE_KPI_PROPAGACION,
                    con=conn,
                    schema=SCHEMA_GOLD,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=2000
                )
            total = len(df_cargar)
            print(f"[+] Gold Loader: {total:,} registros de propagación de flota sincronizados.")
            return total
        except Exception as e:
            print(f"[!] Error cargando kpi_propagacion_flota: {e}")
            logger.error(f"Fallo en carga de kpi_propagacion: {e}")
            return 0

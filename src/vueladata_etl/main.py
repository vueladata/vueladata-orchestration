"""
Punto de Entrada CLI del Pipeline Medallion VuelaData
═══════════════════════════════════════════════════════════════════════════════
Orquesta el ciclo de vida completo:
  1. Ingesta operacional: RDS PostgreSQL -> S3 Bronze
  2. Curaduría y control de calidad: S3 Bronze -> S3 Silver (con desvío a Cuarentena)
  3. Modelado analítico y reglas de negocio: S3 Silver -> S3 Gold (RN-01 OTP y RN-08)
  4. Sincronización analítica: S3 Gold -> RDS PostgreSQL (vueladata_gold)
  5. Registro de observabilidad en vueladata_audit.pipeline_execution_log

Inspirado en mejores prácticas de ingeniería de datos y arquitectura limpia.
"""

import sys
import os
import argparse
from datetime import datetime, timezone

from vueladata_etl.core.config import SparkSessionFactory, PathConfig
from vueladata_etl.core.audit import AuditManager
from vueladata_etl.pipeline.bronze.extractor_rds import RdsBronzeExtractor
from vueladata_etl.pipeline.bronze.reader import BronzeReader
from vueladata_etl.pipeline.silver.transformer import SilverTransformer
from vueladata_etl.pipeline.gold.aggregator import GoldAggregator
from vueladata_etl.pipeline.gold.loader_rds import GoldRdsLoader


def parse_args():
    parser = argparse.ArgumentParser(description="VuelaData Medallion Lakehouse ETL Pipeline")
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="Capa objetivo: bronze, silver, gold o all"
    )
    parser.add_argument(
        "--mode",
        choices=["daily", "historical", "all"],
        default="all",
        help="Modo de ejecución: daily (lote diario), historical (F1 Kaggle 5.8M) o all"
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("S3_DATA_LAKE_BUCKET"),
        required=not bool(os.getenv("S3_DATA_LAKE_BUCKET")),
        help="Bucket S3 o ruta base del Data Lake"
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Fecha de proceso (YYYY-MM-DD) para el lote diario"
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="ID de la corrida de Airflow (run_id) para trazabilidad en auditoría"
    )
    parser.add_argument(
        "--dag-id",
        default="dag_daily_batch_spark",
        help="Identificador del DAG orquestador"
    )
    parser.add_argument("--sync-rds", action="store_true", help="Optional relational serving copy; Gold S3 is the analytical source")
    return parser.parse_args()


def main():
    args = parse_args()
    path_config = PathConfig.from_bucket(args.bucket)
    audit = AuditManager()

    fecha_proceso = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_id = args.run_id or f"manual__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"

    print("═════════════════════════════════════════════════════════════════")
    print("  VuelaData Lakehouse ETL — Pipeline Medallion con Observabilidad")
    print(f"  Capa: {args.layer.upper()} | Modo: {args.mode.upper()}")
    print(f"  Fecha: {fecha_proceso} | Run ID: {run_id}")
    print(f"  Ruta base S3: {path_config.base_path}")
    print("═════════════════════════════════════════════════════════════════")

    # 1. Registrar inicio en la base de datos de auditoría
    id_ejecucion = audit.registrar_inicio(
        dag_id=args.dag_id,
        run_id=run_id,
        capa=args.layer,
        fecha_proceso=fecha_proceso
    )

    spark = None
    filas_leidas = 0
    filas_escritas = 0
    filas_rechazadas = 0

    try:
        # ── FASE 1: INICIALIZAR SPARK PARA EL PIPELINE MEDALLÓN ──────────────
        spark = SparkSessionFactory.create(app_name=f"VuelaData-{args.layer.capitalize()}-{args.mode.capitalize()}")
        reader = BronzeReader(spark)
        silver_transformer = SilverTransformer(reader, path_config, audit_manager=audit)
        gold_aggregator = GoldAggregator(spark, path_config)
        gold_loader = GoldRdsLoader()

        # ── FASE 2: INGESTA OPERACIONAL (RDS -> S3 Bronze) ───────────────────
        if args.layer in ["bronze", "all"] and args.mode in ["daily", "all"]:
            print("\n>>> [1/4] INICIANDO INGESTA OPERACIONAL: RDS -> S3 BRONZE <<<")
            extractor_rds = RdsBronzeExtractor(path_config, spark=spark)
            res_ingesta = extractor_rds.extraer_todo_operacional(fecha_proceso=fecha_proceso)
            filas_leidas += sum(res_ingesta.values())

        df_silver_flights = None

        # ── FASE 3: CAPA SILVER (Limpieza, Validación y Cuarentena) ──────────
        if args.layer in ["silver", "all"]:
            print("\n>>> [2/4] INICIANDO PROCESAMIENTO CAPA SILVER CON CALIDAD <<<")
            df_silver_flights = silver_transformer.process_flights(
                mode=args.mode,
                filter_date=fecha_proceso,
                id_ejecucion=id_ejecucion,
                dag_id=args.dag_id
            )
            if args.mode in ["daily", "all"]:
                silver_transformer.process_events()

        # ── FASE 4: CAPA GOLD (Reglas de Negocio RN-01 y RN-08) ──────────────
        if args.layer in ["gold", "all"]:
            print("\n>>> [3/4] INICIANDO MODELADO CAPA GOLD (RN-01 y RN-08) <<<")
            df_gold = gold_aggregator.process_gold_metrics(df_silver_flights)

            # RDS serving is optional; never silently publish a 5,000-row sample.
            if args.sync_rds:
                if df_gold.count() > 10000:
                    raise ValueError("Relational serving export exceeds 10,000 rows; use S3 Gold/Athena or a dedicated bulk loader")
                filas_escritas += gold_loader.cargar_fact_vuelos(df_gold, limit=10000)
                filas_escritas += gold_loader.cargar_kpi_otp(spark.read.parquet(f"{path_config.gold_path}/puntualidad_aerolineas"))
                filas_escritas += gold_loader.cargar_kpi_propagacion(spark.read.parquet(f"{path_config.gold_path}/propagacion_flota"))

        # 2. Registrar finalización exitosa en la base de datos de auditoría
        audit.registrar_fin(
            id_ejecucion=id_ejecucion,
            filas_leidas=filas_leidas,
            filas_escritas=filas_escritas,
            filas_rechazadas=filas_rechazadas,
            estado="SUCCESS",
            mensaje="Pipeline Medallion finalizado; S3 Gold publicado"
        )

        print("\n═════════════════════════════════════════════════════════════════")
        print(f"[+] Pipeline finalizado con éxito total para capa: {args.layer.upper()}")
        print("═════════════════════════════════════════════════════════════════")

    except Exception as e:
        print(f"\n[!] Error crítico durante la ejecución del pipeline: {e}", file=sys.stderr)
        audit.registrar_fin(
            id_ejecucion=id_ejecucion,
            filas_leidas=filas_leidas,
            filas_escritas=filas_escritas,
            filas_rechazadas=filas_rechazadas,
            estado="FAILED",
            mensaje=f"Error en ejecución: {str(e)[:400]}"
        )
        raise e
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()

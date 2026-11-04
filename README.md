# VuelaData - Orquestación con Airflow 3.x y Lakehouse Medallion con Spark

Este repositorio contiene la arquitectura de orquestación analítica y procesamiento de datos para la plataforma **VuelaData**, integrando **Apache Airflow 3.1.5** (ejecución distribuida con Celery y RabbitMQ en AWS ECS Fargate) y **Apache Spark 3.5.1** (tareas efímeras Medallion en Fargate Spot).

---

## 1. Arquitectura Limpia y Estructura del Proyecto

El proyecto sigue los principios de **Clean Architecture** y el patrón **Medallion Lakehouse**, desacoplando responsabilidades (Single Responsibility Principle - SRP):

```
vueladata-airflow/
├── dags/                                      # Orquestadores de flujo (Airflow 3.x)
│   ├── dag_daily_batch_spark.py               # Lote diario madrugada (@daily)
│   ├── dag_f1_historico_spark.py              # Lote bajo demanda Kaggle 5.8M vuelos
│   ├── dag_ingesta_externa_lambdas.py         # Orquestación de Lambdas OpenSky/OpenMeteo
│   └── hu11_prueba_celery.py                  # Validación de CeleryExecutor distribuido
│
├── src/                                       # Código fuente del Motor Analítico (PySpark)
│   └── vueladata_etl/
│       ├── core/                              # Dominio y configuraciones compartidas
│       │   ├── config.py                      # Fábrica SparkSession y rutas S3/Local
│       │   ├── constants.py                   # Reglas de negocio (OTP <= 15m, Turnaround 45m/60m)
│       │   └── schemas/                       # Esquemas explícitos StructType (HU-12)
│       │       ├── f1_schemas.py              # Esquemas Kaggle (flights, airlines, airports)
│       │       ├── f6_schemas.py              # Esquemas Rotación (itinerary, fleet, routes)
│       │       └── event_schemas.py           # Esquemas Worker Lake (OOOI, pagos, clima, opensky)
│       │
│       ├── pipeline/                          # Etapas desacopladas del pipeline Medallion
│       │   ├── bronze/                        # CAPA BRONZE: Ingesta y lectura tipada en bruto
│       │   │   ├── reader.py                  # Lector con esquemas explícitos sin inferencia
│       │   │   └── validator.py               # Validador de existencia y precondiciones
│       │   ├── silver/                        # CAPA SILVER: Limpieza, estandarización y curaduría
│       │   │   ├── cleaner_flights.py         # Normalización y tipado de vuelos F1 y F6
│       │   │   ├── cleaner_events.py          # Tipado de eventos OOOI, pagos y clima
│       │   │   └── transformer.py             # Persistencia de datasets limpios en Parquet
│       │   └── gold/                          # CAPA GOLD: Reglas de negocio y analítica
│       │       ├── rules_otp.py               # Motor de regla RN-01 (Puntualidad OTP <= 15 min)
│       │       ├── rules_propagation.py       # Motor de regla RN-08 (Propagación retrasos en flota)
│       │       └── aggregator.py              # Persistencia de métricas y KPIs en Parquet
│       │
│       └── main.py                            # CLI de ejecución del pipeline (--layer, --mode)
│
├── infrastructure/                            # Infraestructura como Código y Despliegue
│   ├── docker/
│   │   ├── airflow/
│   │   │   └── Dockerfile                     # Imagen Airflow 3.1.5 + Celery + Amazon Providers
│   │   └── spark/
│   │       ├── Dockerfile                     # Imagen PySpark 3.5.1 + Conectores S3A Hadoop
│   │       ├── entrypoint.sh                  # Entrypoint efímero para Fargate Spot
│   │       └── requirements.txt               # Dependencias Python mínimas (sin pandas)
│   └── aws/
│       ├── ecs/                               # ECS Task Definitions (Fargate & Fargate Spot)
│       │   ├── task-airflow-webserver.json
│       │   ├── task-airflow-scheduler.json
│       │   ├── task-airflow-worker.json
│       │   ├── task-rabbitmq.json
│       │   └── task-spark-spot.json
│       ├── iam/                               # Políticas de permisos IAM
│       │   └── policy-airflow-task.json
│       ├── rds/                               # Scripts SQL para PostgreSQL RDS
│       │   └── init_airflow_db_schema.sql     # Esquema aislado 'airflow_db'
│       └── scripts/                           # Automatizaciones en Bash y PowerShell
│           ├── build_and_push_ecr.ps1
│           ├── deploy_ecs_services.ps1
│           └── upload_bronze_datasets.ps1
│
├── docker-compose.yml                         # Entorno de pruebas local Airflow 3 distribuido
├── .env.example                               # Plantilla de variables de entorno
└── requirements.txt                           # Dependencias raíz para Airflow
```

---

## 2. Responsabilidades por Capas (Medallion)

1. **Bronze (`src/vueladata_etl/pipeline/bronze/`)**:
   - Lectura exclusiva desde `s3a://.../bronze/` aplicando esquemas tipados (`StructType`).
   - Prohibida la inferencia de tipos (`inferSchema=False`) para optimizar velocidad y memoria.
2. **Silver (`src/vueladata_etl/pipeline/silver/`)**:
   - Limpieza, filtrado de anomalías, normalización de columnas (Kaggle inglés -> VuelaData español).
   - Conversión de fechas/horas a `TimestampType`, deduplicación de eventos por ID idempotente.
   - Persistencia en `s3a://.../silver/` en formato Parquet comprimido con Snappy.
3. **Gold (`src/vueladata_etl/pipeline/gold/`)**:
   - **Regla RN-01**: Cálculo de puntualidad OTP (retraso llegada $\le 15$ min en vuelos operados).
   - **Regla RN-08**: Cálculo de propagación y efecto dominó en rotación de flota restando tiempo mínimo de escala (45 min nacional / 60 min internacional).
   - Persistencia en `s3a://.../gold/` en formato Parquet particionado.

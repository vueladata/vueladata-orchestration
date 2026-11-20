# Orquestación de VuelaData con Airflow y Spark

Airflow 3.1.5 usa CeleryExecutor con RabbitMQ. Ejecuta Apache Spark 3.5.1 en tareas efímeras de ECS Fargate Spot para transformar los datos de Bronze a Silver y Gold. [Arquitectura, ADR y accesos de AWS](https://vueladata.github.io/documentacion/docs/).

| DAG | Schedule y etapas |
| --- | --- |
| `dag_daily_batch_spark` | 03:00 UTC. Valida los prefijos de entrada y ejecuta Bronze → Silver → Gold. |
| `dag_f1_historico_spark` | Manual. Valida el F1 histórico y F6 en Bronze, luego ejecuta Silver → Gold. |
| `hu11_prueba_celery` | Prueba manual de ejecución distribuida en Celery. |

EventBridge programa OpenSky cada cinco minutos y Open-Meteo cada quince. Las funciones y su contrato están en [infrastructure/aws/lambdas](infrastructure/aws/lambdas/README.md).

## Desarrollo con Docker Compose

Clona `vueladata-api` y `vueladata-stream` junto a este repositorio porque Compose usa esos directorios como contextos de construcción. Crea `.env` a partir de `.env.example` y configura contraseñas, claves de Airflow y, si se probará correo, el buzón de pruebas. Revisa la configuración y arranca desde la raíz.

```bash
docker compose config --quiet
docker compose up -d --build
docker compose ps
```

La API usa el puerto 8000, Airflow el 8080 y el panel RabbitMQ el 15672. El puerto local PostgreSQL lo controla `POSTGRES_PORT`. Aplica el esquema operacional en el PostgreSQL local antes de usar las rutas de negocio o los workers. El Compose inicializa `airflow_db`; el esquema de negocio y el seed se documentan en [vueladata-database](https://github.com/vueladata/vueladata-database). Para detener el entorno, ejecuta `docker compose down`. El Compose permite probar el stack local. Los DAGs ETL requieren acceso AWS y las definiciones de Spark correspondientes.

## Operación en AWS

El diario comparte fecha y `run_id` entre tareas. El pool `gold_writer` debe tener un slot. La espera de Gold consume ese slot hasta que termina ECS. Gold histórico deshabilita el reintento automático para evitar lanzar un segundo escritor y compacta los archivos en staging antes de publicarlos. El worker Airflow usa concurrencia y prefetch de uno. La definición versionada de Spark asigna 4 vCPU y 16 GiB.

Las opciones del ETL se encuentran en [src/vueladata_etl/main.py](src/vueladata_etl/main.py). Usa `--mode daily|historical|all` y `--layer bronze|silver|gold|all`. `--date` define la fecha diaria y `--bucket` o `S3_DATA_LAKE_BUCKET` establece el Data Lake. Bronze diario exporta un snapshot fechado de RDS, Silver cura datos y separa registros inválidos, y Gold publica Parquet. El diario reemplaza las particiones afectadas. La publicación `--only-star-model` reutiliza Silver para escribir nueve tablas estrella sin tocar los siete conjuntos de Power BI. La copia opcional en RDS se habilita con `--sync-rds` fuera del modo de solo estrella.

Consulta el [contrato de Gold](tests/spark/gold_contract.json) y las [instrucciones de Athena y CT-10](infrastructure/aws/athena/README.md). El workflow de despliegue construye imágenes y actualiza ECS al recibir pushes a las ramas configuradas.

## Validación

El workflow ejecuta las pruebas de DAGs, despliegue y Spark con las versiones de runtime declaradas. Los detalles de la ejecución están en [.github/workflows/deploy.yml](.github/workflows/deploy.yml). Las pruebas de Lambdas están en [tests/lambdas](tests/lambdas/).

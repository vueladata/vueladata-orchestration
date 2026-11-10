#!/bin/bash
set -e

echo "================================================================="
echo "Iniciando Contenedor Efímero Spark ETL VuelaData (Fargate Spot)"
echo "Fecha y hora: $(date -u)"
echo "Argumentos: $@"
echo "================================================================="

# Limitar concurrencia a las dos vCPU provisionadas en Fargate.
/opt/spark/bin/spark-submit \
    --master "${SPARK_MASTER:-local[2]}" \
    --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY:-3g}" \
    --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS:-64}" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain" \
    --conf "spark.hadoop.fs.s3a.path.style.access=${S3A_PATH_STYLE_ACCESS:-false}" \
    --conf "spark.hadoop.fs.s3a.fast.upload=true" \
    /opt/spark/work-dir/src/vueladata_etl/main.py "$@"

EXIT_CODE=$?
echo "================================================================="
echo "Spark ETL finalizado con código de salida: ${EXIT_CODE}"
echo "La tarea efímera Fargate se cerrará automáticamente."
echo "================================================================="

exit ${EXIT_CODE}

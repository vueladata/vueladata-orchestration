#!/bin/bash
set -e

AWS_ACCOUNT_ID="${1:-$AWS_ACCOUNT_ID}"
AWS_REGION="${2:-${AWS_DEFAULT_REGION:-us-east-1}}"

if [ -z "$AWS_ACCOUNT_ID" ]; then
    echo "[!] Error: Debe definir la variable AWS_ACCOUNT_ID o pasar el ID de cuenta: ./build_and_push_ecr.sh <AWS_ACCOUNT_ID>"
    exit 1
fi

ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
AIRFLOW_REPO="${ECR_REGISTRY}/vueladata/airflow"
SPARK_REPO="${ECR_REGISTRY}/vueladata/spark-etl"

echo "========================================================="
echo "Publicación de Imágenes Docker a Amazon ECR"
echo "Registro: ${ECR_REGISTRY}"
echo "========================================================="

# 1. Login en Amazon ECR
echo "[1/4] Autenticando Docker con Amazon ECR..."
aws ecr get-login-password --region "${AWS_REGION}" | docker login --username AWS --password-stdin "${ECR_REGISTRY}"

# 2. Asegurar existencia de los repositorios ECR
echo "[2/4] Verificando repositorios ECR..."
aws ecr describe-repositories --repository-names "vueladata/airflow" --region "${AWS_REGION}" >/dev/null 2>&1 || \
aws ecr create-repository --repository-name "vueladata/airflow" --region "${AWS_REGION}"

aws ecr describe-repositories --repository-names "vueladata/spark-etl" --region "${AWS_REGION}" >/dev/null 2>&1 || \
aws ecr create-repository --repository-name "vueladata/spark-etl" --region "${AWS_REGION}"

# 3. Construir y Publicar Imagen vueladata/airflow
echo "[3/4] Construyendo y subiendo vueladata/airflow (Airflow 3.x)..."
docker build -t vueladata/airflow:3.1.5 -t "${AIRFLOW_REPO}:latest" -t "${AIRFLOW_REPO}:3.1.5" -f infrastructure/docker/airflow/Dockerfile .
docker push "${AIRFLOW_REPO}:latest"
docker push "${AIRFLOW_REPO}:3.1.5"

# 4. Construir y Publicar Imagen vueladata/spark-etl
echo "[4/4] Construyendo y subiendo vueladata/spark-etl (PySpark + S3A)..."
docker build -t vueladata/spark-etl:latest -t "${SPARK_REPO}:latest" -f infrastructure/docker/spark/Dockerfile .
docker push "${SPARK_REPO}:latest"

echo "========================================================="
echo "[+] Publicación en Amazon ECR completada con éxito."
echo "========================================================="

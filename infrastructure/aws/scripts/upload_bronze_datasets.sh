#!/bin/bash
set -e

BUCKET_NAME="${1:-vueladata-datalake-prod}"
DATA_LOCAL_DIR="${2:-../vueladata/data}"

echo "========================================================="
echo "Iniciando carga de datos a la capa S3 Bronze de VuelaData"
echo "Bucket S3 destino: s3://${BUCKET_NAME}/bronze/"
echo "========================================================="

# 1. Estructura de prefijos en S3 Bronze
aws s3api put-object --bucket "${BUCKET_NAME}" --key "bronze/f1_kaggle/"
aws s3api put-object --bucket "${BUCKET_NAME}" --key "bronze/f6_rotacion/"
aws s3api put-object --bucket "${BUCKET_NAME}" --key "bronze/events/oooi/"
aws s3api put-object --bucket "${BUCKET_NAME}" --key "bronze/events/pagos/"
aws s3api put-object --bucket "${BUCKET_NAME}" --key "bronze/events/clima/"
aws s3api put-object --bucket "${BUCKET_NAME}" --key "bronze/events/opensky/"

# 2. Cargar Fuente F1
if [ -d "${DATA_LOCAL_DIR}/reference" ]; then
    aws s3 cp "${DATA_LOCAL_DIR}/reference/airlines.csv" "s3://${BUCKET_NAME}/bronze/f1_kaggle/airlines.csv"
    aws s3 cp "${DATA_LOCAL_DIR}/reference/airports.csv" "s3://${BUCKET_NAME}/bronze/f1_kaggle/airports.csv"
fi

if [ -f "${DATA_LOCAL_DIR}/flights.csv" ]; then
    aws s3 cp "${DATA_LOCAL_DIR}/flights.csv" "s3://${BUCKET_NAME}/bronze/f1_kaggle/flights.csv"
fi

# 3. Cargar Fuente F6
if [ -d "${DATA_LOCAL_DIR}/generated" ]; then
    aws s3 sync "${DATA_LOCAL_DIR}/generated/" "s3://${BUCKET_NAME}/bronze/f6_rotacion/"
fi

echo "========================================================="
echo "[+] Carga a S3 Bronze completada con éxito."
echo "========================================================="

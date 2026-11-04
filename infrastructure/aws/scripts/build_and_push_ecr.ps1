param (
    [string]$AwsAccountId = "239700238526",
    [string]$AwsRegion = "us-east-1"
)

$EcrRegistry = "$AwsAccountId.dkr.ecr.$AwsRegion.amazonaws.com"
$AirflowRepo = "$EcrRegistry/vueladata/airflow"
$SparkRepo = "$EcrRegistry/vueladata/spark-etl"
$RabbitRepo = "$EcrRegistry/vueladata/rabbitmq"

$AirflowLatest = "${AirflowRepo}:latest"
$AirflowVersion = "${AirflowRepo}:3.1.5"
$SparkLatest = "${SparkRepo}:latest"
$RabbitLatest = "${RabbitRepo}:latest"
$RabbitVersion = "${RabbitRepo}:3-management"

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "Publicación de Imágenes Docker a Amazon ECR" -ForegroundColor Cyan
Write-Host "Registro: $EcrRegistry" -ForegroundColor Yellow
Write-Host "=========================================================" -ForegroundColor Cyan

# 1. Login en Amazon ECR
Write-Host "[1/5] Autenticando Docker con Amazon ECR..." -ForegroundColor Green
aws ecr get-login-password --region $AwsRegion | docker login --username AWS --password-stdin $EcrRegistry

# 2. Asegurar existencia de los repositorios ECR
Write-Host "[2/5] Verificando repositorios ECR..." -ForegroundColor Green
function Ensure-EcrRepo($name) {
    $null = aws ecr describe-repositories --repository-names $name --region $AwsRegion 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  -> Creando repositorio ECR $name..."
        aws ecr create-repository --repository-name $name --region $AwsRegion | Out-Null
    } else {
        Write-Host "  -> Repositorio $name ya existe."
    }
}

Ensure-EcrRepo "vueladata/airflow"
Ensure-EcrRepo "vueladata/spark-etl"
Ensure-EcrRepo "vueladata/rabbitmq"

# 3. Construir y Publicar Imagen vueladata/airflow (HU-11)
Write-Host "[3/5] Construyendo y subiendo vueladata/airflow (Airflow 3.1.5)..." -ForegroundColor Green
docker build -t "vueladata/airflow:3.1.5" -t $AirflowLatest -t $AirflowVersion -f infrastructure/docker/airflow/Dockerfile .
if ($LASTEXITCODE -ne 0) { throw "Error al compilar imagen vueladata/airflow" }
docker push $AirflowLatest
docker push $AirflowVersion

# 4. Construir y Publicar Imagen vueladata/spark-etl (HU-12 / HU-27)
Write-Host "[4/5] Construyendo y subiendo vueladata/spark-etl (PySpark + S3A)..." -ForegroundColor Green
docker build -t "vueladata/spark-etl:latest" -t $SparkLatest -f infrastructure/docker/spark/Dockerfile .
if ($LASTEXITCODE -ne 0) { throw "Error al compilar imagen vueladata/spark-etl" }
docker push $SparkLatest

# 5. Publicar Imagen vueladata/rabbitmq (HU-11 Broker)
Write-Host "[5/5] Subiendo vueladata/rabbitmq a Amazon ECR..." -ForegroundColor Green
docker tag rabbitmq:3-management $RabbitVersion
docker tag rabbitmq:3-management $RabbitLatest
docker push $RabbitVersion
docker push $RabbitLatest

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "[+] Publicación en Amazon ECR completada con éxito." -ForegroundColor Green
Write-Host "=========================================================" -ForegroundColor Cyan

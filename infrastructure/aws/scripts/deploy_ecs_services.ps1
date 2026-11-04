param (
    [string]$AwsAccountId = "239700238526",
    [string]$AwsRegion = "us-east-1",
    [string]$ClusterName = "vueladata-cluster",
    [string]$SubnetId = "subnet-063e1dbea89657b96",
    [string]$SecurityGroupId = "sg-0c39aef51177c31de"
)

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "Registro de Task Definitions y Despliegue en AWS ECS" -ForegroundColor Cyan
Write-Host "Cluster: $ClusterName | Región: $AwsRegion" -ForegroundColor Yellow
Write-Host "=========================================================" -ForegroundColor Cyan

# 1. Asegurar existencia del Cluster ECS
Write-Host "[1/8] Verificando ECS Cluster '$ClusterName'..." -ForegroundColor Green
$null = aws ecs describe-clusters --clusters $ClusterName --region $AwsRegion 2>&1
if ($LASTEXITCODE -ne 0) {
    aws ecs create-cluster --cluster-name $ClusterName --capacity-providers FARGATE FARGATE_SPOT --region $AwsRegion | Out-Null
}

$resolvedDir = "infrastructure\aws\ecs\resolved"
if (-not (Test-Path $resolvedDir)) {
    New-Item -ItemType Directory -Force -Path $resolvedDir | Out-Null
}

$utf8NoBom = New-Object System.Text.UTF8Encoding $false

function Register-TaskFile([string]$sourceRelPath, [string]$targetName) {
    $raw = Get-Content $sourceRelPath -Raw
    $replacements = @{
        '${AWS_ACCOUNT_ID}' = $AwsAccountId
        '${AWS_DEFAULT_REGION}' = $AwsRegion
        '${S3_DATA_LAKE_BUCKET}' = 'vueladata-datalake-dev-239700238526-us-east-1-an'
        '${RDS_HOST}' = 'vueladata-postgres-dev.ci9mcasuas3l.us-east-1.rds.amazonaws.com'
        '${RDS_DB}' = 'vueladata'
        '${RDS_USER}' = if ($env:RDS_USER) { $env:RDS_USER } else { 'airflow' }
        '${RDS_PASSWORD}' = if ($env:RDS_PASSWORD) { $env:RDS_PASSWORD } else { 'airflow_local_2026' }
        '${RABBITMQ_USER}' = if ($env:RABBITMQ_USER) { $env:RABBITMQ_USER } else { 'rabbit_admin' }
        '${RABBITMQ_PASSWORD}' = if ($env:RABBITMQ_PASSWORD) { $env:RABBITMQ_PASSWORD } else { 'PSW_rabbit.2' }
        '${RABBITMQ_HOST}' = if ($env:RABBITMQ_HOST) { $env:RABBITMQ_HOST } else { 'rabbitmq.vueladata.local' }
        '${AIRFLOW_FERNET_KEY}' = if ($env:AIRFLOW_FERNET_KEY) { $env:AIRFLOW_FERNET_KEY } else { 'dkoSW-HnalABxFeXIgKL4b0-SmnDocwv5cxBXjmj36Y=' }
        '${AIRFLOW_JWT_SECRET}' = if ($env:AIRFLOW_JWT_SECRET) { $env:AIRFLOW_JWT_SECRET } else { 'vueladata_jwt_secret_dev_2026' }
        '${AIRFLOW_API_SECRET_KEY}' = if ($env:AIRFLOW_API_SECRET_KEY) { $env:AIRFLOW_API_SECRET_KEY } else { 'vueladata_api_secret_key_dev_2026' }
    }

    foreach ($k in $replacements.Keys) {
        $raw = $raw.Replace($k, $replacements[$k])
    }

    $targetFile = Join-Path $resolvedDir "$targetName.json"
    [System.IO.File]::WriteAllText($targetFile, $raw, $utf8NoBom)

    $forwardPath = "file://$targetFile".Replace('\', '/')
    aws ecs register-task-definition --cli-input-json $forwardPath --region $AwsRegion | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Fallo al registrar $targetName en ECS"
    }
}

Write-Host "[2/8] Registrando Task Definition de RabbitMQ..." -ForegroundColor Green
Register-TaskFile "infrastructure\aws\ecs\task-rabbitmq.json" "task-rabbitmq"

Write-Host "[3/8] Registrando Task Definition de Airflow Webserver..." -ForegroundColor Green
Register-TaskFile "infrastructure\aws\ecs\task-airflow-webserver.json" "task-airflow-webserver"

Write-Host "[4/8] Registrando Task Definition de Airflow Scheduler..." -ForegroundColor Green
Register-TaskFile "infrastructure\aws\ecs\task-airflow-scheduler.json" "task-airflow-scheduler"

Write-Host "[5/8] Registrando Task Definition de Airflow Celery Worker..." -ForegroundColor Green
Register-TaskFile "infrastructure\aws\ecs\task-airflow-worker.json" "task-airflow-worker"

Write-Host "[6/8] Registrando Task Definition de Spark ETL Spot..." -ForegroundColor Green
Register-TaskFile "infrastructure\aws\ecs\task-spark-spot.json" "task-spark-spot"

Write-Host "[7/8] Configurando y encendiendo Servicio ECS de RabbitMQ..." -ForegroundColor Green
# Verificar si el servicio de RabbitMQ existe
$rabbitSvcCheck = aws ecs describe-services --cluster $ClusterName --services vueladata-rabbitmq-service --region $AwsRegion --query "services[?status=='ACTIVE'].serviceName" --output text
if ($rabbitSvcCheck -eq "vueladata-rabbitmq-service") {
    aws ecs delete-service --cluster $ClusterName --service vueladata-rabbitmq-service --force --region $AwsRegion | Out-Null
    Start-Sleep -Seconds 3
}

$rmqRegistry = "arn:aws:servicediscovery:us-east-1:239700238526:service/srv-dwgjp25h57552rgi"
aws ecs create-service `
    --cluster $ClusterName `
    --service-name vueladata-rabbitmq-service `
    --task-definition vueladata-rabbitmq `
    --desired-count 1 `
    --launch-type FARGATE `
    --network-configuration "awsvpcConfiguration={subnets=[$SubnetId],securityGroups=[$SecurityGroupId],assignPublicIp=DISABLED}" `
    --service-registries "registryArn=$rmqRegistry" `
    --region $AwsRegion | Out-Null

Write-Host "[+] Servicio RabbitMQ desplegado en ECS con DNS: rabbitmq.vueladata.local" -ForegroundColor Green

Write-Host "[8/8] Configurando y encendiendo Servicios ECS de Airflow..." -ForegroundColor Green

# 8.1 Airflow Webserver Service
$wsRegistry = "arn:aws:servicediscovery:us-east-1:239700238526:service/srv-kjd5hl6wobmc3ilp"
$wsCheck = aws ecs describe-services --cluster $ClusterName --services vueladata-airflow-webserver --region $AwsRegion --query "services[?status=='ACTIVE'].serviceName" --output text
if ($wsCheck -ne "vueladata-airflow-webserver") {
    aws ecs create-service `
        --cluster $ClusterName `
        --service-name vueladata-airflow-webserver `
        --task-definition vueladata-airflow-webserver `
        --desired-count 1 `
        --launch-type FARGATE `
        --network-configuration "awsvpcConfiguration={subnets=[$SubnetId],securityGroups=[$SecurityGroupId],assignPublicIp=DISABLED}" `
        --service-registries "registryArn=$wsRegistry" `
        --region $AwsRegion | Out-Null
    Write-Host "[+] Servicio Airflow Webserver creado y encendido." -ForegroundColor Green
} else {
    aws ecs update-service --cluster $ClusterName --service vueladata-airflow-webserver --task-definition vueladata-airflow-webserver --desired-count 1 --region $AwsRegion | Out-Null
    Write-Host "[+] Servicio Airflow Webserver actualizado y encendido." -ForegroundColor Green
}

# 8.2 Airflow Scheduler Service
$schedCheck = aws ecs describe-services --cluster $ClusterName --services vueladata-airflow-scheduler --region $AwsRegion --query "services[?status=='ACTIVE'].serviceName" --output text
if ($schedCheck -ne "vueladata-airflow-scheduler") {
    aws ecs create-service `
        --cluster $ClusterName `
        --service-name vueladata-airflow-scheduler `
        --task-definition vueladata-airflow-scheduler `
        --desired-count 1 `
        --launch-type FARGATE `
        --network-configuration "awsvpcConfiguration={subnets=[$SubnetId],securityGroups=[$SecurityGroupId],assignPublicIp=DISABLED}" `
        --region $AwsRegion | Out-Null
    Write-Host "[+] Servicio Airflow Scheduler creado y encendido." -ForegroundColor Green
} else {
    aws ecs update-service --cluster $ClusterName --service vueladata-airflow-scheduler --task-definition vueladata-airflow-scheduler --desired-count 1 --region $AwsRegion | Out-Null
    Write-Host "[+] Servicio Airflow Scheduler actualizado y encendido." -ForegroundColor Green
}

# 8.3 Airflow Worker Service
$workerCheck = aws ecs describe-services --cluster $ClusterName --services vueladata-airflow-worker --region $AwsRegion --query "services[?status=='ACTIVE'].serviceName" --output text
if ($workerCheck -ne "vueladata-airflow-worker") {
    aws ecs create-service `
        --cluster $ClusterName `
        --service-name vueladata-airflow-worker `
        --task-definition vueladata-airflow-worker `
        --desired-count 1 `
        --launch-type FARGATE `
        --network-configuration "awsvpcConfiguration={subnets=[$SubnetId],securityGroups=[$SecurityGroupId],assignPublicIp=DISABLED}" `
        --region $AwsRegion | Out-Null
    Write-Host "[+] Servicio Airflow Celery Worker creado y encendido." -ForegroundColor Green
} else {
    aws ecs update-service --cluster $ClusterName --service vueladata-airflow-worker --task-definition vueladata-airflow-worker --desired-count 1 --region $AwsRegion | Out-Null
    Write-Host "[+] Servicio Airflow Celery Worker actualizado y encendido." -ForegroundColor Green
}

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "[+] Despliegue de servicios continuos completado con exito." -ForegroundColor Green
Write-Host "=========================================================" -ForegroundColor Cyan

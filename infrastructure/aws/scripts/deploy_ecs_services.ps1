param (
    [string]$AwsAccountId = $env:AWS_ACCOUNT_ID,
    [string]$AwsRegion = "us-east-1",
    [string]$ClusterName = "vueladata-cluster",
    [string]$SubnetId = $env:ECS_SUBNET_ID,
    [string]$SecurityGroupId = $env:ECS_SECURITY_GROUP_ID
)

$ErrorActionPreference = "Stop"
foreach ($required in @($AwsAccountId, $SubnetId, $SecurityGroupId, $env:RABBITMQ_SERVICE_REGISTRY_ARN, $env:AIRFLOW_SERVICE_REGISTRY_ARN)) {
    if (-not $required) { throw "Set account, subnet, security group and both Cloud Map registry ARNs before deploying." }
}

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "Registro de Task Definitions y Despliegue en AWS ECS" -ForegroundColor Cyan
Write-Host "Cluster: $ClusterName | Región: $AwsRegion" -ForegroundColor Yellow
Write-Host "=========================================================" -ForegroundColor Cyan

# 1. Asegurar existencia del Cluster ECS
Write-Host "[1/8] Verificando ECS Cluster '$ClusterName'..." -ForegroundColor Green
$clusterStatus = aws ecs describe-clusters --clusters $ClusterName --region $AwsRegion --query "clusters[0].status" --output text
if ($LASTEXITCODE -ne 0) { throw "Unable to inspect ECS cluster" }
if ($clusterStatus -ne "ACTIVE") {
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

    }

    foreach ($k in $replacements.Keys) {
        $raw = $raw.Replace($k, $replacements[$k])
    }

    if ($raw -match '\$\{[^}]+\}') { throw "Unresolved task definition placeholder" }
    $null = $raw | ConvertFrom-Json
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

Register-TaskFile "infrastructure\aws\ecs\task-airflow-init.json" "task-airflow-init"

Write-Host "[7/8] Configurando y encendiendo Servicio ECS de RabbitMQ..." -ForegroundColor Green
# Verificar si el servicio de RabbitMQ existe
$rabbitSvcCheck = aws ecs describe-services --cluster $ClusterName --services vueladata-rabbitmq-service --region $AwsRegion --query "services[?status=='ACTIVE'].serviceName" --output text

$rmqRegistry = $env:RABBITMQ_SERVICE_REGISTRY_ARN
if ($rabbitSvcCheck -eq "vueladata-rabbitmq-service") {
    aws ecs update-service --cluster $ClusterName --service vueladata-rabbitmq-service --task-definition vueladata-rabbitmq --region $AwsRegion | Out-Null
} else {
aws ecs create-service `
    --cluster $ClusterName `
    --service-name vueladata-rabbitmq-service `
    --task-definition vueladata-rabbitmq `
    --desired-count 1 `
    --launch-type FARGATE `
    --network-configuration "awsvpcConfiguration={subnets=[$SubnetId],securityGroups=[$SecurityGroupId],assignPublicIp=DISABLED}" `
    --service-registries "registryArn=$rmqRegistry" `
    --region $AwsRegion | Out-Null

}

Write-Host "[+] Servicio RabbitMQ desplegado en ECS con DNS: rabbitmq.vueladata.local" -ForegroundColor Green

# Run migrations once and verify exit code before starting Airflow services.
$initRaw = aws ecs run-task --cluster $ClusterName --task-definition vueladata-airflow-init --launch-type FARGATE --network-configuration "awsvpcConfiguration={subnets=[$SubnetId],securityGroups=[$SecurityGroupId],assignPublicIp=DISABLED}" --region $AwsRegion --output json
if ($LASTEXITCODE -ne 0) { throw "Unable to start Airflow migration" }
$initResult = $initRaw | ConvertFrom-Json
if ($initResult.failures.Count -gt 0 -or -not $initResult.tasks) { throw "ECS rejected Airflow migration" }
$initArn = $initResult.tasks[0].taskArn
aws ecs wait tasks-stopped --cluster $ClusterName --tasks $initArn --region $AwsRegion
if ($LASTEXITCODE -ne 0) { throw "Airflow migration did not stop before waiter timeout" }
$initExit = aws ecs describe-tasks --cluster $ClusterName --tasks $initArn --region $AwsRegion --query "tasks[0].containers[0].exitCode" --output text
if ($LASTEXITCODE -ne 0 -or $initExit -ne "0") { throw "Airflow migration failed; inspect CloudWatch" }

Write-Host "[8/8] Configurando y encendiendo Servicios ECS de Airflow..." -ForegroundColor Green

# 8.1 Airflow Webserver Service
$wsRegistry = $env:AIRFLOW_SERVICE_REGISTRY_ARN
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

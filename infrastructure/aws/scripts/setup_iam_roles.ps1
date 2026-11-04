param (
    [string]$AwsRegion = "us-east-1"
)

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "Configuración de Roles y Políticas IAM para Airflow y Spark" -ForegroundColor Cyan
Write-Host "=========================================================" -ForegroundColor Cyan

# 1. Crear Role vueladata-airflow-task-role
Write-Host "[1/4] Verificando rol 'vueladata-airflow-task-role'..." -ForegroundColor Green
$null = aws iam get-role --role-name "vueladata-airflow-task-role" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "  -> Creando rol vueladata-airflow-task-role..."
    aws iam create-role --role-name "vueladata-airflow-task-role" `
        --assume-role-policy-document file://infrastructure/aws/iam/ecs-trust-policy.json
} else {
    Write-Host "  -> Rol vueladata-airflow-task-role ya existe."
}

# 2. Asignar política a vueladata-airflow-task-role
Write-Host "[2/4] Adjuntando política a 'vueladata-airflow-task-role'..." -ForegroundColor Green
aws iam put-role-policy --role-name "vueladata-airflow-task-role" `
    --policy-name "VuelaDataAirflowTaskPolicy" `
    --policy-document file://infrastructure/aws/iam/policy-airflow-task.json

# 3. Crear Role vueladata-spark-task-role
Write-Host "[3/4] Verificando rol 'vueladata-spark-task-role'..." -ForegroundColor Green
$null = aws iam get-role --role-name "vueladata-spark-task-role" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "  -> Creando rol vueladata-spark-task-role..."
    aws iam create-role --role-name "vueladata-spark-task-role" `
        --assume-role-policy-document file://infrastructure/aws/iam/ecs-trust-policy.json
} else {
    Write-Host "  -> Rol vueladata-spark-task-role ya existe."
}

# 4. Asignar política a vueladata-spark-task-role
Write-Host "[4/4] Adjuntando política a 'vueladata-spark-task-role'..." -ForegroundColor Green
aws iam put-role-policy --role-name "vueladata-spark-task-role" `
    --policy-name "VuelaDataSparkTaskPolicy" `
    --policy-document file://infrastructure/aws/iam/policy-spark-task.json

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "[+] Roles y políticas IAM configurados exitosamente." -ForegroundColor Green
Write-Host "=========================================================" -ForegroundColor Cyan

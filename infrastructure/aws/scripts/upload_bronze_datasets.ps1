param (
    [string]$BucketName = "vueladata-datalake-dev-239700238526-us-east-1-an",
    [string]$DataLocalDir = "C:\Users\lmeji\Desktop\vueladata\data",
    [string]$FlightsCsvPath = "C:\Users\lmeji\Downloads\flights.csv"
)

$ErrorActionPreference = "Stop"

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "Iniciando carga de datos a la capa S3 Bronze de VuelaData" -ForegroundColor Cyan
Write-Host "Bucket S3 destino: s3://$BucketName/bronze/" -ForegroundColor Yellow
Write-Host "=========================================================" -ForegroundColor Cyan

# 1. Crear estructura de prefijos en S3 Bronze
Write-Host "[*] Asegurando prefijos en S3 Bronze..." -ForegroundColor Green
$prefixes = @(
    "bronze/f1_kaggle/",
    "bronze/f6_rotacion/",
    "bronze/events/oooi/",
    "bronze/events/pagos/",
    "bronze/events/clima/",
    "bronze/events/opensky/"
)

foreach ($prefix in $prefixes) {
    aws s3api put-object --bucket $BucketName --key $prefix | Out-Null
}

# 2. Cargar Fuente F1 (Kaggle: airlines, airports y flights)
Write-Host "[*] Subiendo dataset F1 Kaggle..." -ForegroundColor Green
$airlinesPath = Join-Path $DataLocalDir "reference\airlines.csv"
$airportsPath = Join-Path $DataLocalDir "reference\airports.csv"

if (Test-Path $airlinesPath) {
    Write-Host "  -> Subiendo airlines.csv..."
    aws s3 cp $airlinesPath "s3://$BucketName/bronze/f1_kaggle/airlines.csv"
}
if (Test-Path $airportsPath) {
    Write-Host "  -> Subiendo airports.csv..."
    aws s3 cp $airportsPath "s3://$BucketName/bronze/f1_kaggle/airports.csv"
}
if (Test-Path $FlightsCsvPath) {
    Write-Host "  -> Subiendo flights.csv (592 MB)..."
    aws s3 cp $FlightsCsvPath "s3://$BucketName/bronze/f1_kaggle/flights.csv"
} else {
    Write-Host "  [!] No se encontro flights.csv en $FlightsCsvPath" -ForegroundColor Yellow
}

# 3. Cargar Fuente F6 (Rotación e Itinerario Semanal)
Write-Host "[*] Subiendo dataset F6 Rotación de flota..." -ForegroundColor Green
$generatedDir = Join-Path $DataLocalDir "generated"
if (Test-Path $generatedDir) {
    aws s3 sync $generatedDir "s3://$BucketName/bronze/f6_rotacion/"
}

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "[+] Carga a S3 Bronze finalizada exitosamente." -ForegroundColor Green
Write-Host "=========================================================" -ForegroundColor Cyan
aws s3 ls "s3://$BucketName/bronze/" --recursive

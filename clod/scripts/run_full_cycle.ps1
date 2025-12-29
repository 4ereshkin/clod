# PowerShell скрипт для запуска полного цикла инжеста
# 
# Использование:
#   .\scripts\run_full_cycle.ps1 -Company "company1" -Dataset "dataset1" -Cloud "data\user_data\НПС Крутое\1\t100pro_2025-04-28-08-36-08_CGCS.laz"
#
# Или с полными параметрами:
#   .\scripts\run_full_cycle.ps1 -Company "company1" -Dataset "dataset1" `
#       -Cloud "data\user_data\НПС Крутое\1\t100pro_2025-04-28-08-36-08_CGCS.laz" `
#       -Path "data\user_data\НПС Крутое\1\path.txt" `
#       -CP "data\user_data\НПС Крутое\1\ControlPoint.txt"

param(
    [Parameter(Mandatory=$true)]
    [string]$Company,
    
    [Parameter(Mandatory=$true)]
    [string]$Dataset,
    
    [Parameter(Mandatory=$true)]
    [string]$Cloud,
    
    [Parameter(Mandatory=$false)]
    [string]$Path,
    
    [Parameter(Mandatory=$false)]
    [string]$CP,
    
    [Parameter(Mandatory=$false)]
    [string]$CRS = "CGCS2000",
    
    [Parameter(Mandatory=$false)]
    [string]$SchemaVersion = "1.1.0",
    
    [Parameter(Mandatory=$false)]
    [switch]$Force
)

$ErrorActionPreference = "Stop"

# Проверка существования файлов
if (-not (Test-Path $Cloud)) {
    Write-Error "Файл не найден: $Cloud"
    exit 1
}

if ($Path -and -not (Test-Path $Path)) {
    Write-Error "Файл не найден: $Path"
    exit 1
}

if ($CP -and -not (Test-Path $CP)) {
    Write-Error "Файл не найден: $CP"
    exit 1
}

# Построение команды
$scriptPath = Join-Path $PSScriptRoot "full_ingest_cycle.py"
$arguments = @(
    "--company", $Company
    "--dataset", $Dataset
    "--cloud", $Cloud
    "--crs", $CRS
    "--schema-version", $SchemaVersion
)

if ($Path) {
    $arguments += "--path", $Path
}

if ($CP) {
    $arguments += "--cp", $CP
}

if ($Force) {
    $arguments += "--force"
}

# Запуск скрипта
Write-Host "=== Запуск полного цикла инжеста ===" -ForegroundColor Green
Write-Host "Company: $Company"
Write-Host "Dataset: $Dataset"
Write-Host "Cloud: $Cloud"
if ($Path) {
    Write-Host "Path: $Path"
}
if ($CP) {
    Write-Host "Control Point: $CP"
}
Write-Host ""

python $scriptPath $arguments

if ($LASTEXITCODE -ne 0) {
    Write-Error "Скрипт завершился с ошибкой (код: $LASTEXITCODE)"
    exit $LASTEXITCODE
}


# Registro aditivo y validación analítica

Preparado mediante inspección de solo lectura. Ninguna consulta fue ejecutada en AWS.
`registrar_modelo_estrella.sql` crea únicamente seis dimensiones y tres hechos
adicionales en `vueladata_gold_db`. No modifica los siete cubos de Power BI.
Los tipos y particiones corresponden a `pipeline/gold/star.py` y `aggregator.py`.

## Antes de ejecutar

1. Obtener aprobación para las escrituras en Glue y resultados de Athena.
2. Publicar y verificar los Parquet de las nueve tablas nuevas. Hoy no existen
   sus prefijos en el bucket inspeccionado. Usar `--only-star-model` para leer
   Silver existente y escribir exclusivamente las nueve tablas nuevas. Este modo
   omite Bronze, Silver y toda conexión/carga/auditoría RDS; rechaza `--sync-rds`.
   Ejemplo de argumentos del contenedor: `--bucket <bucket> --only-star-model`.
   Los nueve prefijos estrella se reemplazan de forma idempotente; los siete cubos
   no se leen ni escriben. No usar la ejecución completa para esta publicación.
3. El código y el contrato local ahora incluyen `conexiones_hubs.nombre_hub string`.
   El nombre procede del catálogo operacional `aeropuerto.nombre`, mediante el
   broadcast join por id_aeropuerto en Silver. Antes de una publicación completa,
   regenerar Silver conexiones antiguo para incorporar ese atributo; el proceso
   rechaza conexiones no vacías sin la columna antes de escribir cualquier cubo.
4. Comparar el esquema físico Parquet con el DDL. `IF NOT EXISTS` no valida ni
   corrige una tabla existente. Las tablas vacías pueden no tener particiones.
5. Sustituir literalmente `${GOLD_BUCKET}` por el nombre real del bucket, sin
   `s3://`. No pegar el archivo completo en una sola ejecución de Athena.

Desde la raíz del repositorio, preparar archivos por sentencia sin acceder a AWS:

```powershell
# Definir GOLD_BUCKET con el bucket verificado en la cuenta de destino.
if (-not $env:GOLD_BUCKET) { throw 'Falta GOLD_BUCKET' }
if ($env:GOLD_BUCKET -notmatch '^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$') { throw 'Bucket inválido' }
$sqlOut = Join-Path $env:TEMP ('vueladata-athena-' + [guid]::NewGuid())
New-Item -ItemType Directory -Path $sqlOut | Out-Null
$sql = Get-Content infrastructure/aws/athena/registrar_modelo_estrella.sql -Raw
$sql = $sql.Replace('${GOLD_BUCKET}', $env:GOLD_BUCKET)
$sql = $sql -replace '(?m)^--[^\r\n]*', ''
$index = 0
foreach ($statement in ($sql -split ';')) {
    if ($statement.Trim()) {
        $index++
        [IO.File]::WriteAllText((Join-Path $sqlOut ('{0:D2}.sql' -f $index)), $statement.Trim(), [Text.UTF8Encoding]::new($false))
    }
}
$sqlOut
```

Solo después de autorización, ejecutar cada archivo en orden, esperando `SUCCEEDED`
antes del siguiente. Hay 13 sentencias: nueve CREATE y cuatro MSCK.

```powershell
$queryFile = Join-Path $sqlOut '01.sql'
aws athena start-query-execution --query-string "file://$queryFile" --query-execution-context Database=vueladata_gold_db --work-group primary --region us-east-1
# Sustituir el ID por QueryExecutionId de la respuesta anterior.
aws athena get-query-execution --query-execution-id '<QueryExecutionId>' --region us-east-1 --query QueryExecution.Status
```

El Workgroup `primary` impone su ruta de resultados. Consultas SELECT también
escriben resultados en S3 y generan costo de escaneo: no forman parte de la
auditoría estrictamente read-only. `athena_queries_control_ct10.sql` contiene las
consultas de negocio y controles, para ejecutar individualmente. Los controles
de hechos requieren primero registrar y poblar las tablas nuevas. Añadir filtros
`anio`/`mes` coherentes entre cubos, hechos y Power BI para reducir el escaneo.

Los controles indican sus límites: no sustituyen pruebas contra Silver para
clima, aviso de cancelación y propagación. Un resultado vacío sobre una tabla
sin datos no demuestra cumplimiento. No borrar los prefijos históricos `kpi_*`.

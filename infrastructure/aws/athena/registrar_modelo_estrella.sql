-- PROPUESTA: NO ejecutada. Sustituir ${GOLD_BUCKET}; ejecutar UNA sentencia por consulta Athena.

-- Ejecutar solo después de publicar Parquet y comprobar tipos. No altera tablas existentes.

CREATE EXTERNAL TABLE IF NOT EXISTS vueladata_gold_db.dim_fecha (
  id_fecha int,
  fecha_vuelo date,
  dia int
)
PARTITIONED BY (anio int, mes int)
STORED AS PARQUET
LOCATION 's3://${GOLD_BUCKET}/gold/dim_fecha/'
TBLPROPERTIES ('parquet.column.index.access'='false');

MSCK REPAIR TABLE vueladata_gold_db.dim_fecha;

CREATE EXTERNAL TABLE IF NOT EXISTS vueladata_gold_db.dim_aeropuerto (
  codigo_iata string
)
STORED AS PARQUET
LOCATION 's3://${GOLD_BUCKET}/gold/dim_aeropuerto/'
TBLPROPERTIES ('parquet.column.index.access'='false');

CREATE EXTERNAL TABLE IF NOT EXISTS vueladata_gold_db.dim_aerolinea (
  id_aerolinea_gold string,
  codigo_aerolinea string
)
STORED AS PARQUET
LOCATION 's3://${GOLD_BUCKET}/gold/dim_aerolinea/'
TBLPROPERTIES ('parquet.column.index.access'='false');

CREATE EXTERNAL TABLE IF NOT EXISTS vueladata_gold_db.dim_aeronave (
  id_aeronave_gold string,
  matricula_aeronave string
)
STORED AS PARQUET
LOCATION 's3://${GOLD_BUCKET}/gold/dim_aeronave/'
TBLPROPERTIES ('parquet.column.index.access'='false');

CREATE EXTERNAL TABLE IF NOT EXISTS vueladata_gold_db.dim_causa (
  id_causa_gold string,
  codigo_causa string
)
STORED AS PARQUET
LOCATION 's3://${GOLD_BUCKET}/gold/dim_causa/'
TBLPROPERTIES ('parquet.column.index.access'='false');

CREATE EXTERNAL TABLE IF NOT EXISTS vueladata_gold_db.dim_ruta (
  id_ruta_gold string,
  aeropuerto_origen string,
  aeropuerto_destino string
)
STORED AS PARQUET
LOCATION 's3://${GOLD_BUCKET}/gold/dim_ruta/'
TBLPROPERTIES ('parquet.column.index.access'='false');

CREATE EXTERNAL TABLE IF NOT EXISTS vueladata_gold_db.fact_vuelos (
  id_vuelo_gold string,
  id_fecha int,
  id_aerolinea_gold string,
  id_aeronave_gold string,
  id_causa_gold string,
  id_ruta_gold string,
  numero_vuelo string,
  aeropuerto_origen string,
  aeropuerto_destino string,
  retraso_llegada_min double,
  retraso_salida_min double,
  es_puntual_otp int,
  es_cancelado int,
  es_desviado int,
  retraso_propagado_estimado double,
  es_imputable boolean,
  tipo_imputabilidad string,
  salida_programada_utc timestamp,
  llegada_programada_utc timestamp,
  salida_programada_local timestamp,
  llegada_programada_local timestamp,
  requiere_auditoria boolean
)
PARTITIONED BY (anio int, mes int)
STORED AS PARQUET
LOCATION 's3://${GOLD_BUCKET}/gold/fact_vuelos/'
TBLPROPERTIES ('parquet.column.index.access'='false');

MSCK REPAIR TABLE vueladata_gold_db.fact_vuelos;

CREATE EXTERNAL TABLE IF NOT EXISTS vueladata_gold_db.fact_compensaciones (
  id_vuelo_gold string,
  id_fecha int,
  id_causa_gold string,
  id_ruta_gold string,
  porcentaje_compensacion double,
  monto_compensacion double,
  derecho_notificacion boolean,
  derecho_reprogramacion boolean,
  moneda string,
  pasajeros_afectados bigint
)
PARTITIONED BY (anio int, mes int)
STORED AS PARQUET
LOCATION 's3://${GOLD_BUCKET}/gold/fact_compensaciones/'
TBLPROPERTIES ('parquet.column.index.access'='false');

MSCK REPAIR TABLE vueladata_gold_db.fact_compensaciones;

CREATE EXTERNAL TABLE IF NOT EXISTS vueladata_gold_db.fact_conexiones (
  id_conexion bigint,
  id_fecha int,
  aeropuerto_hub string,
  minutos_escala_disponible double,
  es_conexion_en_riesgo boolean,
  es_conexion_perdida int
)
PARTITIONED BY (anio int, mes int)
STORED AS PARQUET
LOCATION 's3://${GOLD_BUCKET}/gold/fact_conexiones/'
TBLPROPERTIES ('parquet.column.index.access'='false');

MSCK REPAIR TABLE vueladata_gold_db.fact_conexiones;
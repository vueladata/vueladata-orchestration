-- ==============================================================================
-- VuelaData Lakehouse: Inicialización Integral de Esquemas en PostgreSQL RDS
-- Base de datos: vueladata
-- Usuario maestro: postgres
-- 1. Rol airflow para ejecución de tareas y aislamiento de permisos
-- 2. airflow_db: Metadatos internos de Apache Airflow 3
-- 3. vueladata_audit: Observabilidad, logs de corridas y cuarentena de errores
-- 4. vueladata_gold: Base de datos analítica relacional (Serving Layer)
-- ==============================================================================

-- 0. Asegurar existencia del rol 'airflow'
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow') THEN
    CREATE ROLE airflow WITH LOGIN PASSWORD 'airflow_local_2026';
  END IF;
END
$$;

GRANT airflow TO postgres;

-- ------------------------------------------------------------------------------
-- 1. ESQUEMA DE METADATOS AIRFLOW
-- ------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS airflow_db AUTHORIZATION airflow;
GRANT ALL ON SCHEMA airflow_db TO airflow;
GRANT ALL ON SCHEMA airflow_db TO postgres;
ALTER DEFAULT PRIVILEGES IN SCHEMA airflow_db GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA airflow_db GRANT ALL ON SEQUENCES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA airflow_db GRANT ALL ON FUNCTIONS TO airflow;

-- ------------------------------------------------------------------------------
-- 2. ESQUEMA DE AUDITORÍA, OBSERVABILIDAD Y ERRORES (vueladata_audit)
-- ------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS vueladata_audit AUTHORIZATION airflow;
GRANT ALL ON SCHEMA vueladata_audit TO airflow;
GRANT ALL ON SCHEMA vueladata_audit TO postgres;
ALTER DEFAULT PRIVILEGES IN SCHEMA vueladata_audit GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA vueladata_audit GRANT ALL ON SEQUENCES TO airflow;

-- Tabla 2.1: Log de Ejecución de Pipelines ETL
CREATE TABLE IF NOT EXISTS vueladata_audit.pipeline_execution_log (
    id_ejecucion bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dag_id varchar(100) NOT NULL,
    run_id varchar(150) NOT NULL,
    capa varchar(20) NOT NULL, -- BRONZE, SILVER, GOLD, SERVING
    fecha_proceso date NOT NULL,
    fecha_inicio_utc timestamptz NOT NULL,
    fecha_fin_utc timestamptz,
    filas_leidas bigint DEFAULT 0,
    filas_escritas bigint DEFAULT 0,
    filas_rechazadas bigint DEFAULT 0,
    estado varchar(30) NOT NULL, -- RUNNING, SUCCESS, FAILED, PARTIAL_WARNING
    mensaje_detalle text
);

-- Tabla 2.2: Cuarentena de Registros Erróneos / Calidad de Datos (Dead Letter)
CREATE TABLE IF NOT EXISTS vueladata_audit.pipeline_error_quarantine (
    id_error bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_ejecucion bigint REFERENCES vueladata_audit.pipeline_execution_log(id_ejecucion) ON DELETE SET NULL,
    dag_id varchar(100),
    capa varchar(20) NOT NULL,
    tabla_origen varchar(100) NOT NULL,
    clave_registro varchar(200),
    tipo_error varchar(100) NOT NULL,
    mensaje_error text NOT NULL,
    payload_original jsonb,
    fecha_captura_utc timestamptz NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------------------------
-- 3. ESQUEMA DE BASE DE DATOS ANALÍTICA GOLD (vueladata_gold)
-- ------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS vueladata_gold AUTHORIZATION airflow;
GRANT ALL ON SCHEMA vueladata_gold TO airflow;
GRANT ALL ON SCHEMA vueladata_gold TO postgres;
ALTER DEFAULT PRIVILEGES IN SCHEMA vueladata_gold GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA vueladata_gold GRANT ALL ON SEQUENCES TO airflow;

-- Tabla 3.1: Fact Vuelo Analítico (Granularidad individual enriquecida)
CREATE TABLE IF NOT EXISTS vueladata_gold.fact_vuelo_analitico (
    id_vuelo varchar(64) PRIMARY KEY,
    numero_vuelo varchar(20) NOT NULL,
    codigo_aerolinea varchar(10) NOT NULL,
    matricula_aeronave varchar(20),
    aeropuerto_origen varchar(5) NOT NULL,
    aeropuerto_destino varchar(5) NOT NULL,
    fecha_salida date NOT NULL,
    retraso_salida_min numeric(8,2),
    retraso_llegada_min numeric(8,2),
    es_cancelado smallint DEFAULT 0,
    es_desviado smallint DEFAULT 0,
    es_operado smallint DEFAULT 1,
    es_puntual_otp smallint DEFAULT 0,
    categoria_retraso varchar(30),
    retraso_propagado_estimado numeric(8,2) DEFAULT 0,
    es_retraso_propagado smallint DEFAULT 0,
    fecha_carga_utc timestamptz DEFAULT now()
);

-- Tabla 3.2: Cubo Agregado OTP (RN-01) por Aerolínea, Mes y Origen
CREATE TABLE IF NOT EXISTS vueladata_gold.kpi_otp_aerolinea_mensual (
    anio integer NOT NULL,
    mes integer NOT NULL,
    codigo_aerolinea varchar(10) NOT NULL,
    aeropuerto_origen varchar(5) NOT NULL,
    total_vuelos_programados bigint NOT NULL,
    total_vuelos_cancelados bigint NOT NULL,
    total_vuelos_desviados bigint NOT NULL,
    total_vuelos_operados bigint NOT NULL,
    total_vuelos_puntuales bigint NOT NULL,
    promedio_retraso_llegada_min numeric(8,2),
    promedio_retraso_salida_min numeric(8,2),
    tasa_otp_porcentaje numeric(5,2),
    fecha_actualizacion_utc timestamptz DEFAULT now(),
    PRIMARY KEY (anio, mes, codigo_aerolinea, aeropuerto_origen)
);

-- Tabla 3.3: Cubo Agregado Propagación de Retrasos en Flota (RN-08)
CREATE TABLE IF NOT EXISTS vueladata_gold.kpi_propagacion_flota (
    anio integer NOT NULL,
    mes integer NOT NULL,
    matricula_aeronave varchar(20) NOT NULL,
    total_vuelos_operados bigint NOT NULL,
    total_vuelos_con_retraso_propagado bigint NOT NULL,
    retraso_propagado_promedio_min numeric(8,2),
    tasa_vulnerabilidad_rotacion_pct numeric(5,2),
    fecha_actualizacion_utc timestamptz DEFAULT now(),
    PRIMARY KEY (anio, mes, matricula_aeronave)
);

COMMENT ON SCHEMA airflow_db IS 'Metadatos de Apache Airflow 3';
COMMENT ON SCHEMA vueladata_audit IS 'Auditoría de pipelines ETL y cuarentena de errores de calidad';
COMMENT ON SCHEMA vueladata_gold IS 'Base de datos analítica Gold para dashboards y reportes de negocio';

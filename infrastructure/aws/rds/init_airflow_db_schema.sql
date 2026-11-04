-- ==============================================================================
-- VuelaData: Inicialización del esquema de metadatos de Airflow en PostgreSQL RDS
-- Cumple con el Criterio de Aceptación de HU-11:
-- Conexión exclusiva al esquema interno 'airflow_db' sin tocar las tablas de negocio.
-- ==============================================================================

-- 1. Crear el esquema aislado para Airflow si no existe
CREATE SCHEMA IF NOT EXISTS airflow_db;

-- 2. Conceder todos los privilegios sobre el esquema al usuario de conexión
GRANT ALL ON SCHEMA airflow_db TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA airflow_db GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA airflow_db GRANT ALL ON SEQUENCES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA airflow_db GRANT ALL ON FUNCTIONS TO airflow;

COMMENT ON SCHEMA airflow_db IS 'Metadatos de Apache Airflow 3.x - VuelaData Data Lake Orchestration';

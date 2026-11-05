-- Inicialización de base de datos para Airflow en el contenedor Postgres compartido
CREATE DATABASE airflow_db;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO postgres;


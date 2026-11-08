# ==============================================================================
# Dockerfile para vueladata/airflow (Airflow 3.1.5)
# Arquitectura limpia: Componente de orquestaciÃ³n distribuida (ECS Fargate)
# ==============================================================================

FROM apache/airflow:3.1.5-python3.12

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Instalar proveedores de AWS, Celery y PostgreSQL
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.1.5" -r /opt/airflow/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.5/constraints-3.12.txt"

# Copiar DAGs del proyecto
COPY --chown=airflow:root dags/ /opt/airflow/dags/

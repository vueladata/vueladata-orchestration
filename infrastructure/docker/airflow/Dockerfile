# ==============================================================================
# Dockerfile para vueladata/airflow (Airflow 3.1.5)
# Arquitectura limpia: Componente de orquestación distribuida (ECS Fargate)
# ==============================================================================

FROM apache/airflow:3.1.5

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Instalar proveedores de AWS, Celery y PostgreSQL
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copiar DAGs del proyecto
COPY --chown=airflow:root dags/ /opt/airflow/dags/

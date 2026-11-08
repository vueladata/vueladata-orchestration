
from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="hu11_prueba_celery",
    description="Validación de ejecución distribuida para HU-11",
    schedule=None,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["hu-11", "infraestructura"],
)
def hu11_prueba_celery():

    @task
    def verificar_worker():
        print("HU-11: tarea ejecutada correctamente por Airflow con CeleryExecutor.")
        return "OK"

    verificar_worker()


hu11_prueba_celery()

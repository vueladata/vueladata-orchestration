
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="hu11_prueba_celery",
    description="Validación de ejecución distribuida para HU-11",
    schedule=None,
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

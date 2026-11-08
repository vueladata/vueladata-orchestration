"""Use the checkout DAG bundle, as the production dag-processor does."""
import os
import sys
from pathlib import Path

DAGS = str(Path(__file__).resolve().parents[1] / 'dags')
os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = DAGS
os.environ['AIRFLOW__CORE__LOAD_EXAMPLES'] = 'false'
sys.path.insert(0, DAGS)

import json
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import airflow
import pytest
from airflow.models import DagBag
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope='module')
def bag():
    assert airflow.__version__ == '3.1.5', 'Run in the pinned Linux image'
    result = DagBag(dag_folder=str(ROOT / 'dags'), include_examples=False)
    assert not result.import_errors, result.import_errors
    return result


def test_load_all_dags(bag):
    assert set(bag.dags) == {'dag_daily_batch_spark', 'dag_f1_historico_spark',
                             'dag_ingesta_externa_lambdas', 'hu11_prueba_celery'}


@pytest.mark.parametrize('name', ['dag_daily_batch_spark', 'dag_f1_historico_spark',
                                  'dag_ingesta_externa_lambdas', 'hu11_prueba_celery'])
def test_dag_contract(bag, name):
    dag = bag.dags[name]
    assert not dag.catchup
    assert dag.default_args['retries'] > 0
    assert dag.default_args['retry_delay'] >= timedelta(minutes=1)
    from airflow.serialization.serialized_objects import SerializedDAG
    SerializedDAG.to_dict(dag)


@pytest.mark.parametrize('name', ['dag_daily_batch_spark', 'dag_f1_historico_spark'])
def test_spark_templates(bag, name):
    dag = bag.dags[name]
    task = next(t for t in dag.tasks if isinstance(t, EcsRunTaskOperator))
    assert task.capacity_provider_strategy == [{'capacityProvider': 'FARGATE_SPOT', 'weight': 1}]
    assert task.wait_for_completion and task.execution_timeout <= timedelta(minutes=35)
    assert task.upstream_task_ids == {'validar_entorno'}
    config = {'cluster': 'test-cluster', 'task_definition': 'test-task', 'container': 'spark-etl',
              'bucket': 'test-bucket', 'network': {'awsvpcConfiguration': {
                  'subnets': ['subnet-test'], 'securityGroups': ['sg-test'], 'assignPublicIp': 'DISABLED'}}}
    class TI:
        def xcom_pull(self, **kwargs):
            return config
    task.render_template_fields({'ti': TI(), 'ds': '2026-11-07', 'run_id': 'test-run'})
    assert task.cluster == config['cluster']
    assert task.network_configuration == config['network']
    assert 'test-bucket' in task.overrides['containerOverrides'][0]['command']


@pytest.mark.parametrize('payload,error', [('{"statusCode":200}', None),
    ('{"statusCode":500}', AirflowException), ('{"statusCode":429}', AirflowFailException),
    ('{}', AirflowException), ('not-json', AirflowException), ('null', AirflowException)])
def test_lambda_response(bag, payload, error):
    task = bag.dags['dag_ingesta_externa_lambdas'].get_task('invocar_lambda_opensky_ingestor')
    with patch.object(LambdaInvokeFunctionOperator, 'execute', return_value=payload):
        if error:
            with pytest.raises(error):
                task.execute({})
        else:
            assert task.execute({}) == {'statusCode': 200}


def test_lambda_timeouts(bag):
    for task in bag.dags['dag_ingesta_externa_lambdas'].tasks:
        assert task.invocation_type == 'RequestResponse'
        assert task.botocore_config['read_timeout'] > 900
        assert task.botocore_config['retries']['total_max_attempts'] == 1
        assert task.retry_delay >= timedelta(minutes=15)


@pytest.mark.parametrize('name', ['dag_daily_batch_spark', 'dag_f1_historico_spark'])
@pytest.mark.parametrize('failure', ['missing', 'aws_error'])
def test_s3_failure_blocks_spark(bag, name, failure):
    task = bag.dags[name].get_task('validar_entorno')
    hook_class = task.python_callable.__globals__['S3Hook']
    with patch.dict(task.python_callable.__globals__, {'spark_configuration': lambda: {'bucket': 'test'}}):
        with patch.object(hook_class, 'check_for_key', return_value=False) as key, \
             patch.object(hook_class, 'check_for_prefix', return_value=False) as prefix:
            if failure == 'aws_error':
                key.side_effect = prefix.side_effect = RuntimeError('AWS unavailable')
            with pytest.raises((ValueError, RuntimeError)):
                task.python_callable()


def test_runtime_configuration(bag, monkeypatch):
    from vueladata_orchestration.config import spark_configuration
    monkeypatch.setenv('ECS_SUBNET_IDS', '["subnet-test"]')
    monkeypatch.setenv('ECS_SECURITY_GROUP_IDS', '["sg-test"]')
    monkeypatch.setenv('S3_DATA_LAKE_BUCKET', 'test-bucket')
    monkeypatch.setenv('ECS_CLUSTER_NAME', 'test-cluster')
    monkeypatch.setenv('ECS_SPARK_TASK_DEFINITION', 'test-task')
    monkeypatch.setenv('ECS_SPARK_CONTAINER_NAME', 'spark-etl')
    assert spark_configuration()['network']['awsvpcConfiguration']['subnets'] == ['subnet-test']
    monkeypatch.setenv('ECS_SUBNET_IDS', '[]')
    with pytest.raises(ValueError):
        spark_configuration()

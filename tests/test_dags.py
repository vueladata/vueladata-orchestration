import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

import airflow
import pytest
from airflow.models import DagBag
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope='module')
def bag():
    assert airflow.__version__.split('.')[0] == '3', 'Requires Airflow 3.x'
    try:
        result = DagBag(dag_folder=str(ROOT / 'dags'), include_examples=False)
    except TypeError:
        result = DagBag(dag_folder=str(ROOT / 'dags'))
    assert not result.import_errors, result.import_errors
    return result


def test_load_all_dags(bag):
    assert set(bag.dags) == {'dag_daily_batch_spark', 'dag_f1_historico_spark', 'hu11_prueba_celery'}


@pytest.mark.parametrize('name', ['dag_daily_batch_spark', 'dag_f1_historico_spark', 'hu11_prueba_celery'])
def test_dag_contract(bag, name):
    dag = bag.dags[name]
    assert not dag.catchup
    assert dag.default_args['retries'] > 0
    assert dag.default_args['retry_delay'] >= timedelta(minutes=1)
    from airflow.serialization.serialized_objects import SerializedDAG
    if hasattr(SerializedDAG, 'to_dict'):
        SerializedDAG.to_dict(dag)
    elif hasattr(SerializedDAG, 'serialize_dag'):
        SerializedDAG.serialize_dag(dag)


@pytest.mark.parametrize('name', ['dag_daily_batch_spark', 'dag_f1_historico_spark'])
def test_spark_templates(bag, name):
    dag = bag.dags[name]
    layers = ['bronze', 'silver', 'gold'] if name == 'dag_daily_batch_spark' else ['silver', 'gold']
    assert set(dag.task_ids) == {'validar_entorno', *(f'procesar_{layer}' for layer in layers)}
    config = {'cluster': 'test-cluster', 'task_definition': 'test-task', 'container': 'spark-etl',
              'bucket': 'test-bucket', 'network': {'awsvpcConfiguration': {
                  'subnets': ['subnet-test'], 'securityGroups': ['sg-test'], 'assignPublicIp': 'DISABLED'}}}
    class TI:
        def xcom_pull(self, **kwargs):
            return config
    previous = 'validar_entorno'
    for layer in layers:
        task = dag.get_task(f'procesar_{layer}')
        assert isinstance(task, EcsRunTaskOperator)
        assert task.capacity_provider_strategy == [{'capacityProvider': 'FARGATE_SPOT', 'weight': 1}]
        assert task.wait_for_completion and task.execution_timeout <= timedelta(hours=1)
        assert task.upstream_task_ids == {previous}
        assert task.trigger_rule == 'all_success'
        assert task.pool == 'gold_writer' and not task.deferrable
        run = SimpleNamespace(conf={}, logical_date=datetime(2026, 11, 7, tzinfo=timezone.utc), start_date=None)
        task.render_template_fields({'ti': TI(), 'dag_run': run, 'run_id': 'test-run'})
        assert task.cluster == config['cluster']
        assert task.network_configuration == config['network']
        command = task.overrides['containerOverrides'][0]['command']
        assert command[command.index('--layer') + 1] == layer
        assert command[command.index('--date') + 1] == '2026-11-07'
        assert 'test-bucket' in command
        previous = task.task_id


@pytest.mark.parametrize('conf,expected', [({}, '2026-11-09'), ({'date': '2026-11-08'}, '2026-11-08')])
def test_manual_run_without_logical_date(bag, conf, expected):
    from vueladata_orchestration.spark_tasks import spark_stage
    task = spark_stage('bronze', 'daily', 'manual_test')
    config = {'cluster': 'cluster', 'task_definition': 'spark', 'container': 'spark-etl', 'bucket': 'lake', 'network': {}}
    ti = SimpleNamespace(xcom_pull=lambda **kwargs: config)
    run = SimpleNamespace(conf=conf, logical_date=None, start_date=datetime(2026, 11, 9, tzinfo=timezone.utc))
    task.render_template_fields({'ti': ti, 'dag_run': run, 'run_id': 'manual-test'})
    command = task.overrides['containerOverrides'][0]['command']
    assert command[command.index('--date') + 1] == expected


@pytest.mark.parametrize('name', ['dag_daily_batch_spark', 'dag_f1_historico_spark'])
@pytest.mark.parametrize('failure', ['missing', 'aws_error'])
def test_s3_failure_blocks_spark(bag, name, failure):
    task = bag.dags[name].get_task('validar_entorno')
    with patch.dict(task.python_callable.__globals__, {
        'spark_configuration': lambda: {'bucket': 'test-bucket', 'region': 'us-east-1'}
    }):
        with patch('boto3.client') as mock_boto:
            mock_s3 = mock_boto.return_value
            if failure == 'missing':
                mock_s3.list_objects_v2.return_value = {'KeyCount': 0}
            else:
                mock_s3.list_objects_v2.side_effect = RuntimeError('AWS unavailable')
            with pytest.raises((ValueError, RuntimeError)):
                task.python_callable()


@pytest.mark.parametrize('name', ['dag_daily_batch_spark', 'dag_f1_historico_spark'])
def test_s3_success_validates_prefixes(bag, name):
    task = bag.dags[name].get_task('validar_entorno')
    with patch.dict(task.python_callable.__globals__, {
        'spark_configuration': lambda: {'bucket': 'test-bucket', 'region': 'us-east-1'}
    }):
        with patch('boto3.client') as mock_boto:
            mock_s3 = mock_boto.return_value
            mock_s3.list_objects_v2.return_value = {'KeyCount': 1}
            cfg = task.python_callable()
            assert cfg['bucket'] == 'test-bucket'
            calls = [c.kwargs['Prefix'] for c in mock_s3.list_objects_v2.call_args_list]
            if name == 'dag_daily_batch_spark':
                assert set(calls) == {'bronze/f6_rotacion/', 'bronze/events/', 'bronze/rds_operacional/'}
            else:
                assert set(calls) == {'bronze/f1_kaggle/', 'bronze/f6_rotacion/'}


def test_runtime_configuration(bag, monkeypatch):
    from vueladata_orchestration.config import spark_configuration
    monkeypatch.setenv('ECS_SUBNET_IDS', '["subnet-test"]')
    monkeypatch.setenv('ECS_SECURITY_GROUP_IDS', '["sg-test"]')
    monkeypatch.setenv('S3_DATA_LAKE_BUCKET', 'test-bucket')
    monkeypatch.setenv('ECS_CLUSTER_NAME', 'vueladata-cluster')
    monkeypatch.setenv('ECS_SPARK_TASK_DEFINITION', 'vueladata-spark-etl:5')
    monkeypatch.setenv('ECS_SPARK_CONTAINER_NAME', 'spark-etl')
    cfg = spark_configuration()
    assert cfg['network']['awsvpcConfiguration']['subnets'] == ['subnet-test']
    assert cfg['cluster'] == 'vueladata-cluster'
    assert cfg['task_definition'] == 'vueladata-spark-etl:5'
    assert cfg['network']['awsvpcConfiguration']['assignPublicIp'] == 'DISABLED'
    monkeypatch.setenv('ECS_SUBNET_IDS', '[]')
    with pytest.raises(ValueError):
        spark_configuration()

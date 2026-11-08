import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_task_definitions_use_ssm():
    files = list((ROOT / 'infrastructure/aws/ecs').glob('task-*.json'))
    assert len(files) == 6
    for path in files:
        raw = path.read_text()
        assert not re.search(r'\b\d{12}\b', raw), path
        definition = json.loads(raw)
        assert definition['networkMode'] == 'awsvpc'
        for container in definition['containerDefinitions']:
            assert container['secrets']
            for secret in container['secrets']:
                assert secret['valueFrom'].startswith('/vueladata/dev/')
            for entry in container.get('environment', []):
                assert not re.search(r'PASSWORD|SECRET|FERNET|BROKER_URL|RESULT_BACKEND|SQL_ALCHEMY_CONN|DATABASE_URL|_PASS$', entry['name'])


def test_compose_health_and_dependencies():
    services = yaml.safe_load((ROOT / 'docker-compose.yml').read_text())['services']
    assert len(services) == 13
    for name, service in services.items():
        if name == 'airflow-init':
            assert service['restart'] == 'no'  # One-shot, completion is its readiness signal.
        else:
            assert service['healthcheck']['test'], name
        for dependency, spec in service.get('depends_on', {}).items():
            assert dependency in services
            if spec['condition'] == 'service_healthy':
                assert 'healthcheck' in services[dependency]
        if name.startswith('airflow-') and name != 'airflow-init':
            assert service['depends_on']['airflow-init']['condition'] == 'service_completed_successfully'
            assert service['depends_on']['postgres']['condition'] == 'service_healthy'


def test_shared_airflow_auth_secrets():
    for path in (ROOT / 'infrastructure/aws/ecs').glob('task-airflow-*.json'):
        for container in json.loads(path.read_text())['containerDefinitions']:
            names = {entry['name'] for entry in container['secrets']}
            assert {'AIRFLOW__API_AUTH__JWT_SECRET', 'AIRFLOW__API__SECRET_KEY',
                    'AIRFLOW__CORE__FERNET_KEY'} <= names

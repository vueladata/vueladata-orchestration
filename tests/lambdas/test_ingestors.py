import json
from unittest.mock import Mock
import pytest
import pika
import event_transport as transport
import opensky
import openmeteo


def test_opensky_contract_units_and_stable_id():
    data = {'states': [['ABC123', ' TEST ', 'US', 1700000000, 1700000001,
                       -73., 40., 1000., False, 100., 90., 0., None, 1050.]]}
    row = opensky.normalize(data)[0]
    assert row['event_type'] == 'opensky_adsb'
    assert row['velocidad_kmh'] == 360 and row['altitud_m'] == 1050
    assert row['codigo_icao24'] == 'abc123' and row['fecha_hora_posicion_utc'].endswith('+00:00')
    assert 'id_aeronave' not in row
    assert opensky.normalize(data)[0]['event_id'] == row['event_id']


def test_weather_contract_visibility_and_utc():
    row = openmeteo.normalize({'current': {'time': '2026-11-08T12:15', 'weather_code': 2,
                               'wind_speed_10m': 51., 'temperature_2m': 12.},
                              'hourly': {'time': ['2026-11-08T12:00'], 'visibility': [800.]}})
    assert row['id_aeropuerto'] == 'ATL' and row['event_type'] == 'clima'
    assert row['visibilidad_km'] == .8 and row['velocidad_viento_kmh'] == 51


def test_fanout_binds_both_durable_queues_and_confirms(monkeypatch):
    monkeypatch.setenv('RABBITMQ_QUEUE_DL', 'q.lake_writer')
    monkeypatch.setenv('RABBITMQ_QUEUE_RDS', 'q.rds_writer')
    connection = Mock()
    ch = transport.prepare_channel(connection)
    assert {c.kwargs['queue'] for c in ch.queue_bind.call_args_list} == {'q.lake_writer', 'q.rds_writer'}
    ch.confirm_delivery.assert_called_once()
    transport.publish(ch, [{'event_id': 'one', 'event_type': 'clima'}])
    ch.basic_publish.assert_called_once()
    assert ch.basic_publish.call_args.kwargs['mandatory'] is True
    assert ch.basic_publish.call_args.kwargs['properties'].delivery_mode == 2


@pytest.mark.parametrize('stopped', [True, False])
def test_connection_failure_falls_back_only_when_ecs_stopped(monkeypatch, stopped):
    monkeypatch.setattr(transport, 'connect', Mock(side_effect=pika.exceptions.AMQPConnectionError()))
    monkeypatch.setattr(transport, 'broker_stopped', lambda: stopped)
    store = Mock(return_value='pending.json')
    monkeypatch.setattr(transport, 'store_pending', store)
    if stopped:
        assert transport.deliver([{'event_id': 'one'}], 'clima')['pending_rds']
        store.assert_called_once()
    else:
        with pytest.raises(pika.exceptions.AMQPConnectionError):
            transport.deliver([{'event_id': 'one'}], 'clima')
        store.assert_not_called()


def test_auth_error_and_publish_failure_do_not_fall_back(monkeypatch):
    store = Mock()
    monkeypatch.setattr(transport, 'store_pending', store)
    monkeypatch.setattr(transport, 'connect', Mock(side_effect=pika.exceptions.ProbableAuthenticationError()))
    with pytest.raises(pika.exceptions.ProbableAuthenticationError):
        transport.deliver([], 'clima')
    connection = Mock()
    monkeypatch.setattr(transport, 'connect', lambda: connection)
    monkeypatch.setattr(transport, 'prepare_channel', lambda c: Mock())
    monkeypatch.setattr(transport, 'publish', Mock(side_effect=RuntimeError('publish failed')))
    with pytest.raises(RuntimeError):
        transport.deliver([{'event_id': 'one'}], 'clima')
    store.assert_not_called()
    connection.close.assert_called_once()


def test_replay_marks_only_after_confirmation_and_retains_file(monkeypatch):
    s3 = Mock()
    s3.get_paginator.return_value.paginate.return_value = [{'Contents': [{'Key': 'pending'}]}]
    s3.get_object_tagging.return_value = {'TagSet': [{'Key': 'rabbitmq', 'Value': 'pending'}]}
    s3.get_object.return_value = {'Body': Mock(read=lambda: b'{"event_id":"one"}\n')}
    monkeypatch.setattr(transport, 'client', lambda service: s3)
    monkeypatch.setenv('S3_BUCKET', 'test-bucket')
    publish = Mock(side_effect=RuntimeError('publish failed'))
    monkeypatch.setattr(transport, 'publish', publish)
    with pytest.raises(RuntimeError):
        transport.replay_pending(Mock(), 'clima', None)
    s3.put_object_tagging.assert_not_called()
    publish.side_effect = None
    assert transport.replay_pending(Mock(), 'clima', None) == 1
    s3.put_object_tagging.assert_called_once()
    s3.delete_object.assert_not_called()


def test_missing_password_has_no_default(monkeypatch):
    monkeypatch.delenv('RABBITMQ_PASSWORD', raising=False)
    with pytest.raises(RuntimeError, match='RABBITMQ_PASSWORD'):
        transport.required('RABBITMQ_PASSWORD')


def test_successful_delivery_does_not_write_direct_s3(monkeypatch):
    monkeypatch.setattr(transport, 'connect', Mock(return_value=Mock()))
    monkeypatch.setattr(transport, 'prepare_channel', Mock())
    monkeypatch.setattr(transport, 'publish', Mock())
    monkeypatch.setattr(transport, 'replay_pending', Mock(return_value=0))
    store = Mock()
    monkeypatch.setattr(transport, 'store_pending', store)
    assert transport.deliver([{'event_id': 'one'}], 'clima')['rabbit_published']
    store.assert_not_called()

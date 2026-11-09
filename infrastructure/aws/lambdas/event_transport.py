"""Confirmed fanout to both writers; S3 backlog only while RabbitMQ ECS is stopped."""
import hashlib
import json
import os
import socket

import boto3
from botocore.config import Config
import pika

EXCHANGE = 'vueladata.external'


def client(service):
    return boto3.client(service, config=Config(connect_timeout=3, read_timeout=5,
                       retries={'max_attempts': 1, 'mode': 'standard'}))


def required(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f'Missing required environment variable: {name}')
    return value


def broker_stopped():
    result = client('ecs').describe_services(
        cluster=required('ECS_CLUSTER'), services=[required('RABBITMQ_SERVICE')])
    services = result.get('services', [])
    return (not result.get('failures') and len(services) == 1
            and all(services[0].get(k) == 0 for k in ('desiredCount', 'runningCount', 'pendingCount')))


def connect():
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=required('RABBITMQ_HOST'), port=int(os.getenv('RABBITMQ_PORT', '5672')),
        credentials=pika.PlainCredentials(required('RABBITMQ_USER'), required('RABBITMQ_PASSWORD')),
        connection_attempts=1, socket_timeout=3, stack_timeout=5,
        blocked_connection_timeout=5, heartbeat=30))


def prepare_channel(connection):
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type='fanout', durable=True)
    queues = [required('RABBITMQ_QUEUE_DL'), required('RABBITMQ_QUEUE_RDS')]
    if len(set(queues)) != 2:
        raise ValueError('Lake and RDS queues must be distinct')
    for queue in queues:
        channel.queue_declare(queue=queue, durable=True)
        channel.queue_bind(exchange=EXCHANGE, queue=queue)
    channel.confirm_delivery()
    return channel


def publish(channel, records):
    # One confirmed fanout publication routes the identical JSON to BOTH queues.
    for record in records:
        channel.basic_publish(exchange=EXCHANGE, routing_key='', mandatory=True,
            body=json.dumps(record, allow_nan=False).encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2, content_type='application/json',
                                            message_id=record['event_id']))


def store_pending(records, source):
    body = '\n'.join(json.dumps(r, allow_nan=False) for r in records).encode('utf-8')
    digest = hashlib.sha256(body).hexdigest()
    key = f'bronze/events/{source}/fallback_{digest}.json'
    client('s3').put_object(Bucket=required('S3_BUCKET'), Key=key, Body=body,
                           ContentType='application/x-ndjson', Tagging='rabbitmq=pending')
    return key


def replay_pending(channel, source, context):
    """Replay retained fallback files, tagging confirmed deliveries without deleting data.

    Invocations may supply replay_only=true to drain the backlog without an API call.
    Replays are at least once; consumers use event IDs / source observation timestamps.
    """
    s3 = client('s3')
    bucket = required('S3_BUCKET')
    count = 0
    for page in s3.get_paginator('list_objects_v2').paginate(
            Bucket=bucket, Prefix=f'bronze/events/{source}/fallback_'):
        for obj in page.get('Contents', []):
            if count >= 5 or (context and context.get_remaining_time_in_millis() < 12000):
                return count
            tags = s3.get_object_tagging(Bucket=bucket, Key=obj['Key'])['TagSet']
            if not any(t == {'Key': 'rabbitmq', 'Value': 'pending'} for t in tags):
                continue
            data = s3.get_object(Bucket=bucket, Key=obj['Key'])['Body'].read().decode('utf-8')
            publish(channel, [json.loads(line) for line in data.splitlines() if line.strip()])
            tags = [t for t in tags if t['Key'] != 'rabbitmq'] + [{'Key': 'rabbitmq', 'Value': 'replayed'}]
            s3.put_object_tagging(Bucket=bucket, Key=obj['Key'], Tagging={'TagSet': tags})
            count += 1
    return count


def deliver(records, source, context=None):
    try:
        connection = connect()
    except (pika.exceptions.ProbableAuthenticationError, pika.exceptions.ProbableAccessDeniedError):
        raise  # Bad credentials are not an offline environment.
    except (pika.exceptions.AMQPConnectionError, socket.gaierror, OSError):
        if not broker_stopped():
            raise
        key = store_pending(records, source) if records else None
        return {'rabbit_published': False, 's3_saved': bool(records), 'pending_rds': bool(records),
                'fallback_key': key, 'records': len(records)}
    try:
        channel = prepare_channel(connection)
        publish(channel, records)
        replayed = replay_pending(channel, source, context)
        return {'rabbit_published': True, 's3_saved': False, 'pending_rds': False,
                'records': len(records), 'replayed_files': replayed}
    finally:
        if connection.is_open:
            connection.close()

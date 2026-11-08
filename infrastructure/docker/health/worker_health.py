"""Liveness + broker readiness, not proof of message processing or SMTP delivery."""
import os
from pathlib import Path
import sys

import pika

expected = sys.argv[1].encode()
assert any(
    expected in path.read_bytes() and path.parent.name != str(os.getpid())
    for path in Path('/proc').glob('[0-9]*/cmdline') if path.exists()
), 'Worker process missing'
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host=os.environ['RABBITMQ_HOST'], port=int(os.getenv('RABBITMQ_PORT', '5672')),
    credentials=pika.PlainCredentials(os.environ['RABBITMQ_USER'], os.environ['RABBITMQ_PASS']),
    socket_timeout=3, stack_timeout=5, connection_attempts=1,
))
connection.close()

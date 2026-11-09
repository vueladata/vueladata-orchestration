"""OpenSky F2: normalized positions, no fabricated observations on API errors."""
import json
from datetime import datetime, timezone
from urllib.request import Request, urlopen

from event_transport import deliver

URL = 'https://opensky-network.org/api/states/all?lamin=40.0&lamax=41.5&lomin=-74.5&lomax=-71.5'


def normalize(data):
    records = []
    for row in data.get('states') or []:
        if len(row) < 14 or row[3] is None or row[5] is None or row[6] is None:
            continue
        icao = str(row[0]).lower()
        timestamp = datetime.fromtimestamp(row[3], timezone.utc).isoformat()
        event_id = f'opensky:{icao}:{row[3]}'
        records.append({'event_type': 'opensky_adsb', 'event_id': event_id,
            'id_externo_posicion': event_id, 'codigo_icao24': icao,
            'callsign': (row[1] or '').strip(), 'fecha_hora_posicion_utc': timestamp,
            'latitud': float(row[6]), 'longitud': float(row[5]),
            'altitud_m': row[13] if row[13] is not None else row[7],
            'velocidad_kmh': float(row[9]) * 3.6 if row[9] is not None else None,
            'rumbo_grados': row[10], 'en_tierra': bool(row[8])})
    return records


def lambda_handler(event, context):
    records = []
    if not event.get('replay_only'):
        # One request per invocation. HTTP 429/error propagates; never invent telemetry.
        with urlopen(Request(URL, headers={'User-Agent': 'VuelaData-Ingestor/2.0'}), timeout=10) as response:
            records = normalize(json.load(response))
    result = deliver(records, 'opensky', context)
    return {'statusCode': 200, 'body': json.dumps(result)}

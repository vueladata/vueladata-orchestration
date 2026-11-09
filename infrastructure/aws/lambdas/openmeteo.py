"""Open-Meteo F3: normalized observation with explicit units and UTC time."""
import json
from datetime import datetime, timezone
from urllib.request import Request, urlopen

from event_transport import deliver

URL = ('https://api.open-meteo.com/v1/forecast?latitude=33.6407&longitude=-84.4277'
       '&current=temperature_2m,precipitation,weather_code,wind_speed_10m,wind_direction_10m'
       '&hourly=visibility&forecast_days=1&timezone=UTC&wind_speed_unit=kmh')


def normalize(data):
    current = data['current']
    timestamp = datetime.fromisoformat(current['time']).replace(tzinfo=timezone.utc)
    hourly = data.get('hourly', {})
    hour = timestamp.strftime('%Y-%m-%dT%H:00')
    times = hourly.get('time', [])
    visibility = hourly.get('visibility', [])[times.index(hour)] if hour in times else None
    return {'event_type': 'clima', 'event_id': f'clima:ATL:{timestamp.isoformat()}',
            'id_aeropuerto': 'ATL', 'codigo_iata': 'ATL',
            'fecha_hora_observacion_utc': timestamp.isoformat(),
            'visibilidad_km': visibility / 1000 if visibility is not None else None,
            'temperatura_c': current.get('temperature_2m'),
            'velocidad_viento_kmh': current.get('wind_speed_10m'),
            'direccion_viento_grados': current.get('wind_direction_10m'),
            'precipitacion_mm': current.get('precipitation'),
            'codigo_clima': str(current['weather_code']) if current.get('weather_code') is not None else None}


def lambda_handler(event, context):
    records = []
    if not event.get('replay_only'):
        with urlopen(Request(URL, headers={'User-Agent': 'VuelaData-Ingestor/2.0'}), timeout=10) as response:
            records = [normalize(json.load(response))]
    result = deliver(records, 'clima', context)
    return {'statusCode': 200, 'body': json.dumps(result)}

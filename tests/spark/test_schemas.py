import ast
import json
from pathlib import Path
from vueladata_etl.pipeline.bronze.reader import BronzeReader


def test_all_text_readers_have_explicit_schemas():
    root=Path(__file__).resolve().parents[2]/'src'
    checked=0
    for path in root.rglob('*.py'):
        tree=ast.parse(path.read_text(encoding='utf-8'))
        for call in ast.walk(tree):
            if isinstance(call,ast.Call) and isinstance(call.func,ast.Attribute) and call.func.attr in ('csv','json'):
                chain=ast.dump(call.func.value)
                assert "attr='schema'" in chain, str(path)
                checked+=1
            if isinstance(call,ast.keyword) and call.arg=='inferSchema':
                assert not (isinstance(call.value,ast.Constant) and call.value.value is True)
    assert checked>=10


def test_canonical_event_schemas(spark,tmp_path):
    rows={
        'oooi':{'id_externo_evento':'evt1','id_vuelo':1,'codigo_evento':'IN','fecha_hora_evento_utc':'2026-01-01T10:00:00Z'},
        'pagos':{'id_externo_evento':'pay1','id_pago':1,'estado_reportado':'PAGADO','fecha_hora_evento_utc':'2026-01-01T10:00:00Z','monto_compensado':25.0},
        'clima':{'id_aeropuerto':'JFK','fecha_hora_observacion_utc':'2026-01-01T10:00:00Z','visibilidad_km':0.5,'velocidad_viento_kmh':51.0},
        'opensky':{'id_externo_posicion':'pos1','fecha_hora_posicion_utc':'2026-01-01T10:00:00Z','latitud':1.0,'longitud':2.0,'en_tierra':False},
    }
    for folder,row in rows.items():
        target=tmp_path/folder
        target.mkdir()
        (target/'part.json').write_text(json.dumps(row))
    reader=BronzeReader(spark)
    for method in [reader.read_oooi_events,reader.read_payment_events,reader.read_weather_observations,reader.read_opensky_events]:
        assert method(str(tmp_path)).count()==1

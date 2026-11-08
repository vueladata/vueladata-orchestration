import json
from datetime import datetime
from pathlib import Path
import pytest
from pyspark.sql import functions as F
from vueladata_etl.core.schemas import SCHEMA_F1_FLIGHTS
from vueladata_etl.pipeline.silver.cleaner_flights import FlightsCleaner
from vueladata_etl.pipeline.silver.validator_quarantine import FlightQualityValidator
from vueladata_etl.pipeline.silver.catalogs import historical_timestamps
from vueladata_etl.pipeline.gold.business_rules import compensation_rows, classify_cause
from vueladata_etl.pipeline.gold.rules_otp import OtpRuleEngine
from vueladata_etl.pipeline.gold.rules_propagation import DelayPropagationEngine
from vueladata_etl.pipeline.gold.rules_weather import WeatherRuleEngine
from vueladata_etl.pipeline.gold.rules_connections import ConnectionsRuleEngine
from vueladata_etl.pipeline.gold.rules_compensations import CompensationRuleEngine
from vueladata_etl.pipeline.gold.rules_causes import CausesRuleEngine
from vueladata_etl.pipeline.gold.star import build_star


def base(spark):
    row = {f.name: None for f in SCHEMA_F1_FLIGHTS}
    row.update(YEAR=2015, MONTH=1, DAY=1, AIRLINE='AA', FLIGHT_NUMBER='1',
               TAIL_NUMBER='N1', ORIGIN_AIRPORT='LAX', DESTINATION_AIRPORT='JFK',
               SCHEDULED_DEPARTURE='2300', SCHEDULED_ARRIVAL='0700', SCHEDULED_TIME=300.0,
               ARRIVAL_DELAY=14.0, DEPARTURE_DELAY=0.0, CANCELLED=0, DIVERTED=0, DISTANCE=2000.0)
    return FlightsCleaner.clean_f1_flights(spark.createDataFrame([row], SCHEMA_F1_FLIGHTS))


def test_ca01_and_rn10(spark):
    df = spark.createDataFrame([(14.,0,0), (15.,0,0), (16.,0,0), (0.,0,1), (None,1,0), (None,0,0)],
                              'retraso_llegada_min double, cancelado int, desviado int')
    for k,v in {'anio':2015,'mes':1,'codigo_aerolinea':'AA','aeropuerto_origen':'LAX','retraso_salida_min':0.}.items():
        df=df.withColumn(k,F.lit(v))
    df=OtpRuleEngine.apply_otp_rule(df)
    assert [r.es_puntual_otp for r in df.collect()] == [1,1,0,0,0,0]
    row=OtpRuleEngine.aggregate_kpi_otp(df).first()
    assert row.total_vuelos_desviados == 1 and row.total_vuelos_operados == 3
    assert row.tasa_otp_porcentaje == 66.67
    assert row.total_vuelos_sin_hora_llegada == 1


@pytest.mark.parametrize('delay,cause,cancel,notice,rate', [
    (119.,'TECNICA',0,None,0.), (120.,'TECNICA',0,None,.25), (180.,'TECNICA',0,None,.25),
    (239.,'TECNICA',0,None,.25), (240.,'TECNICA',0,None,.5), (360.,'TECNICA',0,None,.5),
    (361.,'TECNICA',0,None,1.), (300.,'CLIMA',0,None,0.), (300.,'ATC',0,None,0.),
    (None,'TECNICA',1,71.,1.), (None,'TECNICA',1,72.,0.), (None,'TECNICA',1,None,None),
])
def test_ca02_ca03_compensation_boundaries(spark, delay, cause, cancel, notice, rate):
    df=spark.createDataFrame([(delay,cause,cancel,notice,1000.)],
        'retraso_llegada_min double, codigo_causa_original string, cancelado int, aviso_cancelacion_horas double, monto_tarifa_pagada double')
    row=compensation_rows(df).first()
    assert row.porcentaje_compensacion == rate
    assert row.monto_compensacion == (1000*rate if rate is not None else None)
    assert row.derecho_notificacion


@pytest.mark.parametrize('prior,expected', [(True,True),(False,False),(None,None)])
def test_rn03_inherited_cause(spark, prior, expected):
    df=spark.createDataFrame([('AERONAVE_ANTERIOR',prior)], 'codigo_causa_original string, causa_previa_imputable boolean')
    assert classify_cause(df).first().es_imputable == expected


@pytest.mark.parametrize('visibility,wind,airport,hour,expected', [(.9,50.,'JFK',7,True),(1.,50.,'JFK',7,False),(1.,50.1,'LAX',23,True),(.1,70.,'MIA',7,False),(.1,70.,'JFK',8,False)])
def test_ca04_weather_spacetime(spark, visibility, wind, airport, hour, expected):
    df=base(spark).withColumn('codigo_causa_original',F.lit('TECNICA'))
    df=df.withColumn('salida_real_ts',F.lit(datetime(2015,1,1,23))).withColumn('llegada_real_ts',F.lit(datetime(2015,1,1,7)))
    wx=spark.createDataFrame([(airport,datetime(2015,1,1,hour),visibility,wind)]*2,
        'codigo_iata string, fecha_hora_observacion_utc timestamp, visibilidad_km double, velocidad_viento_kmh double')
    result=WeatherRuleEngine.apply_weather_audit(df,wx).collect()
    assert len(result)==1 and result[0].requiere_auditoria == expected


@pytest.mark.parametrize('kind,expected',[('NACIONAL',45.),('INTERNACIONAL',30.)])
def test_ca05_across_midnight(spark,kind,expected):
    df=spark.createDataFrame([('N1',2015,1,1,'2350','1',90.,kind),('N1',2015,1,2,'0100','2',0.,kind)],
        'matricula_aeronave string, anio int, mes int, dia int, salida_programada_raw string, numero_vuelo string, retraso_llegada_min double, tipo_vuelo string')
    rows=DelayPropagationEngine.calculate_propagation(df).orderBy('dia').collect()
    assert rows[1].retraso_propagado_estimado == expected


def test_rn07_actual_connection(spark):
    df=spark.createDataFrame([(1,datetime(2026,1,1,10),datetime(2026,1,1,10,40),50.)],
        'id_conexion long, llegada_conexion_utc timestamp, salida_conexion_utc timestamp, tiempo_min_conexion_min double')
    row=ConnectionsRuleEngine.evaluate_connections(df).first()
    assert row.es_conexion_en_riesgo and row.es_conexion_perdida==0
    assert row.minutos_escala_disponible==40


def test_f1_dot_and_midnight(spark):
    df=base(spark)
    df=df.unionByName(df.withColumn('aeropuerto_origen',F.lit('12345')))
    valid,rejected=FlightQualityValidator.validar_y_separar(df)
    assert valid.count()==1  # 23:00 -> 07:00 is not a chronological error.
    assert rejected.first().motivo_rechazo=='AEROPUERTO_DOT_O_IATA_INVALIDO'


def test_rn02_f1_utc_and_local(spark):
    zones=spark.createDataFrame([('LAX','America/Los_Angeles'),('JFK','America/New_York')], 'codigo_iata string, zona_horaria string')
    row=historical_timestamps(base(spark),zones).first()
    assert row.salida_programada_ts==datetime(2015,1,2,7)
    assert row.llegada_programada_ts==datetime(2015,1,2,12)
    assert row.llegada_programada_local==datetime(2015,1,2,7)
    assert row.retraso_llegada_min==14


def outputs(df):
    otp=OtpRuleEngine.apply_otp_rule(df)
    return {
        'puntualidad_aerolineas':OtpRuleEngine.aggregate_kpi_otp(otp),
        'puntualidad_rutas':OtpRuleEngine.aggregate_kpi_otp_rutas_franjas(otp),
        'causas_retraso':CausesRuleEngine.aggregate_kpi_causas(otp),
        'costos_compensacion':CompensationRuleEngine.aggregate_kpi_compensaciones(otp),
        'conexiones_hubs':ConnectionsRuleEngine.aggregate_kpi_conexiones(otp),
        'propagacion_flota':DelayPropagationEngine.aggregate_propagation_kpi(DelayPropagationEngine.calculate_propagation(otp)),
        'impacto_clima':WeatherRuleEngine.aggregate_kpi_clima(otp),
    }


def test_seven_gold_contracts(spark):
    contract=json.loads((Path(__file__).parent/'gold_contract.json').read_text())
    tables=outputs(base(spark))
    assert set(tables)==set(contract)
    for name, table in tables.items():
        actual={f.name:f.dataType.simpleString() for f in table.schema}
        assert {k:actual.get(k) for k in contract[name]}==contract[name],name
        table.collect()  # Force Catalyst and actual execution, not schema-only checks.


def test_additive_star_keys(spark):
    df=DelayPropagationEngine.calculate_propagation(OtpRuleEngine.apply_otp_rule(base(spark)))
    star=build_star(df,df.limit(0))
    assert set(star)=={'dim_fecha','dim_aeropuerto','dim_aerolinea','dim_aeronave','dim_causa','dim_ruta','fact_vuelos','fact_compensaciones','fact_conexiones'}
    assert star['fact_vuelos'].select('id_vuelo_gold').distinct().count()==1
    assert star['fact_vuelos'].join(star['dim_ruta'],'id_ruta_gold','left_anti').count()==0


def test_inherited_cause_chain_and_unknown_reset(spark):
    from vueladata_etl.pipeline.gold.business_rules import previous_cause
    codes=['TECNICA','AERONAVE_TARDIA','AERONAVE_TARDIA','CLIMA','AERONAVE_TARDIA',None,'AERONAVE_TARDIA']
    df=spark.createDataFrame([(str(i),code) for i,code in enumerate(codes)],'numero_vuelo string, codigo_causa_original string')
    for key,value in {'matricula_aeronave':'N1','anio':2015,'mes':1,'dia':1,'salida_programada_raw':'1000'}.items():
        df=df.withColumn(key,F.lit(value))
    actual=classify_cause(previous_cause(df)).orderBy('numero_vuelo').collect()
    assert [r.es_imputable for r in actual]==[True,True,True,False,False,None,None]

from datetime import datetime as dt
from pyspark.sql import functions as F
from vueladata_etl.pipeline.silver.operational import enrich_flights, connection_rows
from vueladata_etl.pipeline.silver.cleaner_flights import FlightsCleaner
from vueladata_etl.pipeline.gold.business_rules import compensation_rows
from vueladata_etl.pipeline.gold.rules_connections import ConnectionsRuleEngine


def test_real_catalogs_fares_and_connection(spark):
    def frame(rows, schema):
        return spark.createDataFrame(rows, schema)
    tables = {
        'ciudad':frame([(1,1),(2,2)],'id_ciudad long, id_pais long'),
        'aeropuerto':frame([(1,1,'LAX','America/Los_Angeles','Los Angeles International'),(2,2,'MEX','America/Mexico_City','Benito Juarez')],
            'id_aeropuerto long, id_ciudad long, codigo_iata string, zona_horaria string, nombre string'),
        'aerolinea':frame([(1,'VD')],'id_aerolinea long, codigo_iata string'),
        'aeronave':frame([(1,1,'REAL-TAIL')],'id_aeronave long, id_aerolinea long, matricula string'),
        'ruta':frame([(1,1,2)],'id_ruta long, id_aeropuerto_origen long, id_aeropuerto_destino long'),
        'estado_vuelo':frame([(99,'CANCELADO'),(2,'ARRIBADO')],'id_estado_vuelo long, codigo string'),
        'vuelo':frame([(1,1,1,'V1',dt(2026,1,1,8),dt(2026,1,1,10),dt(2026,1,1,8),dt(2026,1,1,13),2,None),
                      (2,1,1,'V2',dt(2026,1,1,13,40),dt(2026,1,1,15),None,None,99,None)],
            'id_vuelo long, id_ruta long, id_aeronave long, numero_vuelo string, salida_programada_utc timestamp, llegada_programada_utc timestamp, salida_real_puerta_utc timestamp, llegada_real_puerta_utc timestamp, id_estado_vuelo_actual long, id_aeropuerto_desvio long'),
        'causa':frame([(1,'TECNICA','Técnica',True)],'id_causa long, codigo string, nombre string, es_imputable boolean'),
        'incidencia_vuelo':frame([(1,1,1,True,False,dt(2026,1,1,10)),(2,1,1,True,False,dt(2026,1,1,11))],
            'id_incidencia long, id_vuelo long, id_causa long, es_causa_principal boolean, requiere_auditoria boolean, fecha_clasificacion_utc timestamp'),
        'detalle_reserva':frame([(1,1,1000.02,'COP'),(2,1,1000.02,'COP'),(3,2,1000.,'COP')],
            'id_detalle_reserva long, id_vuelo long, monto_tarifa_pagada double, moneda string'),
        'evento_operacional':frame([(2,'CANCEL',dt(2026,1,1,10))], 'id_vuelo long, codigo_evento string, fecha_hora_evento_utc timestamp'),
        'conexion_itinerario':frame([(1,1,1,3)],'id_conexion long, id_regla_conexion long, id_detalle_reserva_llegada long, id_detalle_reserva_salida long'),
        'tiempo_minimo_conexion':frame([(1,2,50)],'id_regla_conexion long, id_aeropuerto long, minutos_minimos int'),
        'evaluacion_conexion':frame([], 'id_conexion long, id_evaluacion long, fecha_evaluacion_utc timestamp, estado string, llegada_estimada_utc timestamp, salida_estimada_utc timestamp'),
    }
    read=tables.__getitem__
    flights=FlightsCleaner.clean_rds_flights(enrich_flights(read))
    rows={r.id_vuelo:r for r in compensation_rows(flights).collect()}
    assert len(rows)==2  # Multiple incidents cannot duplicate a flight/fare.
    assert rows[1].aeropuerto_destino=='MEX' and rows[1].matricula_aeronave=='REAL-TAIL'
    assert rows[1].tipo_vuelo=='INTERNACIONAL'
    assert rows[1].monto_compensacion==500.02  # round EACH segment, not sum then round.
    assert rows[2].cancelado==1  # State catalog code, never a hard-coded state id.
    con=ConnectionsRuleEngine.evaluate_connections(connection_rows(read)).first()
    assert con.aeropuerto_hub=='MEX' and con.minutos_escala_disponible==40
    assert con.es_conexion_en_riesgo

    assert con.es_conexion_perdida == 1  # The outgoing segment was cancelled.

    hub = ConnectionsRuleEngine.aggregate_kpi_conexiones(connection_rows(read)).first()
    assert hub.nombre_hub == "Benito Juarez"
    assert hub.total_conexiones_programadas == 1

"""Join operational records using real FK catalogs; no synthetic hubs or fares."""
from pyspark.sql import Window, functions as F


def enrich_flights(read):
    flights = read('vuelo')
    routes = read('ruta').select('id_ruta', 'id_aeropuerto_origen', 'id_aeropuerto_destino')
    aircraft = read('aeronave').select('id_aeronave', 'id_aerolinea', F.col('matricula').alias('matricula_aeronave'))
    airlines = read('aerolinea').select('id_aerolinea', F.col('codigo_iata').alias('codigo_aerolinea'))
    states = read('estado_vuelo').select(F.col('id_estado_vuelo').alias('id_estado_vuelo_actual'), F.col('codigo').alias('codigo_estado'))
    df = flights.join(F.broadcast(routes), 'id_ruta').join(F.broadcast(aircraft), 'id_aeronave').join(F.broadcast(airlines), 'id_aerolinea').join(F.broadcast(states), 'id_estado_vuelo_actual')
    for side in ('origen', 'destino'):
        airports = read('aeropuerto').join(F.broadcast(read('ciudad').select('id_ciudad', 'id_pais')), 'id_ciudad').select(F.col('id_aeropuerto').alias('id_aeropuerto_' + side),
            F.col('codigo_iata').alias('aeropuerto_' + side), F.col('zona_horaria').alias('zona_' + side), F.col('id_pais').alias('pais_' + side))
        df = df.join(F.broadcast(airports), 'id_aeropuerto_' + side)
    df = df.withColumn('tipo_vuelo', F.when(F.col('pais_origen') == F.col('pais_destino'), 'NACIONAL').otherwise('INTERNACIONAL'))
    causes = read('causa').select('id_causa', F.col('codigo').alias('codigo_causa_original'), 'nombre', 'es_imputable')
    incidents = read('incidencia_vuelo').filter(F.col('es_causa_principal'))
    events = read('evento_operacional')
    if 'id_evento_origen' in incidents.columns and 'id_evento' in events.columns:
        incidents = incidents.join(events.select(F.col('id_evento').alias('id_evento_origen'), 'fecha_hora_evento_utc'), 'id_evento_origen', 'left')
    else:
        incidents = incidents.withColumn('fecha_hora_evento_utc', F.lit(None).cast('timestamp'))
    incidents = (incidents.withColumn('_rn', F.row_number().over(Window.partitionBy('id_vuelo').orderBy(F.col('fecha_clasificacion_utc').desc(), F.col('id_incidencia').desc())))
                 .filter('_rn = 1').join(F.broadcast(causes), 'id_causa')
                 .select('id_vuelo', 'fecha_hora_evento_utc', 'codigo_causa_original', F.col('nombre').alias('nombre_causa'), 'requiere_auditoria', F.col('es_imputable').alias('_imputable')))
    df = df.join(incidents, 'id_vuelo', 'left')
    w = Window.partitionBy('matricula_aeronave').orderBy('salida_programada_utc', 'id_vuelo')
    df = df.withColumn('causa_previa_imputable', F.lag('_imputable').over(w)).drop('_imputable')
    # Keep currency coverage explicit; never add USD values to a COP metric.
    fares = (read('detalle_reserva').groupBy('id_vuelo')
             .agg(F.count('*').alias('pasajeros_afectados'),
                  F.when(F.min('moneda') == F.max('moneda'), F.min('moneda')).alias('moneda'),
                  F.sum('monto_tarifa_pagada').cast('double').alias('monto_tarifa_pagada'),
                  F.collect_list(F.col('monto_tarifa_pagada').cast('double')).alias('_tarifas_segmento')))
    df = df.join(fares, 'id_vuelo', 'left')
    cancellation = (read('evento_operacional').filter(F.col('codigo_evento') == 'CANCEL')
                    .groupBy('id_vuelo').agg(F.min('fecha_hora_evento_utc').alias('_aviso')))
    return (df.join(cancellation, 'id_vuelo', 'left').withColumn('aviso_cancelacion_horas',
            (F.col('salida_programada_utc').cast('timestamp').cast('long') - F.col('_aviso').cast('timestamp').cast('long')) / 3600).drop('_aviso'))


def connection_rows(read):
    links = read('conexion_itinerario')
    details = read('detalle_reserva')
    flights = read('vuelo').join(F.broadcast(read('estado_vuelo').select(F.col('id_estado_vuelo').alias('id_estado_vuelo_actual'), F.col('codigo').alias('_estado'))), 'id_estado_vuelo_actual')
    for side, suffix in (('llegada', 'llegada'), ('salida', 'salida')):
        detail = details.select(F.col('id_detalle_reserva').alias('id_detalle_reserva_' + side), F.col('id_vuelo').alias('_vuelo_' + side))
        links = links.join(detail, 'id_detalle_reserva_' + side)
        actual = 'llegada_real_puerta_utc' if side == 'llegada' else 'salida_real_puerta_utc'
        projected = F.coalesce(F.col(actual), F.col(side + '_programada_utc'))
        selected = flights.select(F.col('id_vuelo').alias('_vuelo_' + side), projected.alias(side + '_conexion_utc'), F.col(actual).alias('_real_' + side), F.col('_estado').isin('CANCELADO', 'CANCELLED', 'CANCEL').alias('_cancel_' + side))
        links = links.join(selected, '_vuelo_' + side)
    minimum = read('tiempo_minimo_conexion').select('id_regla_conexion', 'id_aeropuerto', F.col('minutos_minimos').alias('tiempo_min_conexion_min'))
    airports = read('aeropuerto').select('id_aeropuerto', F.col('codigo_iata').alias('aeropuerto_hub'), F.col('zona_horaria').alias('zona_hub'))
    links = links.join(F.broadcast(minimum), 'id_regla_conexion').join(F.broadcast(airports), 'id_aeropuerto')
    latest = (read('evaluacion_conexion').withColumn('_rn', F.row_number().over(Window.partitionBy('id_conexion').orderBy(F.col('fecha_evaluacion_utc').desc(), F.col('id_evaluacion').desc())))
              .filter('_rn = 1').select('id_conexion', F.col('estado').alias('estado_conexion'), 'llegada_estimada_utc', 'salida_estimada_utc'))
    return (links.join(latest, 'id_conexion', 'left')
            .withColumn('conexion_cancelada', F.col('_cancel_llegada') | F.col('_cancel_salida'))
            .withColumn('llegada_conexion_utc', F.coalesce('_real_llegada', 'llegada_estimada_utc', 'llegada_conexion_utc'))
            .withColumn('salida_conexion_utc', F.coalesce('_real_salida', 'salida_estimada_utc', 'salida_conexion_utc'))
            .withColumn('fecha_conexion', F.to_date(F.from_utc_timestamp('llegada_conexion_utc', F.col('zona_hub'))))
            .withColumn('anio', F.year('fecha_conexion')).withColumn('mes', F.month('fecha_conexion')))

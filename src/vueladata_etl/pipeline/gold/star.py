"""Additional star model. Existing seven BI prefixes are never renamed."""
from pyspark.sql import functions as F
from vueladata_etl.pipeline.gold.business_rules import optional, compensation_rows
from vueladata_etl.pipeline.gold.rules_connections import ConnectionsRuleEngine


def key(*columns):
    # JSON preserves boundaries and explicit nulls; no delimiter collisions.
    return F.sha2(F.to_json(F.struct(*columns), {'ignoreNullFields': 'false'}), 256)


def build_star(flights, connections):
    df = compensation_rows(flights)
    df = (df.withColumn('id_fecha', F.date_format('fecha_vuelo', 'yyyyMMdd').cast('int'))
          .withColumn('id_aerolinea_gold', F.coalesce(F.col('codigo_aerolinea'), F.lit('DESCONOCIDA')))
          .withColumn('id_aeronave_gold', F.coalesce(F.col('matricula_aeronave'), F.lit('DESCONOCIDA')))
          .withColumn('id_causa_gold', F.coalesce(F.col('codigo_causa'), F.lit('DESCONOCIDA')))
          .withColumn('id_ruta_gold', key(F.col('aeropuerto_origen'), F.col('aeropuerto_destino')))
          .withColumn('id_vuelo_gold', key(*[F.col(n) for n in ['fuente', 'codigo_aerolinea', 'numero_vuelo', 'fecha_vuelo', 'salida_programada_raw', 'aeropuerto_origen', 'aeropuerto_destino']])))
    tables = {
        'dim_fecha': df.select('id_fecha', 'fecha_vuelo', 'anio', 'mes', 'dia').distinct(),
        'dim_aerolinea': df.select('id_aerolinea_gold', 'codigo_aerolinea').distinct(),
        'dim_aeronave': df.select('id_aeronave_gold', 'matricula_aeronave').distinct(),
        'dim_ruta': df.select('id_ruta_gold', 'aeropuerto_origen', 'aeropuerto_destino').distinct(),
        # Imputability of inherited delays is a fact, not a fixed cause attribute.
        'dim_causa': df.select('id_causa_gold', 'codigo_causa').distinct(),
        'dim_aeropuerto': df.select(F.col('aeropuerto_origen').alias('codigo_iata')).union(
            df.select(F.col('aeropuerto_destino').alias('codigo_iata'))).distinct(),
    }
    fact_columns = ['id_vuelo_gold', 'id_fecha', 'id_aerolinea_gold', 'id_aeronave_gold', 'id_causa_gold', 'id_ruta_gold',
                    'anio', 'mes', 'numero_vuelo', 'aeropuerto_origen', 'aeropuerto_destino',
                    'retraso_llegada_min', 'retraso_salida_min', 'es_puntual_otp', 'es_cancelado', 'es_desviado',
                    'retraso_propagado_estimado', 'es_imputable', 'tipo_imputabilidad']
    tables['fact_vuelos'] = df.select(*fact_columns,
        optional(df, 'salida_programada_ts', 'timestamp').alias('salida_programada_utc'),
        optional(df, 'llegada_programada_ts', 'timestamp').alias('llegada_programada_utc'),
        optional(df, 'salida_programada_local', 'timestamp').alias('salida_programada_local'),
        optional(df, 'llegada_programada_local', 'timestamp').alias('llegada_programada_local'),
        optional(df, 'requiere_auditoria', 'boolean').alias('requiere_auditoria'))
    # Grain: flight/aggregate of booked segment fares, not individual passenger payments.
    tables['fact_compensaciones'] = df.filter((F.col('cancelado') == 1) | (F.col('retraso_llegada_min') >= 120)).select(
        'id_vuelo_gold', 'id_fecha', 'id_causa_gold', 'id_ruta_gold', 'anio', 'mes',
        'porcentaje_compensacion', 'monto_compensacion', 'derecho_notificacion', 'derecho_reprogramacion',
        optional(df, 'moneda').alias('moneda'), optional(df, 'pasajeros_afectados', 'long').alias('pasajeros_afectados'))
    con = ConnectionsRuleEngine.evaluate_connections(connections)
    con = con.filter(optional(con, 'id_conexion', 'long').isNotNull())
    con = con.withColumn('fecha_conexion', F.coalesce(optional(con, 'fecha_conexion', 'date'),
        F.to_date(optional(con, 'llegada_conexion_utc', 'timestamp'))))
    con = con.withColumn('id_fecha', F.date_format('fecha_conexion', 'yyyyMMdd').cast('int'))
    dates = con.select('id_fecha', F.col('fecha_conexion').alias('fecha_vuelo'), 'anio', 'mes', F.dayofmonth('fecha_conexion').alias('dia'))
    tables['dim_fecha'] = tables['dim_fecha'].unionByName(dates).distinct()
    tables['fact_conexiones'] = con.select(optional(con, 'id_conexion', 'long').alias('id_conexion'), 'id_fecha', 'anio', 'mes',
        optional(con, 'aeropuerto_hub').alias('aeropuerto_hub'), 'minutos_escala_disponible', 'es_conexion_en_riesgo', 'es_conexion_perdida')
    return tables

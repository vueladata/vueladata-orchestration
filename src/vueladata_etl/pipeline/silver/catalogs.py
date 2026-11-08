"""Small reference joins and historical local-time conversion (never infer UTC)."""
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


def airport_timezones(airports):
    from timezonefinder import TimezoneFinder
    finder = TimezoneFinder(in_memory=True)
    rows = []
    # Only the small airport catalog reaches the driver, never flight records.
    for row in airports.select('IATA_CODE', 'LATITUDE', 'LONGITUDE').collect():
        zone = (finder.timezone_at(lat=row.LATITUDE, lng=row.LONGITUDE)
                if row.LATITUDE is not None and row.LONGITUDE is not None else None)
        rows.append((row.IATA_CODE, zone))
    schema = StructType([StructField('codigo_iata', StringType()), StructField('zona_horaria', StringType())])
    return airports.sparkSession.createDataFrame(rows, schema)


def historical_timestamps(df, zones):
    for side in ('origen', 'destino'):
        zone = zones.select(F.col('codigo_iata').alias('_iata'), F.col('zona_horaria').alias('zona_' + side))
        df = df.join(F.broadcast(zone), F.col('aeropuerto_' + side) == F.col('_iata'), 'left').drop('_iata')
    raw = F.lpad(F.col('salida_programada_raw'), 4, '0')
    hour = F.substring(raw, 1, 2).cast('int')
    minute = F.substring(raw, 3, 2).cast('int')
    local = (F.to_timestamp('fecha_vuelo').cast('long') + (hour * 60 + minute) * 60).cast('timestamp')
    valid = ((hour.between(0, 23) & minute.between(0, 59)) | ((hour == 24) & (minute == 0)))
    df = (df.withColumn('salida_programada_local', F.when(valid, local))
          .withColumn('salida_programada_ts', F.when(F.col('zona_origen').isNotNull(),
                      F.to_utc_timestamp('salida_programada_local', F.col('zona_origen'))))
          .withColumn('llegada_programada_ts', (F.col('salida_programada_ts').cast('long') + F.col('SCHEDULED_TIME') * 60).cast('timestamp'))
          .withColumn('salida_real_ts', (F.col('salida_programada_ts').cast('long') + F.col('retraso_salida_min') * 60).cast('timestamp'))
          .withColumn('llegada_real_ts', (F.col('llegada_programada_ts').cast('long') + F.col('retraso_llegada_min') * 60).cast('timestamp'))
          .withColumn('llegada_programada_local', F.when(F.col('zona_destino').isNotNull(),
                      F.from_utc_timestamp('llegada_programada_ts', F.col('zona_destino')))))
    return df

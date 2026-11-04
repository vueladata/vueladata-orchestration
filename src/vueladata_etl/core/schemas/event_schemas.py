"""
Esquemas explícitos PySpark para los eventos JSON acumulados por Worker Lake en Bronze.
Cumple con HU-12 evitando inferencia de esquemas.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType
)

SCHEMA_EVENTO_OOOI = StructType([
    StructField("id_externo_evento", StringType(), False),
    StructField("id_vuelo", LongType(), True),
    StructField("numero_vuelo", StringType(), True),
    StructField("codigo_evento", StringType(), False), # OUT, OFF, ON, IN
    StructField("fecha_hora_evento_utc", StringType(), False),
    StructField("codigo_causa_original", StringType(), True)
])

SCHEMA_EVENTO_PAGO = StructType([
    StructField("id_externo_evento", StringType(), False),
    StructField("id_pago", LongType(), True),
    StructField("estado_reportado", StringType(), False), # PENDIENTE, PAGADO, RECHAZADO
    StructField("fecha_hora_evento_utc", StringType(), False),
    StructField("monto_compensado", DoubleType(), True),
    StructField("moneda", StringType(), True)
])

SCHEMA_OBSERVACION_CLIMA = StructType([
    StructField("id_aeropuerto", StringType(), False),
    StructField("fecha_hora_observacion_utc", StringType(), False),
    StructField("visibilidad_km", DoubleType(), True),
    StructField("velocidad_viento_kmh", DoubleType(), True),
    StructField("direccion_viento_grados", DoubleType(), True),
    StructField("temperatura_c", DoubleType(), True),
    StructField("precipitacion_mm", DoubleType(), True),
    StructField("codigo_clima", StringType(), True)
])

SCHEMA_POSICION_AERONAVE = StructType([
    StructField("id_externo_posicion", StringType(), False),
    StructField("codigo_icao24", StringType(), True),
    StructField("matricula", StringType(), True),
    StructField("fecha_hora_posicion_utc", StringType(), False),
    StructField("latitud", DoubleType(), False),
    StructField("longitud", DoubleType(), False),
    StructField("altitud_m", DoubleType(), True),
    StructField("velocidad_kmh", DoubleType(), True),
    StructField("rumbo_grados", DoubleType(), True),
    StructField("en_tierra", BooleanType(), False)
])

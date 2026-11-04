"""
Esquemas explícitos PySpark para la fuente F1: Dataset Histórico de Kaggle (CSV).
Cumple con HU-12 evitando inferencia de esquemas.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

SCHEMA_F1_FLIGHTS = StructType([
    StructField("YEAR", IntegerType(), True),
    StructField("MONTH", IntegerType(), True),
    StructField("DAY", IntegerType(), True),
    StructField("DAY_OF_WEEK", IntegerType(), True),
    StructField("AIRLINE", StringType(), True),
    StructField("FLIGHT_NUMBER", StringType(), True),
    StructField("TAIL_NUMBER", StringType(), True),
    StructField("ORIGIN_AIRPORT", StringType(), True),
    StructField("DESTINATION_AIRPORT", StringType(), True),
    StructField("SCHEDULED_DEPARTURE", StringType(), True),
    StructField("DEPARTURE_TIME", StringType(), True),
    StructField("DEPARTURE_DELAY", DoubleType(), True),
    StructField("TAXI_OUT", DoubleType(), True),
    StructField("WHEELS_OFF", StringType(), True),
    StructField("SCHEDULED_TIME", DoubleType(), True),
    StructField("ELAPSED_TIME", DoubleType(), True),
    StructField("AIR_TIME", DoubleType(), True),
    StructField("DISTANCE", DoubleType(), True),
    StructField("WHEELS_ON", StringType(), True),
    StructField("TAXI_IN", DoubleType(), True),
    StructField("SCHEDULED_ARRIVAL", StringType(), True),
    StructField("ARRIVAL_TIME", StringType(), True),
    StructField("ARRIVAL_DELAY", DoubleType(), True),
    StructField("DIVERTED", IntegerType(), True),
    StructField("CANCELLED", IntegerType(), True),
    StructField("CANCELLATION_REASON", StringType(), True),
    StructField("AIR_SYSTEM_DELAY", DoubleType(), True),
    StructField("SECURITY_DELAY", DoubleType(), True),
    StructField("AIRLINE_DELAY", DoubleType(), True),
    StructField("LATE_AIRCRAFT_DELAY", DoubleType(), True),
    StructField("WEATHER_DELAY", DoubleType(), True)
])

SCHEMA_F1_AIRLINES = StructType([
    StructField("IATA_CODE", StringType(), False),
    StructField("AIRLINE", StringType(), False)
])

SCHEMA_F1_AIRPORTS = StructType([
    StructField("IATA_CODE", StringType(), False),
    StructField("AIRPORT", StringType(), False),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("COUNTRY", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True)
])

"""
Capa Silver: Limpiador y Estandarizador de Datos de Vuelos
Responsabilidad única (SRP): Normalizar esquemas, corregir tipos, manejar nulos y generar el dataset curado de vuelos.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from vueladata_etl.core.constants import FUENTE_F1_KAGGLE, FUENTE_F6_ROTACION, FUENTE_RDS_OPERACIONAL


class FlightsCleaner:
    """Estandariza datasets heterogéneos de vuelos (Kaggle F1 y Rotación F6) a un modelo canónico Silver."""

    @staticmethod
    def _asignar_franja_y_ruta(df: DataFrame, col_hora_raw: str = "salida_programada_raw") -> DataFrame:
        """Calcula de forma estandarizada la ruta comercial y la franja horaria."""
        hora_int = F.coalesce(
            F.substring(F.lpad(F.col(col_hora_raw).cast("string"), 4, "0"), 1, 2).cast(IntegerType()),
            F.lit(12)
        )
        return df \
            .withColumn("ruta", F.concat(F.col("aeropuerto_origen"), F.lit(" - "), F.col("aeropuerto_destino"))) \
            .withColumn(
                "franja_horaria",
                F.when((hora_int >= 0) & (hora_int < 6), "1. Madrugada (00:00 - 05:59)")
                .when((hora_int >= 6) & (hora_int < 12), "2. Mañana (06:00 - 11:59)")
                .when((hora_int >= 12) & (hora_int < 18), "3. Tarde (12:00 - 17:59)")
                .otherwise("4. Noche (18:00 - 23:59)")
            )

    @staticmethod
    def clean_f1_flights(df_raw: DataFrame) -> DataFrame:
        """
        Limpia y estandariza los 5.8 millones de registros de Kaggle F1.
        Mapea nombres en inglés a nomenclatura en español de VuelaData y convierte tipos.
        Preserva desglose analítico de causas para imputabilidad (HU-31).
        """
        df = df_raw \
            .withColumnRenamed("YEAR", "anio") \
            .withColumnRenamed("MONTH", "mes") \
            .withColumnRenamed("DAY", "dia") \
            .withColumnRenamed("AIRLINE", "codigo_aerolinea") \
            .withColumnRenamed("FLIGHT_NUMBER", "numero_vuelo") \
            .withColumnRenamed("TAIL_NUMBER", "matricula_aeronave") \
            .withColumnRenamed("ORIGIN_AIRPORT", "aeropuerto_origen") \
            .withColumnRenamed("DESTINATION_AIRPORT", "aeropuerto_destino") \
            .withColumnRenamed("SCHEDULED_DEPARTURE", "salida_programada_raw") \
            .withColumnRenamed("SCHEDULED_ARRIVAL", "llegada_programada_raw") \
            .withColumnRenamed("DEPARTURE_DELAY", "retraso_salida_min") \
            .withColumnRenamed("ARRIVAL_DELAY", "retraso_llegada_min") \
            .withColumnRenamed("DISTANCE", "distancia_millas") \
            .withColumnRenamed("CANCELLED", "cancelado") \
            .withColumnRenamed("DIVERTED", "desviado") \
            .withColumn("distancia_km", F.round(F.col("distancia_millas") * 1.60934, 2)) \
            .withColumn("cancelado", F.coalesce(F.col("cancelado").cast(IntegerType()), F.lit(0))) \
            .withColumn("desviado", F.coalesce(F.col("desviado").cast(IntegerType()), F.lit(0))) \
            .withColumn("retraso_salida_min", F.coalesce(F.col("retraso_salida_min").cast(DoubleType()), F.lit(0.0))) \
            .withColumn("retraso_llegada_min", F.coalesce(F.col("retraso_llegada_min").cast(DoubleType()), F.lit(0.0))) \
            .withColumn("retraso_aerolinea_min", F.coalesce(F.col("AIRLINE_DELAY").cast(DoubleType()), F.lit(0.0))) \
            .withColumn("retraso_aeronave_tardia_min", F.coalesce(F.col("LATE_AIRCRAFT_DELAY").cast(DoubleType()), F.lit(0.0))) \
            .withColumn("retraso_clima_min", F.coalesce(F.col("WEATHER_DELAY").cast(DoubleType()), F.lit(0.0))) \
            .withColumn("retraso_sistema_atc_min", F.coalesce(F.col("AIR_SYSTEM_DELAY").cast(DoubleType()), F.lit(0.0))) \
            .withColumn("retraso_seguridad_min", F.coalesce(F.col("SECURITY_DELAY").cast(DoubleType()), F.lit(0.0))) \
            .withColumn("tipo_vuelo", F.lit("NACIONAL")) \
            .withColumn("fuente", F.lit(FUENTE_F1_KAGGLE))

        return FlightsCleaner._asignar_franja_y_ruta(df)

    @staticmethod
    def clean_f6_itinerary(df_raw: DataFrame, filter_date: str = None) -> DataFrame:
        """
        Limpia y estandariza el itinerario semanal sintético F6.
        Alinea el esquema con el de F1 para permitir unificación de capa Silver.
        """
        df = df_raw \
            .withColumn("fecha_vuelo", F.to_date(F.col("flight_date"))) \
            .withColumn("anio", F.year(F.col("fecha_vuelo"))) \
            .withColumn("mes", F.month(F.col("fecha_vuelo"))) \
            .withColumn("dia", F.dayofmonth(F.col("fecha_vuelo"))) \
            .withColumn("numero_vuelo", F.col("flight_id")) \
            .withColumn("codigo_aerolinea", F.lit("VD")) \
            .withColumn("matricula_aeronave", F.col("aircraft_registration")) \
            .withColumn("aeropuerto_origen", F.col("origin_airport")) \
            .withColumn("aeropuerto_destino", F.col("destination_airport")) \
            .withColumn("salida_programada_ts", F.to_timestamp(F.col("scheduled_departure"))) \
            .withColumn("llegada_programada_ts", F.to_timestamp(F.col("scheduled_arrival"))) \
            .withColumn("salida_programada_raw", F.col("scheduled_departure")) \
            .withColumn("llegada_programada_raw", F.col("scheduled_arrival")) \
            .withColumn("cancelado", F.lit(0)) \
            .withColumn("desviado", F.lit(0)) \
            .withColumn("retraso_salida_min", F.lit(0.0)) \
            .withColumn("retraso_llegada_min", F.lit(0.0)) \
            .withColumn("tipo_vuelo", F.lit("NACIONAL")) \
            .withColumn("fuente", F.lit(FUENTE_F6_ROTACION))

        if filter_date and filter_date != "all":
            df_filtered = df.filter(F.col("flight_date") == filter_date)
            # Si el filtro por fecha contiene datos, lo usamos; si no, procesamos el lote completo disponible
            if df_filtered.count() > 0:
                df = df_filtered
            else:
                print(f"[*] Aviso: 0 vuelos F6 para '{filter_date}'. Procesando lote sintético completo disponible...")

        return FlightsCleaner._asignar_franja_y_ruta(df)

    @staticmethod
    def clean_rds_flights(df_raw: DataFrame, filter_date: str = None) -> DataFrame:
        """
        Limpia y estandariza los vuelos transaccionales extraídos desde PostgreSQL RDS.
        Transforma timestamps, calcula retrasos de salida/llegada en minutos y añade metadata.
        """
        df = df_raw \
            .withColumn("salida_programada_ts", F.to_timestamp(F.col("salida_programada_utc"))) \
            .withColumn("llegada_programada_ts", F.to_timestamp(F.col("llegada_programada_utc"))) \
            .withColumn("salida_real_ts", F.to_timestamp(F.col("salida_real_puerta_utc"))) \
            .withColumn("llegada_real_ts", F.to_timestamp(F.col("llegada_real_puerta_utc"))) \
            .withColumn("anio", F.year(F.col("salida_programada_ts"))) \
            .withColumn("mes", F.month(F.col("salida_programada_ts"))) \
            .withColumn("dia", F.dayofmonth(F.col("salida_programada_ts"))) \
            .withColumn("retraso_salida_min", F.coalesce(
                F.round((F.unix_timestamp("salida_real_ts") - F.unix_timestamp("salida_programada_ts")) / 60.0, 2),
                F.lit(0.0)
            )) \
            .withColumn("retraso_llegada_min", F.coalesce(
                F.round((F.unix_timestamp("llegada_real_ts") - F.unix_timestamp("llegada_programada_ts")) / 60.0, 2),
                F.lit(0.0)
            )) \
            .withColumn("salida_programada_raw", F.col("salida_programada_utc").cast("string")) \
            .withColumn("llegada_programada_raw", F.col("llegada_programada_utc").cast("string")) \
            .withColumn("codigo_aerolinea", F.lit("VD")) \
            .withColumn("matricula_aeronave", F.concat(F.lit("HK-"), F.col("id_aeronave").cast("string"))) \
            .withColumn("aeropuerto_origen", F.lit("BOG")) \
            .withColumn("aeropuerto_destino", F.lit("MDE")) \
            .withColumn("cancelado", F.when(F.col("id_estado_vuelo_actual") == 5, 1).otherwise(0)) \
            .withColumn("desviado", F.when(F.col("id_aeropuerto_desvio").isNotNull(), 1).otherwise(0)) \
            .withColumn("distancia_km", F.lit(250.0)) \
            .withColumn("tipo_vuelo", F.lit("NACIONAL")) \
            .withColumn("fuente", F.lit(FUENTE_RDS_OPERACIONAL))

        if filter_date and filter_date != "all":
            df_filtered = df.filter(F.to_date(F.col("salida_programada_ts")) == filter_date)
            if df_filtered.count() > 0:
                df = df_filtered
            else:
                print(f"[*] Aviso: 0 vuelos RDS para '{filter_date}'. Procesando lote completo disponible...")

        return FlightsCleaner._asignar_franja_y_ruta(df)

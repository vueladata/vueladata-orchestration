"""
Capa Gold: Motor de Reglas de Negocio - Causas de Retraso e Imputabilidad
═══════════════════════════════════════════════════════════════════════════════
Responsabilidad única (SRP): Modelar el cubo analítico de causas de retraso,
clasificando la participación imputable vs no imputable (HU-31).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


class CausesRuleEngine:
    """Calcula y agrega las incidencias y minutos de demora por tipo de causa e imputabilidad."""

    @staticmethod
    def aggregate_kpi_causas(df_flights: DataFrame) -> DataFrame:
        """
        Unstack/unpivot de las 5 categorías oficiales de retraso:
        - Imputables: AEROLINEA, AERONAVE_TARDIA (Rotación)
        - No Imputables: CLIMA, SISTEMA_ATC, SEGURIDAD
        """
        # Asegurar columnas numéricas de retraso por causa defensivamente
        cause_map = {
            "m_aerolinea": "retraso_aerolinea_min",
            "m_aeronave": "retraso_aeronave_tardia_min",
            "m_clima": "retraso_clima_min",
            "m_sistema": "retraso_sistema_atc_min",
            "m_seguridad": "retraso_seguridad_min",
        }
        df = df_flights
        for alias, col_name in cause_map.items():
            if col_name in df.columns:
                df = df.withColumn(alias, F.coalesce(F.col(col_name).cast(DoubleType()), F.lit(0.0)))
            else:
                df = df.withColumn(alias, F.lit(0.0).cast(DoubleType()))

        # Generar registros desglosados mediante stack
        df_unpivoted = df.select(
            "anio", "mes",
            F.expr("""
                stack(5,
                    'AEROLINEA', 'Mantenimiento y Operaciones Aerolínea', true, 'IMPUTABLE', m_aerolinea,
                    'AERONAVE_TARDIA', 'Llegada Tardía de Aeronave (Rotación)', true, 'IMPUTABLE', m_aeronave,
                    'CLIMA', 'Condiciones Meteorológicas Adversas', false, 'NO IMPUTABLE', m_clima,
                    'SISTEMA_ATC', 'Control de Tráfico Aéreo (ATC / NAS)', false, 'NO IMPUTABLE', m_sistema,
                    'SEGURIDAD', 'Seguridad Aeroportuaria y Terminal', false, 'NO IMPUTABLE', m_seguridad
                ) as (codigo_causa, nombre_causa, es_imputable, tipo_imputabilidad, minutos_retraso)
            """)
        ).filter(F.col("minutos_retraso") > 0)

        kpi = df_unpivoted.groupBy("anio", "mes", "codigo_causa", "nombre_causa", "es_imputable", "tipo_imputabilidad") \
            .agg(
                F.count("*").alias("total_vuelos_afectados"),
                F.round(F.sum("minutos_retraso"), 2).alias("total_minutos_retraso"),
                F.round(F.avg("minutos_retraso"), 2).alias("promedio_minutos_retraso")
            ) \
            .withColumn("fecha_actualizacion_utc", F.current_timestamp())

        return kpi

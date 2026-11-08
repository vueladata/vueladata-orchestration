"""Row-level project rules. Unknown source data stays unknown, never estimated."""
from pyspark.sql import Window, functions as F


def optional(df, name, kind='string'):
    return F.col(name).cast(kind) if name in df.columns else F.lit(None).cast(kind)


def classify_cause(df):
    code = F.upper(F.trim(optional(df, 'codigo_causa_original')))
    previous = optional(df, 'causa_previa_imputable', 'boolean')
    imputable = (F.when(code.isin('TECNICA', 'TRIPULACION', 'AEROLINEA', 'A'), True)
                 .when(code.isin('CLIMA', 'ATC', 'SISTEMA_ATC', 'SEGURIDAD', 'B', 'C', 'D'), False)
                 .when(code.isin('AERONAVE_ANTERIOR', 'AERONAVE_TARDIA', 'ROTACION'), previous))
    return (df.withColumn('codigo_causa', code)
            .withColumn('es_imputable', imputable.cast('boolean'))
            .withColumn('tipo_imputabilidad', F.when(imputable, 'IMPUTABLE')
                        .when(~imputable, 'NO_IMPUTABLE').otherwise('DESCONOCIDA')))


def compensation_rows(df):
    df = classify_cause(df)
    delay = optional(df, 'retraso_llegada_min', 'double')
    cancelled = optional(df, 'cancelado', 'int') == 1
    notice = optional(df, 'aviso_cancelacion_horas', 'double')
    # Boundaries are disjoint: [2,4), [4,6], >6 hours.
    rate = (F.when(~F.col('es_imputable'), 0.0)
            .when(F.col('es_imputable') & cancelled & (notice < 72), 1.0)
            .when(F.col('es_imputable') & cancelled & (notice >= 72), 0.0)
            .when(F.col('es_imputable') & ~cancelled & (delay > 360), 1.0)
            .when(F.col('es_imputable') & ~cancelled & (delay >= 240), 0.5)
            .when(F.col('es_imputable') & ~cancelled & (delay >= 120), 0.25)
            .when(F.col('es_imputable') & ~cancelled & delay.isNotNull(), 0.0))
    fare = optional(df, 'monto_tarifa_pagada', 'double')
    amount = F.round(fare * rate, 2)
    if '_tarifas_segmento' in df.columns:
        amount = F.aggregate(F.transform('_tarifas_segmento', lambda value: F.round(value * rate, 2)),
                             F.lit(0.0), lambda total, value: total + value)
    return (df.withColumn('porcentaje_compensacion', rate)
            .withColumn('monto_compensacion', F.when(rate == 0, 0.0)
                        .when(fare >= 0, amount))
            .withColumn('derecho_notificacion', F.coalesce(cancelled | (delay > 0), F.lit(False)))
            .withColumn('derecho_reprogramacion', F.coalesce(cancelled | (delay > 0), F.lit(False))))


def previous_cause(df):
    """Follow inherited cause chains, resetting at unknown/non-inherited flights."""
    df = classify_cause(df)
    inherited = F.coalesce(F.col('codigo_causa').isin('AERONAVE_ANTERIOR', 'AERONAVE_TARDIA', 'ROTACION'), F.lit(False))
    order = ['anio', 'mes', 'dia', 'salida_programada_raw', 'numero_vuelo']
    if 'salida_programada_ts' in df.columns:
        order = ['salida_programada_ts', 'numero_vuelo']
    w = Window.partitionBy('matricula_aeronave').orderBy(*order)
    df = df.withColumn('_chain', F.sum(F.when(inherited, 0).otherwise(1)).over(w.rowsBetween(Window.unboundedPreceding, 0)))
    root = Window.partitionBy('matricula_aeronave', '_chain').orderBy(*order).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    df = df.withColumn('_root_imputable', F.first('es_imputable', ignorenulls=False).over(root))
    df = df.withColumn('causa_previa_imputable', F.when(F.col('matricula_aeronave').isNotNull() & (F.col('matricula_aeronave') != ''), F.lag('_root_imputable').over(w)))
    return df.drop('_chain', '_root_imputable', 'codigo_causa', 'es_imputable', 'tipo_imputabilidad')

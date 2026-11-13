"""Stage before overwrite to avoid reading files being replaced by this job.

Only one writer may publish this dataset at a time (Airflow pool gold_writer).
Parquet/S3 is not a transaction format; publication failure must fail the run.
"""
from uuid import uuid4
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F


def replace_sources(incoming, output, sources, base):
    spark = incoming.sparkSession
    try:
        previous = spark.read.parquet(output)
    except AnalysisException as exc:
        if exc.getErrorClass() != 'PATH_NOT_FOUND':
            raise
        previous = None

    if previous is None or previous.isEmpty():
        writer = incoming.write.mode('overwrite').option('compression', 'snappy')
        if not incoming.isEmpty():
            writer = writer.partitionBy('anio', 'mes')
        writer.parquet(output)
        return spark.read.parquet(output)

    # Historical load: F1 Kaggle is strictly anio=2015.
    # Write directly with dynamic partition overwrite without reading/staging 5.8M rows.
    if sources == ["F1_KAGGLE"]:
        writer = incoming.write.mode('overwrite').option('partitionOverwriteMode', 'dynamic').option('compression', 'snappy')
        if not incoming.isEmpty():
            writer = writer.partitionBy('anio', 'mes')
        writer.parquet(output)
        return spark.read.parquet(output)

    if "F1_KAGGLE" in sources:
        # Full reload of all sources
        combined = previous.filter(~F.col('fuente').isin(sources)).unionByName(incoming, allowMissingColumns=True)
        write_mode = "static"
    else:
        # Incremental daily reload: do NOT touch F1_KAGGLE (anio=2015)
        # Only combine non-Kaggle partitions (anio >= 2026)
        active_previous = previous.filter((F.col('fuente') != 'F1_KAGGLE') & (~F.col('fuente').isin(sources)))
        combined = active_previous.unionByName(incoming, allowMissingColumns=True)
        write_mode = "dynamic"

    staging = f'{base}/_etl_staging/{uuid4().hex}'
    combined.write.mode('overwrite').option('compression', 'snappy').parquet(staging)
    staged = spark.read.parquet(staging)
    writer = staged.write.mode('overwrite').option('partitionOverwriteMode', write_mode).option('compression', 'snappy')
    if not staged.isEmpty():
        writer = writer.partitionBy('anio', 'mes')
    writer.parquet(output)
    fs_path = spark._jvm.org.apache.hadoop.fs.Path(staging)
    fs_path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(fs_path, True)
    return spark.read.parquet(output)

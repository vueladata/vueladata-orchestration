"""Stage before overwrite to avoid reading files being replaced by this job.

Only one writer may publish this dataset at a time (Airflow pool gold_writer).
Parquet/S3 is not a transaction format; publication failure must fail the run.
"""
from uuid import uuid4
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F


def replace_sources(incoming, output, sources, base):
    spark = incoming.sparkSession
    combined = incoming
    try:
        previous = spark.read.parquet(output)
    except AnalysisException as exc:
        if exc.getErrorClass() != 'PATH_NOT_FOUND':
            raise
    else:
        combined = previous.filter(~F.col('fuente').isin(sources)).unionByName(incoming, allowMissingColumns=True)
    staging = f'{base}/_etl_staging/{uuid4().hex}'
    combined.write.mode('overwrite').option('compression', 'snappy').parquet(staging)
    staged = spark.read.parquet(staging)
    writer = staged.write.mode('overwrite').option('partitionOverwriteMode', 'static').option('compression', 'snappy')
    if not staged.isEmpty():
        writer = writer.partitionBy('anio', 'mes')
    writer.parquet(output)
    fs_path = spark._jvm.org.apache.hadoop.fs.Path(staging)
    fs_path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(fs_path, True)
    return spark.read.parquet(output)

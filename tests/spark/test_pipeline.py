import json
from pathlib import Path
from pyspark.sql import functions as F
import pyarrow.parquet as pq
from vueladata_etl.core.config import PathConfig
from vueladata_etl.core.persistence import replace_sources
from vueladata_etl.pipeline.gold.aggregator import GoldAggregator
from test_rules import base


def test_snapshot_replacement_preserves_other_sources(spark, tmp_path):
    output=str(tmp_path/'silver/vuelos_silver')
    schema='fuente string, anio int, mes int, dia int, value int'
    first=spark.createDataFrame([('F1_KAGGLE',2015,1,1,1),('RDS_OPERACIONAL',2026,1,1,2),('RDS_OPERACIONAL',2026,1,2,3)],schema)
    replace_sources(first,output,['F1_KAGGLE','RDS_OPERACIONAL'],str(tmp_path))
    update=spark.createDataFrame([('RDS_OPERACIONAL',2026,1,1,4),('RDS_OPERACIONAL',2026,1,2,3)],schema)
    for _ in range(2):
        result=replace_sources(update,output,['RDS_OPERACIONAL'],str(tmp_path))
        assert sorted(r.value for r in result.collect())==[1,3,4]
    empty=spark.createDataFrame([],schema)
    assert replace_sources(empty,output,['F1_KAGGLE','RDS_OPERACIONAL'],str(tmp_path)).count()==0


def test_publish_gold_roundtrip(spark, tmp_path):
    paths=PathConfig(str(tmp_path))
    aggregator=GoldAggregator(spark,paths)
    aggregator.process_gold_metrics(base(spark))
    contract=json.loads((Path(__file__).parent/'gold_contract.json').read_text())
    for name, expected in contract.items():
        table=spark.read.parquet(f'{paths.gold_path}/{name}')
        actual={f.name:f.dataType.simpleString() for f in table.schema}
        assert {key:actual[key] for key in expected}==expected
    assert len(list((tmp_path/'gold').iterdir()))==16
    parquet=next((tmp_path/'gold/fact_vuelos').rglob('*.parquet'))
    assert pq.ParquetFile(parquet).metadata.row_group(0).column(0).compression=='SNAPPY'
    # A later correction must remove stale rows, not leave old nonempty partitions.
    no_weather=base(spark).withColumn('retraso_clima_min',F.lit(45.))
    aggregator.process_gold_metrics(no_weather)
    assert spark.read.parquet(f'{paths.gold_path}/impacto_clima').count()==1
    aggregator.process_gold_metrics(base(spark))
    assert spark.read.parquet(f'{paths.gold_path}/impacto_clima').count()==0

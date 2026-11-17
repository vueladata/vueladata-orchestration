import json
from unittest.mock import Mock

import pytest

from vueladata_etl.core.config import PathConfig
from vueladata_etl.pipeline.gold.aggregator import GoldAggregator
from vueladata_etl.pipeline.gold.publication import HistoricalGoldPublication


@pytest.fixture
def publication():
    s3 = Mock()
    publication = HistoricalGoldPublication("s3a://test-bucket/gold", s3)
    objects = {
        f"{publication.snapshot}/fact_vuelos/anio=2015/mes=1/part-new.parquet",
        f"{publication.snapshot}/fact_vuelos/_SUCCESS",
        "gold/fact_vuelos/anio=2015/mes=1/part-old.parquet",
    }
    s3.get_paginator.return_value.paginate.side_effect = lambda *, Bucket, Prefix: [
        {"Contents": [{"Key": key} for key in objects if key.startswith(Prefix)]}
    ]
    s3.delete_objects.return_value = {}
    return publication, s3, objects


def test_all_tables_must_complete_before_publication(publication):
    publisher, s3, _ = publication
    with pytest.raises(RuntimeError, match="dim_fecha"):
        publisher.publish(["fact_vuelos", "dim_fecha"])
    s3.copy.assert_not_called()
    s3.delete_objects.assert_not_called()
    s3.put_object.assert_not_called()


def test_copy_failure_keeps_previous_snapshot_selected(publication):
    publisher, s3, _ = publication
    s3.copy.side_effect = RuntimeError("copy failed")
    with pytest.raises(RuntimeError, match="copy failed"):
        publisher.publish(["fact_vuelos"])
    s3.delete_objects.assert_not_called()
    s3.put_object.assert_not_called()


def test_publishes_snapshot_after_refreshing_existing_paths(publication):
    publisher, s3, _ = publication
    publisher.publish(["fact_vuelos"])
    assert [call[0] for call in s3.method_calls if not call[0].startswith("get_paginator")] == [
        "copy", "delete_objects", "put_object", "put_object",
    ]
    assert s3.copy.call_args.args[-1] == "gold/fact_vuelos/anio=2015/mes=1/part-new.parquet"
    assert s3.delete_objects.call_args.kwargs["Delete"]["Objects"] == [
        {"Key": "gold/fact_vuelos/anio=2015/mes=1/part-old.parquet"},
    ]
    manifest = s3.put_object.call_args.kwargs
    assert manifest["Key"] == "_gold_snapshots/latest.json"
    assert json.loads(manifest["Body"]) == {"path": publisher.path, "tables": ["fact_vuelos"]}


def test_historical_snapshot_preserves_empty_schema_and_partitions(spark, tmp_path, monkeypatch):
    snapshot = Mock(path=str(tmp_path / "snapshot"))
    monkeypatch.setattr(
        "vueladata_etl.pipeline.gold.aggregator.HistoricalGoldPublication", lambda _: snapshot
    )
    flights = spark.createDataFrame([(2015, 1, 10), (2015, 2, 20)], "anio int, mes int, vuelo int")
    tables = {"fact_vuelos": flights, "fact_conexiones": flights.limit(0)}
    GoldAggregator(spark, PathConfig(str(tmp_path)), mode="historical")._publish_historical(tables)
    assert sorted(row.vuelo for row in spark.read.parquet(f"{snapshot.path}/fact_vuelos").collect()) == [10, 20]
    assert (tmp_path / "snapshot/fact_vuelos/anio=2015/mes=1").is_dir()
    empty = spark.read.parquet(f"{snapshot.path}/fact_conexiones")
    assert empty.columns == flights.columns and empty.isEmpty()
    snapshot.publish.assert_called_once_with(tables)

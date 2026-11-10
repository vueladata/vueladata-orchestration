from unittest.mock import Mock, MagicMock
import pandas as pd
from sqlalchemy import types as sqltypes
from vueladata_etl.core.config import PathConfig
from vueladata_etl.pipeline.bronze import extractor_rds
from vueladata_etl.pipeline.bronze.reader import BronzeReader


def test_empty_snapshot_is_readable_and_clears_previous_rows(spark, tmp_path, monkeypatch):
    paths = PathConfig(str(tmp_path))
    extractor = extractor_rds.RdsBronzeExtractor(paths, db_url='unused', spark=spark)
    extractor._engine = Mock()
    extractor._engine.connect.return_value = MagicMock()
    inspector = Mock()
    inspector.get_columns.return_value = [{'name': 'id', 'type': sqltypes.Integer()},
                                         {'name': 'name', 'type': sqltypes.String()}]
    monkeypatch.setattr(extractor_rds, 'inspect', lambda _: inspector)
    rows = pd.DataFrame({'id': [1], 'name': ['existing']})
    monkeypatch.setattr(extractor_rds.pd, 'read_sql', lambda *args, **kwargs: rows.copy())
    reader = BronzeReader(spark)
    directory = f'{paths.bronze_path}/rds_operacional'
    extractor.extraer_tabla_a_bronze('sample', fecha_proceso='2026-11-08')
    extractor.extraer_tabla_a_bronze('sample', fecha_proceso='2026-11-09')
    rows = rows.iloc[:0]
    extractor.extraer_tabla_a_bronze('sample', fecha_proceso='2026-11-09')
    assert reader.read_rds_table(directory, 'sample', '2026-11-09').count() == 0
    assert reader.read_rds_table(directory, 'sample', '2026-11-08').count() == 1

from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from vueladata_etl import main


@pytest.mark.parametrize('layer', ['bronze', 'silver', 'gold'])
def test_independent_stage_consumes_persisted_inputs(monkeypatch, layer):
    args = SimpleNamespace(layer=layer, mode='daily', bucket='/tmp/lake', date='2026-11-09',
                           run_id='batch-1', dag_id='daily', only_star_model=False, sync_rds=False)
    monkeypatch.setattr(main, 'parse_args', lambda: args)
    audit, spark, reader, silver, gold, loader, bronze = [Mock() for _ in range(7)]
    bronze.extraer_todo_operacional.return_value = {'vuelo': 10}
    for name, instance in [('AuditManager', audit), ('BronzeReader', reader),
                           ('SilverTransformer', silver), ('GoldAggregator', gold),
                           ('GoldRdsLoader', loader), ('RdsBronzeExtractor', bronze)]:
        monkeypatch.setattr(main, name, Mock(return_value=instance))
    monkeypatch.setattr(main.SparkSessionFactory, 'create', Mock(return_value=spark))
    main.main()
    assert bronze.extraer_todo_operacional.called == (layer == 'bronze')
    assert silver.process_flights.called == (layer == 'silver')
    assert gold.process_gold_metrics.called == (layer == 'gold')
    if layer == 'gold':
        gold.process_gold_metrics.assert_called_once_with(None)  # reload complete Silver from S3
    if layer == 'silver':
        assert silver.process_flights.call_args.kwargs['filter_date'] == '2026-11-09'
    spark.stop.assert_called_once()

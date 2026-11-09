import sys
from unittest.mock import Mock
import pytest
from vueladata_etl import main as entry


def test_star_entrypoint_skips_rds_and_other_layers(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['etl', '--bucket', '/tmp/lake', '--only-star-model'])
    session = Mock()
    monkeypatch.setattr(entry.SparkSessionFactory, 'create', Mock(return_value=session))
    aggregator = Mock()
    monkeypatch.setattr(entry, 'GoldAggregator', Mock(return_value=aggregator))
    def forbidden(*args, **kwargs):
        raise AssertionError('Star-only must not initialize RDS or other layers')
    for name in ('AuditManager', 'RdsBronzeExtractor', 'SilverTransformer', 'GoldRdsLoader'):
        monkeypatch.setattr(entry, name, forbidden)
    entry.main()
    aggregator.process_gold_metrics.assert_called_once_with(only_star_model=True)
    session.stop.assert_called_once()


@pytest.mark.parametrize('extra', [['--sync-rds'], ['--layer', 'bronze'], ['--layer', 'silver']])
def test_star_rejects_conflicting_options(monkeypatch, extra):
    monkeypatch.setattr(sys, 'argv', ['etl', '--bucket', '/tmp/lake', '--only-star-model', *extra])
    with pytest.raises(SystemExit) as exc:
        entry.parse_args()
    assert exc.value.code == 2

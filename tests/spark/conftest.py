import sys
from pathlib import Path
import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src'))


@pytest.fixture(scope='session')
def spark():
    session = (SparkSession.builder.master('local[2]').appName('VuelaData-rules-tests')
               .config('spark.sql.session.timeZone', 'UTC')
               .config('spark.sql.shuffle.partitions', '2')
               .config('spark.ui.enabled', 'false').getOrCreate())
    session.sparkContext.setLogLevel('ERROR')
    assert session.version == '3.5.1'
    yield session
    session.stop()

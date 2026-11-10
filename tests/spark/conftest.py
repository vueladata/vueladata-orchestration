import os
import sys
import subprocess
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src'))

_docker_spark_tested = False


if sys.platform == 'win32':
    @pytest.fixture(scope='session')
    def spark():
        return None

    def pytest_pyfunc_call(pyfuncitem):
        global _docker_spark_tested
        if not _docker_spark_tested:
            root = Path(__file__).resolve().parents[2]
            cmd = [
                'docker', 'run', '--rm', '--entrypoint', 'pytest',
                '-v', f'{root}:/workspace', '-w', '/workspace',
                'vueladata/spark:audit', 'tests/spark'
            ]
            res = subprocess.run(cmd, capture_output=True, text=True)
            if res.returncode != 0:
                raise RuntimeError(f"Docker spark tests failed:\n{res.stdout}\n{res.stderr}")
            _docker_spark_tested = True
        return True
else:
    from pyspark.sql import SparkSession

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

"""Use the checkout DAG bundle, as the production dag-processor does."""
import os
import signal
import sys
from pathlib import Path
from unittest.mock import MagicMock

if sys.platform == 'win32':
    if 'fcntl' not in sys.modules:
        mock_fcntl = MagicMock()
        mock_fcntl.LOCK_SH = 1
        mock_fcntl.LOCK_EX = 2
        mock_fcntl.LOCK_NB = 4
        mock_fcntl.LOCK_UN = 8
        mock_fcntl.flock = lambda *args, **kwargs: None
        sys.modules['fcntl'] = mock_fcntl
    if not hasattr(signal, 'setitimer'):
        signal.setitimer = lambda *args, **kwargs: None
        signal.ITIMER_REAL = 0
    if not hasattr(signal, 'SIGALRM'):
        signal.SIGALRM = 14

DAGS = str(Path(__file__).resolve().parents[1] / 'dags')
os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = DAGS
os.environ['AIRFLOW__CORE__LOAD_EXAMPLES'] = 'false'
sys.path.insert(0, DAGS)

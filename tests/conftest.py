import os
import sys
from pathlib import Path

# collector.py reads its configuration from the environment at import time.
# The tests never talk to devices or InfluxDB, so placeholder values are
# enough. Set explicitly (not setdefault) so a real .env is never used.
for key, value in {
    "NIBE_HOST": "nibe.test",
    "HW_IP": "192.0.2.1",
    "DUCO_IP": "192.0.2.2",
    "HEALTH_ENDPOINT_URL": "http://host.test/health",
    "HUAWEI_DONGLE_IP": "192.0.2.3",
    "INFLUX_COLLECTOR_TOKEN": "test-token",
    "INFLUX_ORG": "test",
    "INFLUX_BUCKET": "test",
    "OPEN_METEO_LAT": "51.0",
    "OPEN_METEO_LON": "4.0",
}.items():
    os.environ[key] = value

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

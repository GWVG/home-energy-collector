#!/usr/bin/env python3
"""
Polls a NIBE heat pump, a HomeWizard P1 meter, a Duco ventilation unit, a
Huawei SUN2000 inverter and a host-health endpoint every cycle, plus
Open-Meteo and a MeteoBelgie weather-station page on their own, longer
intervals, and writes everything to InfluxDB.

Device points share one timestamp per cycle, so they line up exactly.
Weather points carry the source's own observation time.

Usage:
    uv run collector.py
"""
from __future__ import annotations

import asyncio
import os
import re
import sys
import time
import logging
from datetime import datetime, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from urllib3.exceptions import InsecureRequestWarning
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

import huawei_solar as huawei
from huawei_solar import register_names as hrn

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("collector")

# --- Configuration from the environment ---
NIBE_HOST = os.environ["NIBE_HOST"]  # auth via ~/.netrc, machine = NIBE_HOST
HW_IP = os.environ["HW_IP"]
DUCO_IP = os.environ["DUCO_IP"]
HEALTH_ENDPOINT_URL = os.environ["HEALTH_ENDPOINT_URL"]  # health_endpoint.py on the Proxmox host
HUAWEI_DONGLE_IP = os.environ["HUAWEI_DONGLE_IP"]  # SDongleA, Modbus TCP port 502
# Unit 0 is the dongle itself and doesn't answer here; unit 1 is the inverter.
HUAWEI_UNIT_ID = int(os.environ.get("HUAWEI_UNIT_ID", "1"))
HUAWEI_TIMEOUT = int(os.environ.get("HUAWEI_TIMEOUT", "10"))  # seconds, per Modbus call
INFLUX_URL = os.environ.get("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.environ["INFLUX_COLLECTOR_TOKEN"]  # a write-only token is enough
INFLUX_ORG = os.environ["INFLUX_ORG"]
INFLUX_BUCKET = os.environ["INFLUX_BUCKET"]
INTERVAL = int(os.environ.get("COLLECTOR_INTERVAL", "60"))

# None of the devices measures outdoor humidity, hence two weather sources:
# Open-Meteo (model data) and a real station in Sint-Niklaas as a cross-check.
OPEN_METEO_LAT = float(os.environ["OPEN_METEO_LAT"])
OPEN_METEO_LON = float(os.environ["OPEN_METEO_LON"])
OUTDOOR_WEATHER_INTERVAL = int(os.environ.get("OUTDOOR_WEATHER_INTERVAL", "900"))  # 15 min

# MeteoBelgie has no API, so this scrapes an ordinary web page: once an hour,
# with an honest User-Agent, on a path robots.txt allows.
METEOBELGIE_URL = "https://www.meteobelgie.be/waarnemingen/belgie/weerstation/262/sint-niklaas-provincie-oost-vlaanderen-21m"
METEOBELGIE_USER_AGENT = "OstdonkIntelligence/1.0 (personal project, non-commercial)"
METEOBELGIE_INTERVAL = int(os.environ.get("METEOBELGIE_INTERVAL", "3600"))  # 1 hour
METEOBELGIE_TZ = ZoneInfo("Europe/Brussels")  # the page shows local time


# --- Generic flatten pipeline ---

def _is_entity_map(d: dict) -> bool:
    """A dict whose keys are all numeric strings is a collection of entities
    keyed by ID (like the NIBE point list), not a record with named fields."""
    return bool(d) and all(k.isdigit() for k in d)


class ForceFieldStr(str):
    """A string leaf that to_points() writes as a field instead of a tag.

    For categorical values that change between cycles (Huawei's
    device_status: "On-grid" vs "Standby"). As a tag, every new value would
    start a new series, and last() would return one row per series instead
    of one value.
    """


def to_points(measurement: str, obj, ts: datetime, tags: dict[str, str] | None = None) -> list[Point]:
    """Converts arbitrary nested JSON into InfluxDB Points without knowing
    any field names. The rules look only at structure:

    - dict with only numeric keys -> one Point per entry, tagged id=<key>
    - list -> one Point per element, tagged index=<position>
    - other dict -> flattened into the current Point; nested dicts become
      path-joined field names, nested collections become sub-measurements
      named "<measurement>_<path>"
    - str -> tag, bool/int/float -> field, None or "" -> ignored
    """
    tags = dict(tags or {})
    point = Point(measurement).time(ts)
    for k, v in tags.items():
        point = point.tag(k, v)
    has_field = False
    leaf_tags: dict[str, str] = {}
    sub_points: list[Point] = []

    def walk(node, path: tuple[str, ...]) -> None:
        nonlocal point, has_field
        if isinstance(node, dict) and not _is_entity_map(node):
            for k, v in node.items():
                walk(v, path + (str(k),))
        elif isinstance(node, (dict, list)):
            sub_measurement = "_".join((measurement, *path)).lower()
            entries = node.items() if isinstance(node, dict) else enumerate(node)
            tag = "id" if isinstance(node, dict) else "index"
            for key, val in entries:
                sub_points.extend(to_points(sub_measurement, val, ts, {**tags, tag: str(key)}))
        elif node is None or node == "":
            return
        else:
            name = "_".join(path).lower()
            if not name:
                return
            if isinstance(node, bool):
                point = point.field(name, node)
                has_field = True
            elif isinstance(node, (int, float)):
                # Always float: a JSON number can flip between 0 and 0.02 from
                # one poll to the next, and InfluxDB rejects a field type change.
                point = point.field(name, float(node))
                has_field = True
            elif isinstance(node, ForceFieldStr):
                point = point.field(name, str(node))
                has_field = True
            elif isinstance(node, str):
                point = point.tag(name, node)
                leaf_tags[name] = node

    walk(obj, ())
    if not has_field and leaf_tags:
        # InfluxDB needs at least one field per point. An entity with only
        # strings (a Duco subsystem with just Component/Status) would vanish,
        # so its strings are also written as fields.
        for k, v in leaf_tags.items():
            point = point.field(k, v)
        has_field = True

    return ([point] if has_field else []) + sub_points


# --- Source-specific steps before the generic pipeline ---

def _decorate_nibe_point(raw: dict) -> dict:
    """Adds 'scaled_value' and 'is_valid' next to the raw fields of one NIBE
    point. -32768, isOk and the divisor are one convention for every NIBE
    point, so this is the only NIBE-specific step."""
    value_block = raw.get("value") or {}
    raw_int = value_block.get("integerValue")
    decorated = dict(raw)
    decorated["is_valid"] = bool(value_block.get("isOk")) and raw_int is not None and raw_int != -32768
    if decorated["is_valid"]:
        divisor = (raw.get("metadata") or {}).get("divisor") or 1
        decorated["scaled_value"] = float(raw_int) / divisor
    return decorated


def _strip_smartctl_asctime(smart_raw: dict) -> dict:
    """Drops smartctl's `local_time.asctime`, a run-time string that would
    become a tag with a new value every cycle (see ForceFieldStr).
    `local_time.time_t` keeps the same information as a number."""
    if not isinstance(smart_raw, dict):
        return smart_raw
    local_time = smart_raw.get("local_time")
    if not isinstance(local_time, dict) or "asctime" not in local_time:
        return smart_raw
    stripped = dict(smart_raw)
    stripped["local_time"] = {k: v for k, v in local_time.items() if k != "asctime"}
    return stripped


def _clean_host_health(body: dict) -> dict:
    """health_endpoint.py can fail one section (one drive, the storage pools)
    without failing the request, and reports that as {"error": ...}. Those
    sections are dropped: an error is never written as data."""
    return {
        section: _strip_smartctl_asctime(data)
        for section, data in body.items()
        if not (isinstance(data, dict) and "error" in data)
    }


# Huawei status and bit-field registers that change value, so they are
# written as fields (see ForceFieldStr). Model, serial and firmware stay tags.
_HUAWEI_FORCE_FIELD_KEYS = frozenset({
    hrn.DEVICE_STATUS, hrn.STATE_1, hrn.STATE_2, hrn.STATE_3,
    hrn.ALARM_1, hrn.ALARM_2, hrn.ALARM_3,
    hrn.STARTUP_TIME, hrn.SHUTDOWN_TIME,
})


def _decorate_huawei_result(raw: dict[str, Any]) -> dict[str, Any]:
    """Converts the two value types to_points() can't handle: datetimes
    (startup/shutdown time) to ISO strings, and lists of flag names (state
    and alarm bit fields) to one "|"-joined string."""
    decorated: dict[str, Any] = {}
    for key, value in raw.items():
        if isinstance(value, datetime):
            value = value.isoformat()
        elif isinstance(value, list):
            value = "|".join(str(v) for v in value)
        if key in _HUAWEI_FORCE_FIELD_KEYS and isinstance(value, str):
            value = ForceFieldStr(value)
        decorated[key] = value
    return decorated


# --- Fetchers: each returns data ready for to_points(), or raises ---

def _get_json(url: str, timeout: float = 5, **kwargs):
    resp = requests.get(url, timeout=timeout, **kwargs)
    resp.raise_for_status()
    return resp.json()


def fetch_nibe() -> dict:
    # requests reads Basic auth from ~/.netrc. Self-signed cert -> verify=False.
    points = _get_json(f"https://{NIBE_HOST}:8443/api/v1/devices/0/points", timeout=10, verify=False)
    return {pid: _decorate_nibe_point(p) if isinstance(p, dict) else p for pid, p in points.items()}


def fetch_homewizard() -> dict:
    return _get_json(f"http://{HW_IP}/api/v1/data")


def fetch_duco_info() -> dict:
    return _get_json(f"http://{DUCO_IP}/v2/info")


def fetch_duco_nodes() -> list:
    return _get_json(f"http://{DUCO_IP}/v2/info/nodes").get("Nodes", [])


def fetch_host_health() -> dict:
    return _clean_host_health(_get_json(HEALTH_ENDPOINT_URL, timeout=10))


# Modbus has no "give me everything" call: batch_update() needs an explicit
# list of registers. PV string registers are added per installed string.
HUAWEI_REGISTER_NAMES: list[hrn.RegisterName] = [
    hrn.MODEL_NAME, hrn.SERIAL_NUMBER, hrn.PN, hrn.FIRMWARE_VERSION, hrn.SOFTWARE_VERSION, hrn.MODEL_ID,
    hrn.NB_PV_STRINGS, hrn.NB_MPP_TRACKS, hrn.NB_OPTIMIZERS, hrn.RATED_POWER,
    hrn.DEVICE_STATUS, hrn.STATE_1, hrn.STATE_2, hrn.STATE_3, hrn.ALARM_1, hrn.ALARM_2, hrn.ALARM_3,
    hrn.INPUT_POWER, hrn.ACTIVE_POWER, hrn.REACTIVE_POWER, hrn.POWER_FACTOR, hrn.GRID_FREQUENCY,
    hrn.EFFICIENCY, hrn.INTERNAL_TEMPERATURE, hrn.INSULATION_RESISTANCE,
    hrn.LINE_VOLTAGE_A_B, hrn.LINE_VOLTAGE_B_C, hrn.LINE_VOLTAGE_C_A,
    hrn.PHASE_A_CURRENT, hrn.PHASE_B_CURRENT, hrn.PHASE_C_CURRENT,
    hrn.DAY_ACTIVE_POWER_PEAK, hrn.ACCUMULATED_YIELD_ENERGY, hrn.TOTAL_DC_INPUT_POWER,
    hrn.HOURLY_YIELD_ENERGY, hrn.DAILY_YIELD_ENERGY, hrn.MONTHLY_YIELD_ENERGY, hrn.YEARLY_YIELD_ENERGY,
    hrn.STARTUP_TIME, hrn.SHUTDOWN_TIME,
    hrn.METER_STATUS, hrn.METER_TYPE, hrn.POWER_METER_ACTIVE_POWER, hrn.POWER_METER_REACTIVE_POWER,
]


async def _fetch_huawei_async() -> dict[str, Any]:
    """Connects, reads and disconnects every cycle: the dongle accepts only
    one Modbus client at a time, so holding the connection would lock out
    the FusionSolar app."""
    client = huawei.create_tcp_client(HUAWEI_DONGLE_IP, unit_id=HUAWEI_UNIT_ID, timeout=HUAWEI_TIMEOUT)
    await client.connect()
    try:
        device = await huawei.create_device_instance(client)
        pv_strings = range(1, (getattr(device, "pv_string_count", 0) or 0) + 1)
        register_names = HUAWEI_REGISTER_NAMES + [
            getattr(hrn, f"PV_{i:02d}_{quantity}") for i in pv_strings for quantity in ("VOLTAGE", "CURRENT")
        ]
        results = await device.batch_update(register_names)
        return {name: result.value for name, result in results.items()}
    finally:
        await client.disconnect()


def fetch_huawei() -> dict:
    # Hard deadline: the library retries a connection internally for up to
    # 60 s, which would stall the whole cycle on a flaky dongle. At night the
    # inverter is in standby and this raises, like any other failed source.
    raw = asyncio.run(asyncio.wait_for(_fetch_huawei_async(), timeout=HUAWEI_TIMEOUT + 5))
    return _decorate_huawei_result(raw)


# --- Weather: each fetcher returns a reading with its observation time ---

def parse_open_meteo(data: dict) -> dict | None:
    """Turns an Open-Meteo response into a reading stamped with current.time,
    the start of its 15-minute model interval."""
    current = data.get("current") or {}
    observed = current.get("time")
    reading = {"temp_c": current.get("temperature_2m"), "humidity_pct": current.get("relative_humidity_2m")}
    if not isinstance(observed, (int, float)) or all(v is None for v in reading.values()):
        log.error(f"Open-Meteo: no usable reading in response: {current!r}")
        return None
    return {"observed_at": datetime.fromtimestamp(observed, timezone.utc), **reading}


def fetch_outdoor_weather() -> dict | None:
    data = _get_json(
        "https://api.open-meteo.com/v1/forecast",
        timeout=10,
        params={
            "latitude": OPEN_METEO_LAT,
            "longitude": OPEN_METEO_LON,
            "current": "temperature_2m,relative_humidity_2m",
            "timeformat": "unixtime",  # UTC epoch instead of a zone-less ISO string
        },
    )
    return parse_open_meteo(data)


def parse_meteobelgie_observed_at(text: str) -> datetime | None:
    """Parses "Overzicht van 09-11-2026 om 20:11" (Belgian local time) to UTC.
    In the repeated hour when summer time ends, zoneinfo picks the first one."""
    match = re.search(r"Overzicht van (\d{2})-(\d{2})-(\d{4}) om (\d{2}):(\d{2})", text)
    if match is None:
        return None
    day, month, year, hour, minute = (int(g) for g in match.groups())
    try:
        local = datetime(year, month, day, hour, minute, tzinfo=METEOBELGIE_TZ)
    except ValueError:
        return None
    return local.astimezone(timezone.utc)


def parse_meteobelgie(html: str) -> dict | None:
    """Reads the overview block of the station page.

    Returns None with an ERROR when the expected structure is missing, so a
    site redesign reads as "the scraper broke", not as a network error. Also
    returns None when the station isn't online: its values are then stale.
    """
    section = BeautifulSoup(html, "html.parser").find("section", class_="weather-station")
    if section is None:
        log.error("MeteoBelgie: <section class='weather-station'> not found, the page has changed.")
        return None
    text = section.get_text(" ", strip=True)

    def number_after(label: str) -> float | None:
        match = re.search(rf"{label}\s*:\s*(-?\d+(?:\.\d+)?)", text)
        return float(match.group(1)) if match else None

    status = re.search(r"Status\s*:\s*(\S+)", text)
    reading = {
        "observed_at": parse_meteobelgie_observed_at(text),
        "temp_c": number_after("Temperatuur"),
        "humidity_pct": number_after("Vochtigheid"),
        "pressure_hpa": number_after("Druk"),
        "dewpoint_c": number_after("Dauwpunt"),
    }
    missing = [name for name, value in reading.items() if value is None] + ([] if status else ["status"])
    if missing:
        log.error(f"MeteoBelgie: could not parse {missing}, the page has probably changed.")
        return None
    if status.group(1) != "Online":
        log.warning(f"MeteoBelgie station reports '{status.group(1)}', not 'Online'; values not written.")
        return None
    return reading


def fetch_meteobelgie_station() -> dict | None:
    resp = requests.get(METEOBELGIE_URL, headers={"User-Agent": METEOBELGIE_USER_AGENT}, timeout=10)
    resp.raise_for_status()
    return parse_meteobelgie(resp.text)


def weather_point(measurement: str, reading: dict) -> Point:
    """One Point per reading, stamped with its observation time. The cached
    reading is written again every cycle; with the same timestamp, each
    rewrite overwrites the same point."""
    point = Point(measurement).time(reading["observed_at"])
    for name, value in reading.items():
        if name != "observed_at" and value is not None:
            point = point.field(name, float(value))
    return point


# --- Cycle ---

# Polled every cycle. The key is the measurement name.
LOCAL_SOURCES: dict[str, Callable[[], Any]] = {
    "nibe": fetch_nibe,
    "homewizard": fetch_homewizard,
    "duco_info": fetch_duco_info,
    "duco_nodes": fetch_duco_nodes,
    "host_health": fetch_host_health,
    "huawei": fetch_huawei,
}


def fetch_or_empty(name: str, fetch: Callable[[], Any]) -> Any:
    """One failing source never stops the others: its error is logged and
    the source contributes nothing this cycle."""
    try:
        return fetch()
    except Exception as e:
        log.error(f"{name} fetch failed: {e}")
        return {}


class SlowSource:
    """A source fetched at most once per `interval` seconds, keeping its last
    good reading in between and after a failed attempt. Every attempt counts,
    so a failing source is retried after the interval, not every cycle."""

    def __init__(self, fetch: Callable[[], dict | None], interval: float):
        self.fetch = fetch
        self.interval = interval
        self.reading: dict | None = None
        self.last_attempt: float | None = None

    def get(self, name: str, now: float) -> dict | None:
        if self.last_attempt is None or now - self.last_attempt >= self.interval:
            self.last_attempt = now
            self.reading = fetch_or_empty(name, self.fetch) or self.reading
        return self.reading


def build_points(local: dict[str, Any], weather: dict[str, dict | None], ts: datetime) -> list[Point]:
    """Device data through the generic pipeline on the cycle timestamp,
    weather readings on their own observation time."""
    points: list[Point] = []
    for name, data in local.items():
        if name == "host_health":
            # Each section becomes its own measurement ("sensors",
            # "smart_nvme"), not "host_health_sensors".
            for section, section_data in data.items():
                points += to_points(section, section_data, ts)
        else:
            points += to_points(name, data, ts)
    points += [weather_point(name, reading) for name, reading in weather.items() if reading]
    return points


def next_cycle_delay(scheduled: float, interval: float, now: float) -> tuple[float, float]:
    """Returns the next cycle's start and how long to sleep until then.
    Cycles start `interval` apart, however long the fetches took. A cycle
    that overran starts the next one now, without catching up."""
    upcoming = scheduled + interval
    if upcoming <= now:
        return now, 0.0
    return upcoming, upcoming - now


def main():
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    log.info(f"Collector started, interval={INTERVAL}s, bucket={INFLUX_BUCKET}")

    # Keyed by measurement name, like LOCAL_SOURCES.
    weather_sources = {
        "outdoor_weather": SlowSource(fetch_outdoor_weather, OUTDOOR_WEATHER_INTERVAL),
        "meteobelgie_sint_niklaas": SlowSource(fetch_meteobelgie_station, METEOBELGIE_INTERVAL),
    }
    next_cycle = time.monotonic()

    while True:
        ts = datetime.now(timezone.utc)
        local = {name: fetch_or_empty(name, fetch) for name, fetch in LOCAL_SOURCES.items()}
        now = time.monotonic()
        weather = {name: source.get(name, now) for name, source in weather_sources.items()}

        if not any(local.values()) and not any(weather.values()):
            log.warning("All sources failed this cycle, skipping write")
        else:
            try:
                points = build_points(local, weather, ts)
                write_api.write(bucket=INFLUX_BUCKET, record=points)
                log.info(f"{len(points)} points written ({ts.isoformat()})")
            except Exception as e:
                log.error(f"Processing/writing failed: {e}")

        next_cycle, delay = next_cycle_delay(next_cycle, INTERVAL, time.monotonic())
        if delay > 0:
            time.sleep(delay)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Collector stopped by user")
        sys.exit(0)

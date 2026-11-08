#!/usr/bin/env python3
"""
Polls a NIBE heat pump, a HomeWizard P1 meter, a Duco ventilation unit, a
Huawei SUN2000 inverter and a host-health endpoint on a fixed interval, plus
Open-Meteo and a MeteoBelgie weather-station page on their own, much longer
intervals. Everything is written to InfluxDB with one shared timestamp per
cycle, so the sources line up exactly for later analysis.

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
from typing import Any

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
# Unit ID 0 (the SDongleA itself) does not answer on this setup; unit ID 1
# (the SUN2000 inverter behind it) does. Hence a default of 1 instead of the
# library default of 0. Configurable for other installations (for example
# several inverters behind one dongle).
HUAWEI_UNIT_ID = int(os.environ.get("HUAWEI_UNIT_ID", "1"))
HUAWEI_TIMEOUT = int(os.environ.get("HUAWEI_TIMEOUT", "10"))  # seconds, per Modbus call
INFLUX_URL = os.environ.get("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.environ["INFLUX_COLLECTOR_TOKEN"]  # a write-only token is enough
INFLUX_ORG = os.environ["INFLUX_ORG"]
INFLUX_BUCKET = os.environ["INFLUX_BUCKET"]
INTERVAL = int(os.environ.get("COLLECTOR_INTERVAL", "60"))

# Outdoor humidity: none of the local devices measures it (the NIBE only
# reports outdoor temperature; Duco's weather registers need an accessory
# that isn't installed), hence Open-Meteo as an external source.
OPEN_METEO_LAT = float(os.environ["OPEN_METEO_LAT"])
OPEN_METEO_LON = float(os.environ["OPEN_METEO_LON"])
OUTDOOR_WEATHER_INTERVAL = int(os.environ.get("OUTDOOR_WEATHER_INTERVAL", "900"))  # 15 min

# MeteoBelgie: a real weather station (Davis Vantage Pro II) in Sint-Niklaas,
# used as a cross-check against Open-Meteo's model data. This is not an API
# but an ordinary web page, scraped because no public structured API exists.
# Hence stricter manners than for the other sources: a very low poll rate
# (default once an hour), an honest User-Agent (no browser impersonation),
# and robots.txt checked (only a ?list=*&key=* query pattern is disallowed,
# not this path).
METEOBELGIE_URL = "https://www.meteobelgie.be/waarnemingen/belgie/weerstation/262/sint-niklaas-provincie-oost-vlaanderen-21m"
METEOBELGIE_USER_AGENT = "OstdonkIntelligence/1.0 (personal project, non-commercial)"
METEOBELGIE_INTERVAL = int(os.environ.get("METEOBELGIE_INTERVAL", "3600"))  # 1 hour


# --- Generic flatten pipeline ---

def _is_entity_map(d: dict) -> bool:
    """Structural heuristic: a dict whose keys are ALL numeric strings is an
    ID-keyed collection of entities (like the NIBE point list, keyed by point
    ID) rather than a fixed record with named fields. Looks only at the shape
    of the keys, never at their names or values.
    """
    return bool(d) and all(k.isdigit() for k in d)


class ForceFieldStr(str):
    """Marks a string leaf that must be written as an InfluxDB *field*, never
    as a tag. An explicit, documented exception to the generic pipeline.

    Why: to_points() turns every string leaf into a tag. That is right for
    stable identity (serial numbers, model names) but harmful for a
    categorical value that can change between cycles (for example Huawei's
    device_status: "On-grid" vs "Standby"). A tag that changes value splits
    the InfluxDB series into a separate Flux table per write, so last() no
    longer returns one value but one row per table. The same problem as the
    smartctl local_time.asctime bug (see _strip_smartctl_asctime()).

    Usage: wrap the value before to_points(), in the source-specific
    decoration step (see _decorate_huawei_result()). The class itself is
    source-independent; which fields qualify stays a per-source decision.
    """


def to_points(measurement: str, obj, ts: datetime, tags: dict[str, str] | None = None) -> list[Point]:
    """Recursively converts an arbitrary nested JSON structure into InfluxDB
    Points, without knowing any specific field names.

    Rules (structural, not name-based):
    - dict with only numeric-string keys -> ID-keyed entity collection:
      every key/value becomes its own Point, tagged id=<key>.
    - list -> entity collection: every element becomes its own Point, tagged
      index=<position>; the measurement becomes "<measurement>_<path>".
    - any other dict -> fixed record: keys are flattened into the current
      Point (nested dicts -> joined path name, nested lists -> sub-Points as
      above).
    - scalar: str -> tag, bool/int/float -> field, None -> ignored.

    A Point without fields (tags only) is not written: InfluxDB needs at
    least one field per point.
    """
    tags = dict(tags or {})

    if isinstance(obj, dict) and _is_entity_map(obj):
        return [
            p
            for key, val in obj.items()
            for p in to_points(measurement, val, ts, {**tags, "id": key})
        ]

    if isinstance(obj, list):
        return [
            p
            for i, val in enumerate(obj)
            for p in to_points(measurement, val, ts, {**tags, "index": str(i)})
        ]

    point = Point(measurement).time(ts)
    for k, v in tags.items():
        point = point.tag(k, v)
    has_field = False
    leaf_tags: dict[str, str] = {}
    sub_points: list[Point] = []

    def walk(node, path: tuple[str, ...]) -> None:
        nonlocal point, has_field
        if isinstance(node, dict):
            if _is_entity_map(node):
                sub_measurement = "_".join((measurement, *path)).lower()
                for key, val in node.items():
                    sub_points.extend(to_points(sub_measurement, val, ts, {**tags, "id": key}))
                return
            for k, v in node.items():
                walk(v, path + (str(k),))
        elif isinstance(node, list):
            sub_measurement = "_".join((measurement, *path)).lower()
            for i, val in enumerate(node):
                sub_points.extend(to_points(sub_measurement, val, ts, {**tags, "index": str(i)}))
        elif node is None:
            return
        else:
            name = "_".join(path).lower()
            if not name:
                return
            if isinstance(node, bool):
                point = point.field(name, node)
                has_field = True
            elif isinstance(node, (int, float)):
                # Always coerce to float: a JSON number can flip between int
                # (e.g. "0") and float (e.g. "0.02") from one poll to the
                # next, as HomeWizard's current fields do, and InfluxDB does
                # not allow a type change on a field within a measurement.
                point = point.field(name, float(node))
                has_field = True
            elif isinstance(node, ForceFieldStr):
                # Must come before the generic str branch (ForceFieldStr is a
                # str subclass). An empty ForceFieldStr is ignored like an
                # empty plain string, so an empty bit field at night (e.g.
                # Huawei's alarm_1) doesn't fail the cycle.
                if node != "":
                    point = point.field(name, str(node))
                    has_field = True
            elif isinstance(node, str) and node != "":
                point = point.tag(name, node)
                leaf_tags[name] = node

    walk(obj, ())
    if not has_field and leaf_tags:
        # InfluxDB doesn't accept a Point without fields. An entity with only
        # categorical (string) data, such as a Duco subsystem with just
        # Component/Status, would otherwise disappear silently. The same
        # string values are therefore also written as string fields.
        for k, v in leaf_tags.items():
            point = point.field(k, v)
        has_field = True

    return ([point] if has_field else []) + sub_points


def _decorate_nibe_point(raw: dict) -> dict:
    """Adds a derived, aggregation-safe 'scaled_value' and 'is_valid' next to
    the raw fields of one NIBE point.

    The only NIBE-specific step in the otherwise generic pipeline:
    -32768/isOk/divisor are a protocol-wide sentinel convention that is the
    same for every NIBE point, not a per-point or per-name choice. The raw
    integerValue/isOk still pass through the generic flatten, so nothing is
    lost.
    """
    metadata = raw.get("metadata") or {}
    value_block = raw.get("value") or {}
    raw_int = value_block.get("integerValue")
    is_ok = bool(value_block.get("isOk"))

    decorated = dict(raw)
    if is_ok and raw_int is not None and raw_int != -32768:
        divisor = metadata.get("divisor") or 1
        decorated["is_valid"] = True
        decorated["scaled_value"] = raw_int / divisor if divisor != 1 else float(raw_int)
    else:
        decorated["is_valid"] = False
    return decorated


def _strip_smartctl_asctime(smart_raw: dict) -> dict:
    """Removes smartctl's `local_time.asctime` (a human-readable timestamp
    string, e.g. "Sat Aug 15 12:30:57 2026 CEST") before to_points(), so it
    doesn't end up as a tag.

    The string changes on every collector cycle (it's smartctl's own run
    time, not a measurement), and the generic rule turns every string leaf
    into a tag. A tag with a new value on nearly every write splits the
    series into as many Flux tables as there are writes: a `last()` query
    then returns one row per table instead of one value (328 rows over 6
    hours in practice), which Grafana renders as a grid of separate tiles.
    Nothing is lost: `local_time.time_t` (numeric epoch) stays as a field,
    and every point has its own InfluxDB `_time` anyway."""
    if not isinstance(smart_raw, dict):
        return smart_raw
    local_time = smart_raw.get("local_time")
    if not isinstance(local_time, dict) or "asctime" not in local_time:
        return smart_raw
    stripped = dict(smart_raw)
    stripped["local_time"] = {k: v for k, v in local_time.items() if k != "asctime"}
    return stripped


# --- Fetchers: each returns the raw, unfiltered API response ---

def fetch_nibe() -> dict:
    """Fetches the complete, raw NIBE point list (device 0).

    Auth via ~/.netrc (machine = NIBE_HOST): requests picks this up
    automatically as long as no explicit auth= is passed (trust_env=True is
    the default). Self-signed NIBE certificate -> verify=False.
    """
    url = f"https://{NIBE_HOST}:8443/api/v1/devices/0/points"
    resp = requests.get(url, verify=False, timeout=10)
    resp.raise_for_status()
    return resp.json()


def fetch_homewizard() -> dict:
    """Fetches the raw HomeWizard P1 data (no auth)."""
    resp = requests.get(f"http://{HW_IP}/api/v1/data", timeout=5)
    resp.raise_for_status()
    return resp.json()


def fetch_duco_info() -> dict:
    """Fetches the raw system-wide Duco status (no auth)."""
    resp = requests.get(f"http://{DUCO_IP}/v2/info", timeout=5)
    resp.raise_for_status()
    return resp.json()


def fetch_duco_nodes() -> list:
    """Fetches the raw per-node Duco sensor data (no auth).

    Only unwraps the fixed envelope key 'Nodes', which is the endpoint's
    top-level response shape (API contract), not a selection of fields.
    """
    resp = requests.get(f"http://{DUCO_IP}/v2/info/nodes", timeout=5)
    resp.raise_for_status()
    return resp.json().get("Nodes", [])


def fetch_host_health() -> dict:
    """Fetches hardware health (lm-sensors, SMART, storage pools) from
    health_endpoint.py, which runs on the bare Proxmox host, not in a
    container."""
    resp = requests.get(HEALTH_ENDPOINT_URL, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _huawei_pv_string_registers(pv_string_count: int) -> list[hrn.RegisterName]:
    """Builds the PV string register list from the number of strings actually
    installed (device.pv_string_count, read by the library), not hardcoded to
    this unit's 2 strings, so the same code works on a SUN2000 model with more
    or fewer strings. Builds the names itself (PV_01_VOLTAGE/CURRENT, ...)
    instead of relying on the library's internal, undocumented
    device._pv_registers attribute.
    """
    names: list[hrn.RegisterName] = []
    for idx in range(1, pv_string_count + 1):
        names.append(getattr(hrn, f"PV_{idx:02d}_VOLTAGE"))
        names.append(getattr(hrn, f"PV_{idx:02d}_CURRENT"))
    return names


# Choosing WHICH registers to request is unavoidable here: huawei-solar has
# no "give me everything" call like the JSON APIs of NIBE, HomeWizard and
# Duco; batch_update() needs an explicit list of register names. This is the
# Modbus equivalent of "which endpoint do we call", not a choice of which
# fields of a response to throw away.
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

# These nine registers can change value (status and bit-field flags,
# start/stop times), so _decorate_huawei_result() wraps them in ForceFieldStr
# to avoid series fragmentation (see ForceFieldStr). startup_time and
# shutdown_time only change once or twice a day, but "can change" is enough.
# model_name/serial_number/pn/firmware_version/software_version stay plain
# tags: they are stable device identity, with no risk of fragmentation.
_HUAWEI_FORCE_FIELD_KEYS = frozenset({
    hrn.DEVICE_STATUS, hrn.STATE_1, hrn.STATE_2, hrn.STATE_3,
    hrn.ALARM_1, hrn.ALARM_2, hrn.ALARM_3,
    hrn.STARTUP_TIME, hrn.SHUTDOWN_TIME,
})


def _decorate_huawei_result(raw: dict[str, Any]) -> dict[str, Any]:
    """Converts huawei-solar's result values to types to_points() can handle.

    Two cases the generic pipeline doesn't handle natively:
    - datetime (startup_time/shutdown_time) -> ISO 8601 string, as a
      ForceFieldStr (field, not tag).
    - list of strings (state_1/2/3, alarm_1/2/3: Modbus bit fields already
      translated to readable flag names by the library) -> joined into one
      "|"-separated ForceFieldStr, or None for an empty list (no alarms or
      flags: ignored, no empty string field).

    All other values (int/float/bool/str/None/IntEnum) pass through
    unchanged. An IntEnum (e.g. meter_status) is an int subtype and becomes a
    numeric field automatically.
    """
    decorated: dict[str, Any] = {}
    for key, value in raw.items():
        if isinstance(value, datetime):
            iso = value.isoformat()
            decorated[key] = ForceFieldStr(iso) if key in _HUAWEI_FORCE_FIELD_KEYS else iso
        elif isinstance(value, list):
            joined = "|".join(str(v) for v in value)
            if key in _HUAWEI_FORCE_FIELD_KEYS:
                decorated[key] = ForceFieldStr(joined) if joined else None
            else:
                decorated[key] = joined or None
        elif key in _HUAWEI_FORCE_FIELD_KEYS and isinstance(value, str):
            decorated[key] = ForceFieldStr(value) if value != "" else None
        else:
            decorated[key] = value
    return decorated


async def _fetch_huawei_async() -> dict[str, Any]:
    """One short-lived Modbus TCP connection: connect, read, disconnect.
    Never keeps a connection open between cycles: the SDongleA accepts only
    one Modbus TCP client at a time, so this minimises conflicts with, for
    example, the FusionSolar app. disconnect() runs in a finally block so a
    read that fails halfway doesn't leak the socket.
    """
    client = huawei.create_tcp_client(HUAWEI_DONGLE_IP, unit_id=HUAWEI_UNIT_ID, timeout=HUAWEI_TIMEOUT)
    await client.connect()
    try:
        device = await huawei.create_device_instance(client)
        register_names = list(HUAWEI_REGISTER_NAMES)
        pv_string_count = getattr(device, "pv_string_count", 0) or 0
        register_names.extend(_huawei_pv_string_registers(pv_string_count))
        results = await device.batch_update(register_names)
        return {name: result.value for name, result in results.items()}
    finally:
        await client.disconnect()


def fetch_huawei() -> dict:
    """Fetches the raw Huawei SUN2000 inverter data via the SDongleA (Modbus
    TCP, unit ID HUAWEI_UNIT_ID).

    No try/except of its own: like every other source, main()'s per-source
    try/except catches any failure and skips this source for the cycle. There
    is deliberately no distinction between "inverter in standby at night" and
    "dongle unreachable"; both are just an exception.

    The outer asyncio.wait_for() is a hard deadline: the library can retry a
    connection internally for up to 60 s, which would otherwise eat most of
    a COLLECTOR_INTERVAL cycle and stall the collector on a flaky dongle.
    """
    raw = asyncio.run(asyncio.wait_for(_fetch_huawei_async(), timeout=HUAWEI_TIMEOUT + 5))
    return _decorate_huawei_result(raw)


def fetch_outdoor_weather() -> dict:
    """Fetches current outdoor temperature and humidity from Open-Meteo (free,
    no API key). Goes over the public internet instead of the local network,
    hence the explicit timeout and a separate, much lower poll rate (see
    OUTDOOR_WEATHER_INTERVAL and main()).
    """
    resp = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": OPEN_METEO_LAT,
            "longitude": OPEN_METEO_LON,
            "current": "temperature_2m,relative_humidity_2m",
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def build_outdoor_point(outdoor_raw: dict, ts: datetime) -> Point | None:
    """Turns the Open-Meteo response into one 'outdoor_weather' Point with
    raw measurements (temp_c, humidity_pct). No derived vapour pressure here;
    that is calculated at query time in Flux (Magnus formula).

    Hand-curated instead of going through to_points(): the schema is small
    and fixed (two values), and 'temp_c'/'humidity_pct' are deliberately
    chosen field names.
    """
    current = outdoor_raw.get("current") or {}
    temp_c = current.get("temperature_2m")
    humidity_pct = current.get("relative_humidity_2m")
    if temp_c is None and humidity_pct is None:
        return None

    point = Point("outdoor_weather").time(ts)
    if temp_c is not None:
        point = point.field("temp_c", float(temp_c))
    if humidity_pct is not None:
        point = point.field("humidity_pct", float(humidity_pct))
    return point


def fetch_meteobelgie_station() -> dict | None:
    """Scrapes the "Overzicht" (overview) block of the MeteoBelgie weather
    station page for Sint-Niklaas (Davis Vantage Pro II). Not an API, an
    ordinary web page.

    DOM parsing with BeautifulSoup, deliberately not a regex over the raw
    HTML: only the loose text nodes without schema.org markup (Dauwpunt =
    dew point, Status) are parsed with a small regex on that single node.

    Returns None (with a clear ERROR log, never silently) as soon as the
    expected structure no longer matches. That has to read as "the scraper
    broke because the site changed", not be confused with an ordinary
    temporary network error (already handled and logged by the try/except in
    main()).

    Also returns None (with a WARNING) when the station doesn't report
    "Online": its values are then probably stale and are not written.
    """
    resp = requests.get(METEOBELGIE_URL, headers={"User-Agent": METEOBELGIE_USER_AGENT}, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    section = soup.find("section", class_="weather-station")
    if section is None:
        log.error(
            "MeteoBelgie: <section class='weather-station'> not found on the page -- "
            "the site structure has probably changed (redesign?). The parser in "
            "fetch_meteobelgie_station() needs updating. Cycle skipped."
        )
        return None

    def _itemprop_float(name: str) -> float | None:
        node = section.find(attrs={"itemprop": name})
        if node is None:
            return None
        try:
            # split(":", 1): split on the FIRST colon only. The value itself
            # has none, but windSpeed's text does (a time like "13:51"), so
            # split(":")[-1] would break there.
            return float(node.get_text(" ", strip=True).split(":", 1)[-1].strip())
        except (ValueError, IndexError):
            return None

    def _labeled_float(label: str) -> float | None:
        """For values without an itemprop (e.g. Dauwpunt): finds the text
        node containing the label and parses the first number after it."""
        text_node = section.find(string=re.compile(re.escape(label)))
        if text_node is None:
            return None
        match = re.search(rf"{re.escape(label)}\s*:\s*(-?\d+(?:\.\d+)?)", text_node)
        return float(match.group(1)) if match else None

    temp_c = _itemprop_float("temperature")
    humidity_pct = _itemprop_float("humidity")
    pressure_hpa = _itemprop_float("airPressure")
    dewpoint_c = _labeled_float("Dauwpunt")

    status_node = section.find(string=re.compile(r"Status\s*:"))
    status_match = re.search(r"Status\s*:\s*(\S+)", status_node) if status_node else None
    status = status_match.group(1).strip() if status_match else None

    wind_direction = None
    wind_node = section.find(attrs={"itemprop": "windDirection"})
    if wind_node is not None:
        wind_direction = wind_node.get_text(" ", strip=True).split(":", 1)[-1].strip()

    wind_speed_raw = None
    wind_speed_node = section.find(attrs={"itemprop": "windSpeed"})
    if wind_speed_node is not None:
        # split(":", 1): the value contains a time ("13:51"), so split only
        # on the first colon (the label/value separator).
        wind_speed_raw = wind_speed_node.get_text(" ", strip=True).split(":", 1)[-1].strip()

    missing = [
        name for name, val in (
            ("temperature", temp_c), ("humidity", humidity_pct),
            ("pressure", pressure_hpa), ("dew point", dewpoint_c), ("status", status),
        )
        if val is None
    ]
    if missing:
        log.error(
            f"MeteoBelgie: could not find or parse these fields in the overview block: {missing} -- "
            "the site structure has probably changed. The parser needs updating. "
            "Cycle skipped."
        )
        return None

    if status != "Online":
        log.warning(
            f"MeteoBelgie station reports status '{status}' (not 'Online') -- values are treated as "
            "stale and not written this cycle."
        )
        return None

    return {
        "temp_c": temp_c,
        "humidity_pct": humidity_pct,
        "pressure_hpa": pressure_hpa,
        "dewpoint_c": dewpoint_c,
        "wind_direction": wind_direction,
        "wind_speed_raw": wind_speed_raw,
    }


def build_meteobelgie_point(station_raw: dict, ts: datetime) -> Point:
    """Turns the MeteoBelgie scrape into one 'meteobelgie_sint_niklaas'
    Point. Hand-curated like Open-Meteo (not through to_points()): a small,
    fixed set of fields. Wind direction and speed are not written yet: their
    raw string form isn't unambiguous enough to model as typed fields.
    """
    return (
        Point("meteobelgie_sint_niklaas")
        .field("temp_c", float(station_raw["temp_c"]))
        .field("humidity_pct", float(station_raw["humidity_pct"]))
        .field("pressure_hpa", float(station_raw["pressure_hpa"]))
        .field("dewpoint_c", float(station_raw["dewpoint_c"]))
        .time(ts)
    )


def build_points(
    nibe_raw: dict,
    hw_raw: dict,
    duco_info_raw: dict,
    duco_nodes_raw: list,
    host_health_raw: dict,
    huawei_raw: dict,
    outdoor_raw: dict | None,
    meteobelgie_raw: dict | None,
    ts: datetime,
) -> list[Point]:
    """Converts the raw sources into InfluxDB Points through the generic
    flatten pipeline (to_points), plus the two separately handled weather
    Points (outdoor_weather, meteobelgie_sint_niklaas).
    """
    decorated_nibe = {
        point_id: _decorate_nibe_point(raw) if isinstance(raw, dict) else raw
        for point_id, raw in nibe_raw.items()
    }

    points: list[Point] = []
    points += to_points("nibe", decorated_nibe, ts)
    points += to_points("homewizard", hw_raw, ts)
    points += to_points("duco_info", duco_info_raw, ts)
    points += to_points("duco_nodes", duco_nodes_raw, ts)
    # health_endpoint.py's envelope is split into its sections before
    # flattening, not passed as a whole; otherwise to_points() would produce
    # measurement names like "host_health_sensors" instead of "sensors",
    # "smart_nvme" and "smart_nvme1". to_points() safely returns [] for
    # None/{} (e.g. after a failed fetch). health_endpoint.py can also fail a
    # single drive without failing the whole request, returning {"error": ...}
    # under that drive's key. That error must not be flattened as if it were
    # smartctl data, so it is skipped here: like the other sources, a failed
    # source is skipped, never written as error data.
    points += to_points("sensors", host_health_raw.get("sensors"), ts)
    for smart_key in ("smart_nvme", "smart_nvme1"):
        smart_data = host_health_raw.get(smart_key)
        if isinstance(smart_data, dict) and "error" in smart_data:
            continue
        points += to_points(smart_key, _strip_smartctl_asctime(smart_data), ts)

    # storage_pools is a list of pool dicts (pvesm status has no JSON output,
    # so health_endpoint.py parses it). to_points() splits the list into one
    # Point per pool (tag index=<position>), and because every pool dict
    # contains a "name" string, the same Point is also tagged name=<pool>,
    # without any change to to_points(). Same skip-on-error guard as the
    # SMART sources above.
    storage_pools = host_health_raw.get("storage_pools")
    if not (isinstance(storage_pools, dict) and "error" in storage_pools):
        points += to_points("storage_pools", storage_pools, ts)

    # huawei_raw has already been through _decorate_huawei_result() (in
    # fetch_huawei()), so only the generic flatten is needed here.
    # to_points() safely returns [] for an empty dict (failed fetch, e.g. an
    # unreachable dongle).
    points += to_points("huawei", huawei_raw, ts)

    if outdoor_raw is not None:
        outdoor_point = build_outdoor_point(outdoor_raw, ts)
        if outdoor_point is not None:
            points.append(outdoor_point)

    if meteobelgie_raw is not None:
        points.append(build_meteobelgie_point(meteobelgie_raw, ts))

    return points


def main():
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    log.info(f"Collector started, interval={INTERVAL}s, bucket={INFLUX_BUCKET}")

    outdoor_cache: dict | None = None
    last_outdoor_fetch = 0.0
    meteobelgie_cache: dict | None = None
    last_meteobelgie_fetch = 0.0

    while True:
        ts = datetime.now(timezone.utc)

        try:
            nibe_raw = fetch_nibe()
        except Exception as e:
            log.error(f"NIBE fetch failed: {e}")
            nibe_raw = {}

        try:
            hw_raw = fetch_homewizard()
        except Exception as e:
            log.error(f"HomeWizard fetch failed: {e}")
            hw_raw = {}

        try:
            duco_info_raw = fetch_duco_info()
        except Exception as e:
            log.error(f"Duco info fetch failed: {e}")
            duco_info_raw = {}

        try:
            duco_nodes_raw = fetch_duco_nodes()
        except Exception as e:
            log.error(f"Duco nodes fetch failed: {e}")
            duco_nodes_raw = []

        try:
            host_health_raw = fetch_host_health()
        except Exception as e:
            log.error(f"Host health fetch failed: {e}")
            host_health_raw = {}

        try:
            huawei_raw = fetch_huawei()
        except Exception as e:
            # No distinction between "inverter in standby at night" and
            # "dongle really unreachable": both are just an exception here,
            # like the other sources. An empty dict makes
            # to_points("huawei", {}, ts) in build_points() return [] for
            # this cycle without crashing the collector.
            log.error(f"Huawei fetch failed: {e}")
            huawei_raw = {}

        # Open-Meteo: its own, much lower poll rate than COLLECTOR_INTERVAL
        # (weather changes slowly, and polling a free public API every cycle
        # would be rude). In between, the last known value is reused so every
        # write cycle still gets an outdoor_weather point.
        now_monotonic = time.monotonic()
        if outdoor_cache is None or (now_monotonic - last_outdoor_fetch) >= OUTDOOR_WEATHER_INTERVAL:
            try:
                outdoor_cache = fetch_outdoor_weather()
                last_outdoor_fetch = now_monotonic
            except Exception as e:
                log.error(f"Outdoor weather fetch failed: {e}")
                # keep using the last known value (may still be None if the
                # very first fetch failed)

        # MeteoBelgie: even more careful than Open-Meteo, as this is a real
        # website not meant for machines. Its own, even lower poll rate
        # (default once an hour) and the same caching approach.
        if meteobelgie_cache is None or (now_monotonic - last_meteobelgie_fetch) >= METEOBELGIE_INTERVAL:
            try:
                meteobelgie_cache = fetch_meteobelgie_station()
                last_meteobelgie_fetch = now_monotonic
            except Exception as e:
                log.error(f"MeteoBelgie fetch failed: {e}")
                # keep using the last known value (may still be None if the
                # very first fetch failed, or if the station wasn't 'Online';
                # see fetch_meteobelgie_station())

        if (
            not nibe_raw and not hw_raw and not duco_info_raw and not duco_nodes_raw
            and not host_health_raw and not huawei_raw and not outdoor_cache and not meteobelgie_cache
        ):
            log.warning("All sources failed this cycle, skipping write")
        else:
            try:
                points = build_points(
                    nibe_raw, hw_raw, duco_info_raw, duco_nodes_raw, host_health_raw, huawei_raw,
                    outdoor_cache, meteobelgie_cache, ts,
                )
                write_api.write(bucket=INFLUX_BUCKET, record=points)
                log.info(f"{len(points)} points written ({ts.isoformat()})")
            except Exception as e:
                log.error(f"Processing/writing failed: {e}")

        time.sleep(INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Collector stopped by user")
        sys.exit(0)

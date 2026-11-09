"""The few source-specific steps around the generic pipeline, and
build_points() wiring all sources together."""

from datetime import datetime, timezone

import pytest
from huawei_solar import register_names as hrn

import collector
from collector import (
    ForceFieldStr,
    _clean_host_health,
    _decorate_huawei_result,
    _decorate_nibe_point,
    _strip_smartctl_asctime,
    build_points,
    parse_meteobelgie,
    parse_meteobelgie_observed_at,
    parse_open_meteo,
    to_points,
    weather_point,
)
from helpers import parse, parse_all

TS = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)
# Weather observations are older than the cycle that writes them.
OPEN_METEO_TIME = datetime(2026, 8, 4, 11, 45, tzinfo=timezone.utc)
STATION_TIME = datetime(2026, 8, 4, 11, 10, tzinfo=timezone.utc)


def stamp(point):
    """The point's timestamp, as a UTC datetime."""
    return datetime.fromtimestamp(int(point.to_line_protocol().rsplit(" ", 1)[1]) / 1e9, timezone.utc)


def nibe_point(integer_value, divisor=10, is_ok=True):
    return {"metadata": {"divisor": divisor}, "value": {"integerValue": integer_value, "isOk": is_ok}}


# --- NIBE: protocol-wide sentinel and divisor convention ---

def test_nibe_valid_value_is_scaled_by_divisor():
    decorated = _decorate_nibe_point(nibe_point(215))
    assert decorated["is_valid"] is True
    assert decorated["scaled_value"] == 21.5


def test_nibe_divisor_one_still_yields_float():
    scaled = _decorate_nibe_point(nibe_point(3, divisor=1))["scaled_value"]
    assert scaled == 3.0 and isinstance(scaled, float)


def test_nibe_sentinel_and_not_ok_are_invalid_without_scaled_value():
    for raw in (nibe_point(-32768), nibe_point(215, is_ok=False)):
        decorated = _decorate_nibe_point(raw)
        assert decorated["is_valid"] is False
        assert "scaled_value" not in decorated


def test_nibe_decoration_keeps_raw_fields_and_does_not_mutate_input():
    raw = nibe_point(215)
    decorated = _decorate_nibe_point(raw)
    assert decorated["value"] == raw["value"]
    assert "scaled_value" not in raw


def test_fetch_nibe_decorates_every_point(monkeypatch):
    monkeypatch.setattr(collector, "_get_json", lambda url, **kw: {"40004": nibe_point(215)})
    [point] = to_points("nibe", collector.fetch_nibe(), TS)
    _, tags, fields = parse(point)
    assert tags == {"id": "40004"}
    assert fields["scaled_value"] == 21.5 and fields["is_valid"] is True


# --- Host health: smartctl's per-run string, failed sections ---

def test_smartctl_asctime_is_removed_but_epoch_kept():
    raw = {"local_time": {"asctime": "Sat Aug 15 12:30:57 2026", "time_t": 1786789857}}
    stripped = _strip_smartctl_asctime(raw)
    assert stripped["local_time"] == {"time_t": 1786789857}
    assert "asctime" in raw["local_time"]  # input untouched


def test_smartctl_strip_passes_through_other_shapes():
    assert _strip_smartctl_asctime(None) is None
    assert _strip_smartctl_asctime({"temperature": 40}) == {"temperature": 40}


def test_host_health_drops_failed_sections_and_strips_asctime():
    body = {
        "sensors": {"cpu": {"temp": 45}},
        "smart_nvme": {"error": "smartctl failed"},
        "smart_nvme1": {"local_time": {"asctime": "Sat Aug 15", "time_t": 1}},
        "storage_pools": {"error": "pvesm failed"},
    }
    assert _clean_host_health(body) == {
        "sensors": {"cpu": {"temp": 45}},
        "smart_nvme1": {"local_time": {"time_t": 1}},
    }


# --- Huawei: types the generic pipeline cannot digest ---

def test_huawei_datetime_and_flag_lists_become_field_strings():
    raw = {
        hrn.STARTUP_TIME: datetime(2026, 8, 4, 6, 12, tzinfo=timezone.utc),
        hrn.STATE_1: ["Grid-connected", "MPPT"],
        hrn.ALARM_1: [],
        hrn.DEVICE_STATUS: "On-grid",
        hrn.MODEL_NAME: "SUN2000",
        hrn.ACTIVE_POWER: 4200,
    }
    decorated = _decorate_huawei_result(raw)
    assert isinstance(decorated[hrn.STARTUP_TIME], ForceFieldStr)
    assert decorated[hrn.STATE_1] == "Grid-connected|MPPT"
    assert isinstance(decorated[hrn.STATE_1], ForceFieldStr)
    assert isinstance(decorated[hrn.DEVICE_STATUS], ForceFieldStr)
    assert type(decorated[hrn.MODEL_NAME]) is str  # stable identity stays a tag
    assert decorated[hrn.ACTIVE_POWER] == 4200

    _, tags, fields = parse(to_points("huawei", decorated, TS)[0])
    assert hrn.ALARM_1 not in fields  # no alarms: nothing written
    assert tags == {hrn.MODEL_NAME: "SUN2000"}


# --- Open-Meteo ---

def open_meteo(temp=31.8, rh=50, time=OPEN_METEO_TIME.timestamp()):
    return {"current": {"time": time, "temperature_2m": temp, "relative_humidity_2m": rh}}


def test_open_meteo_reading_carries_its_own_time():
    assert parse_open_meteo(open_meteo()) == {"observed_at": OPEN_METEO_TIME, "temp_c": 31.8, "humidity_pct": 50}


@pytest.mark.parametrize("current", [
    {"time": OPEN_METEO_TIME.timestamp()},                 # no values
    {"temperature_2m": 20.0, "relative_humidity_2m": 85},  # no time
])
def test_open_meteo_without_values_or_time_is_no_reading(current):
    assert parse_open_meteo({"current": current}) is None


# --- MeteoBelgie ---

STATION_PAGE = """
<html><body><section class="weather-station" itemscope itemtype="https://schema.org/WeatherObservation">
  <h2>Station van Sint-Niklaas</h2>
  <p>Overzicht van 04-08-2026 om 13:10</p>
  <ul>
    <li itemprop="temperature">Temperatuur : 31.5 °C</li>
    <li itemprop="humidity">Vochtigheid : 46 %</li>
    <li itemprop="airPressure">Druk : 1012 hPa</li>
    <li>Dauwpunt : 18.5°C</li>
    <li itemprop="windSpeed">Snelheid : 0.0km/u (30.6km/u om 20:41)</li>
    <li>Status : {status}</li>
  </ul>
</section></body></html>
"""


def test_station_page_is_parsed_into_a_reading():
    assert parse_meteobelgie(STATION_PAGE.format(status="Online")) == {
        "observed_at": STATION_TIME, "temp_c": 31.5, "humidity_pct": 46.0,
        "pressure_hpa": 1012.0, "dewpoint_c": 18.5,
    }


def test_station_offline_or_changed_page_is_no_reading():
    assert parse_meteobelgie(STATION_PAGE.format(status="Offline")) is None
    assert parse_meteobelgie(STATION_PAGE.format(status="Online").replace("Druk", "Luchtdruk :x")) is None
    assert parse_meteobelgie("<html><body>redesign</body></html>") is None


def test_station_time_is_converted_from_winter_time():
    assert parse_meteobelgie_observed_at("Overzicht van 09-11-2026 om 20:11") == datetime(
        2026, 11, 9, 19, 11, tzinfo=timezone.utc
    )


def test_station_time_missing_or_invalid_is_none():
    assert parse_meteobelgie_observed_at("Overzicht van vandaag") is None
    assert parse_meteobelgie_observed_at("Overzicht van 31-02-2026 om 10:00") is None


def test_weather_point_carries_observation_time_and_skips_missing_values():
    point = weather_point("outdoor_weather", {"observed_at": OPEN_METEO_TIME, "temp_c": 20, "humidity_pct": None})
    assert parse(point) == ("outdoor_weather", {}, {"temp_c": 20.0})
    assert stamp(point) == OPEN_METEO_TIME


# --- build_points: all sources in one cycle ---

def sources(**overrides):
    local = dict(
        nibe={"40004": _decorate_nibe_point(nibe_point(215))},
        homewizard={"active_power_w": 350},
        duco_info={"General": {"Board": {"Uptime": 100}}},
        duco_nodes=[{"Ventilation": {"FlowLvlTgt": 100}}],
        host_health={
            "sensors": {"cpu": {"temp": 45}},
            "smart_nvme": {"temperature": {"current": 38}},
            "storage_pools": [{"name": "local", "used": 0.4}],
        },
        huawei={hrn.ACTIVE_POWER: 4200},
    )
    local.update(overrides)
    weather = {
        "outdoor_weather": parse_open_meteo(open_meteo(temp=20.0, rh=85)),
        "meteobelgie_sint_niklaas": {"observed_at": STATION_TIME, "temp_c": 20.4},
    }
    return local, weather


def measurements(points):
    return {m for m, _, _ in parse_all(points)}


def test_build_points_covers_every_source():
    assert measurements(build_points(*sources(), ts=TS)) == {
        "nibe", "homewizard", "duco_info", "duco_nodes", "sensors", "smart_nvme",
        "storage_pools", "huawei", "outdoor_weather", "meteobelgie_sint_niklaas",
    }


def test_build_points_skips_failed_sources():
    local, weather = sources(huawei={}, duco_nodes={})
    weather["outdoor_weather"] = None
    found = measurements(build_points(local, weather, TS))
    assert {"huawei", "duco_nodes", "outdoor_weather"}.isdisjoint(found)
    assert {"nibe", "meteobelgie_sint_niklaas"} <= found


def test_build_points_stamps_devices_with_cycle_time_and_weather_with_its_own():
    stamps = {parse(p)[0]: stamp(p) for p in build_points(*sources(), ts=TS)}
    assert stamps.pop("outdoor_weather") == OPEN_METEO_TIME
    assert stamps.pop("meteobelgie_sint_niklaas") == STATION_TIME
    assert set(stamps.values()) == {TS}

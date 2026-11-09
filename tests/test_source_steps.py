"""The few source-specific steps around the generic pipeline, and
build_points() wiring all sources together."""

from datetime import datetime, timezone

from huawei_solar import register_names as hrn

from collector import (
    ForceFieldStr,
    _decorate_huawei_result,
    _decorate_nibe_point,
    _strip_smartctl_asctime,
    build_meteobelgie_point,
    build_outdoor_point,
    build_points,
    parse_meteobelgie_observed_at,
)
from helpers import parse, parse_all

TS = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)
# Weather observations are older than the cycle that writes them.
OPEN_METEO_TIME = datetime(2026, 8, 4, 11, 45, tzinfo=timezone.utc)
STATION_TIME = datetime(2026, 8, 4, 11, 10, tzinfo=timezone.utc)


def open_meteo(temp=31.8, rh=50, time=OPEN_METEO_TIME.timestamp()):
    return {"current": {"time": time, "temperature_2m": temp, "relative_humidity_2m": rh}}


def station(**overrides):
    raw = {"temp_c": 20.4, "humidity_pct": 84.9, "pressure_hpa": 1012, "dewpoint_c": 17.8,
           "observed_at": STATION_TIME}
    raw.update(overrides)
    return raw


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
    assert _decorate_nibe_point(nibe_point(3, divisor=1))["scaled_value"] == 3.0


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


# --- smartctl: drop the per-run asctime string before it becomes a tag ---

def test_smartctl_asctime_is_removed_but_epoch_kept():
    raw = {"local_time": {"asctime": "Sat Aug 15 12:30:57 2026", "time_t": 1786789857}}
    stripped = _strip_smartctl_asctime(raw)
    assert stripped["local_time"] == {"time_t": 1786789857}
    assert "asctime" in raw["local_time"]  # input untouched


def test_smartctl_strip_passes_through_other_shapes():
    assert _strip_smartctl_asctime(None) is None
    assert _strip_smartctl_asctime({"temperature": 40}) == {"temperature": 40}


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
    assert decorated[hrn.ALARM_1] is None
    assert isinstance(decorated[hrn.DEVICE_STATUS], ForceFieldStr)
    assert type(decorated[hrn.MODEL_NAME]) is str  # stable identity stays a tag
    assert decorated[hrn.ACTIVE_POWER] == 4200


# --- Open-Meteo: hand-curated point ---

def test_outdoor_point_uses_chosen_field_names():
    point = build_outdoor_point(open_meteo())
    assert parse(point) == ("outdoor_weather", {}, {"temp_c": 31.8, "humidity_pct": 50.0})


def test_outdoor_point_carries_open_meteo_time_not_cycle_time():
    assert stamp(build_outdoor_point(open_meteo())) == OPEN_METEO_TIME


def test_outdoor_point_skipped_when_response_has_no_values():
    assert build_outdoor_point({"current": {"time": OPEN_METEO_TIME.timestamp()}}) is None


def test_outdoor_point_skipped_without_observation_time():
    assert build_outdoor_point({"current": {"temperature_2m": 20.0, "relative_humidity_2m": 85}}) is None


# --- MeteoBelgie: observation time from the page header ---

def test_station_time_is_converted_from_summer_time():
    text = "Station van Sint-Niklaas Overzicht van 04-08-2026 om 13:10 Temperatuur : 31.5 °C"
    assert parse_meteobelgie_observed_at(text) == STATION_TIME


def test_station_time_is_converted_from_winter_time():
    text = "Overzicht van 09-11-2026 om 20:11"
    assert parse_meteobelgie_observed_at(text) == datetime(2026, 11, 9, 19, 11, tzinfo=timezone.utc)


def test_station_time_missing_or_invalid_is_none():
    assert parse_meteobelgie_observed_at("Overzicht van vandaag") is None
    assert parse_meteobelgie_observed_at("Overzicht van 31-02-2026 om 10:00") is None


def test_station_point_carries_observation_time():
    assert stamp(build_meteobelgie_point(station())) == STATION_TIME


# --- build_points: all sources in one cycle ---

def sources(**overrides):
    base = dict(
        nibe_raw={"40004": nibe_point(215)},
        hw_raw={"active_power_w": 350},
        duco_info_raw={"General": {"Board": {"Uptime": 100}}},
        duco_nodes_raw=[{"Ventilation": {"FlowLvlTgt": 100}}],
        host_health_raw={
            "sensors": {"cpu": {"temp": 45}},
            "smart_nvme": {"temperature": {"current": 38}},
            "smart_nvme1": {"temperature": {"current": 36}},
            "storage_pools": [{"name": "local", "used": 0.4}],
        },
        huawei_raw={hrn.ACTIVE_POWER: 4200},
        outdoor_raw=open_meteo(temp=20.0, rh=85),
        meteobelgie_raw=station(),
    )
    base.update(overrides)
    return base


def measurements(points):
    return {m for m, _, _ in parse_all(points)}


def test_build_points_covers_every_source():
    assert measurements(build_points(**sources(), ts=TS)) == {
        "nibe", "homewizard", "duco_info", "duco_nodes", "sensors",
        "smart_nvme", "smart_nvme1", "storage_pools", "huawei",
        "outdoor_weather", "meteobelgie_sint_niklaas",
    }


def test_build_points_skips_failed_sources_instead_of_writing_errors():
    host = {
        "sensors": {"cpu": {"temp": 45}},
        "smart_nvme": {"error": "smartctl failed"},
        "smart_nvme1": {"temperature": {"current": 36}},
        "storage_pools": {"error": "pvesm failed"},
    }
    points = build_points(**sources(host_health_raw=host, huawei_raw={}, outdoor_raw=None), ts=TS)
    found = measurements(points)
    assert {"smart_nvme", "storage_pools", "huawei", "outdoor_weather"}.isdisjoint(found)
    assert {"sensors", "smart_nvme1", "nibe"} <= found


def test_build_points_writes_nibe_scaled_value():
    [nibe] = [p for p in parse_all(build_points(**sources(), ts=TS)) if p[0] == "nibe"]
    assert nibe[1] == {"id": "40004"}
    assert nibe[2]["scaled_value"] == 21.5
    assert nibe[2]["is_valid"] is True


def test_build_points_stamps_devices_with_cycle_time_and_weather_with_its_own():
    stamps = {parse(p)[0]: stamp(p) for p in build_points(**sources(), ts=TS)}
    assert stamps.pop("outdoor_weather") == OPEN_METEO_TIME
    assert stamps.pop("meteobelgie_sint_niklaas") == STATION_TIME
    assert set(stamps.values()) == {TS}

"""The generic flatten pipeline: arbitrary device JSON in, InfluxDB points
out, without knowing any field name."""

from datetime import datetime, timezone

from collector import ForceFieldStr, to_points
from helpers import parse, parse_all

TS = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)


def test_scalars_map_to_fields_and_tags():
    [point] = to_points("dev", {"temp": 21, "on": True, "model": "X1", "unused": None}, TS)
    measurement, tags, fields = parse(point)
    assert measurement == "dev"
    assert tags == {"model": "X1"}
    assert fields == {"temp": 21.0, "on": True}


def test_numbers_are_always_floats():
    # A JSON number can flip between int and float from one poll to the next;
    # InfluxDB rejects a type change on an existing field.
    # In line protocol an integer field carries an "i" suffix; a float doesn't.
    as_int = to_points("dev", {"power": 0}, TS)[0].to_line_protocol()
    as_float = to_points("dev", {"power": 0.02}, TS)[0].to_line_protocol()
    assert "power=0 " in as_int and "power=0i" not in as_int
    assert "power=0.02 " in as_float


def test_nested_dicts_flatten_into_lowercase_paths():
    [point] = to_points("dev", {"Supply": {"Temp": 18.5, "Flow": {"Rate": 120}}}, TS)
    assert parse(point)[2] == {"supply_temp": 18.5, "supply_flow_rate": 120.0}


def test_numeric_keys_become_one_point_per_entity():
    obj = {"40004": {"value": 1}, "40005": {"value": 2}}
    assert parse_all(to_points("nibe", obj, TS)) == [
        ("nibe", {"id": "40004"}, {"value": 1.0}),
        ("nibe", {"id": "40005"}, {"value": 2.0}),
    ]


def test_top_level_list_becomes_indexed_points():
    assert parse_all(to_points("nodes", [{"rh": 60}, {"rh": 70}], TS)) == [
        ("nodes", {"index": "0"}, {"rh": 60.0}),
        ("nodes", {"index": "1"}, {"rh": 70.0}),
    ]


def test_nested_list_becomes_sub_measurement():
    obj = {"uptime": 5, "Pools": [{"used": 1.5}, {"used": 2.5}]}
    assert parse_all(to_points("host", obj, TS)) == [
        ("host", {}, {"uptime": 5.0}),
        ("host_pools", {"index": "0"}, {"used": 1.5}),
        ("host_pools", {"index": "1"}, {"used": 2.5}),
    ]


def test_parent_tags_propagate_to_sub_points():
    obj = {"7": {"name": "box", "sensors": [{"rh": 55}]}}
    sub = [p for p in parse_all(to_points("duco", obj, TS)) if p[0] == "duco_sensors"]
    assert sub == [("duco_sensors", {"id": "7", "index": "0"}, {"rh": 55.0})]


def test_string_only_entity_is_kept_as_string_fields():
    # InfluxDB needs at least one field per point; without this fallback an
    # entity with only categorical data would silently disappear.
    [point] = to_points("dev", {"component": "fan", "status": "ok"}, TS)
    _, tags, fields = parse(point)
    assert tags == {"component": "fan", "status": "ok"}
    assert fields == {"component": "fan", "status": "ok"}


def test_empty_input_and_empty_strings_produce_nothing():
    assert to_points("dev", {}, TS) == []
    assert to_points("dev", None, TS) == []
    assert to_points("dev", {"label": ""}, TS) == []


def test_force_field_str_is_written_as_field_not_tag():
    [point] = to_points("inv", {"power": 1.0, "device_status": ForceFieldStr("On-grid")}, TS)
    _, tags, fields = parse(point)
    assert "device_status" not in tags
    assert fields["device_status"] == "On-grid"


def test_empty_force_field_str_is_ignored():
    [point] = to_points("inv", {"power": 1.0, "alarm_1": ForceFieldStr("")}, TS)
    assert "alarm_1" not in parse(point)[2]


def test_every_point_carries_the_cycle_timestamp():
    points = to_points("dev", {"a": 1, "list": [{"b": 2}]}, TS)
    stamps = {p.to_line_protocol().rsplit(" ", 1)[1] for p in points}
    assert stamps == {str(int(TS.timestamp() * 1e9))}

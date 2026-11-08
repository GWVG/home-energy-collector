"""Turn InfluxDB Points into plain (measurement, tags, fields) tuples, so the
tests assert on meaning instead of on line-protocol formatting."""


def parse(point):
    line = point.to_line_protocol()
    head, fields_part, _ts = line.rsplit(" ", 2)
    measurement, *tag_parts = head.split(",")
    tags = dict(t.split("=", 1) for t in tag_parts)
    fields = {}
    for f in fields_part.split(","):
        key, raw = f.split("=", 1)
        if raw.startswith('"'):
            fields[key] = raw[1:-1]
        elif raw in ("true", "false"):
            fields[key] = raw == "true"
        else:
            fields[key] = float(raw.rstrip("i"))
    return measurement, tags, fields


def parse_all(points):
    return sorted((parse(p) for p in points), key=repr)

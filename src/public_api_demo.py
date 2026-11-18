"""Normalize and validate the two bounded public API responses."""
from datetime import datetime, timezone
from math import isfinite


def earthquake_rows(payload):
    features = payload["features"]
    if (payload["type"] != "FeatureCollection" or not 0 < len(features) < 1000
            or payload["metadata"]["count"] != len(features)):
        raise ValueError("USGS response is empty, at the query limit, or inconsistent")
    rows = []
    keys = set()
    for feature in features:
        key = feature["id"]
        props = feature["properties"]
        geometry = feature["geometry"]
        longitude, latitude, depth = map(float, geometry["coordinates"])
        magnitude = float(props["mag"])
        timestamp = datetime.fromtimestamp(props["time"] / 1000, tz=timezone.utc)
        if (not key or key in keys or geometry["type"] != "Point"
                or not -180 <= longitude <= 180 or not -90 <= latitude <= 90
                or not isfinite(depth) or not isfinite(magnitude) or magnitude < 6
                or (timestamp.year, timestamp.month) != (2024, 1)):
            raise ValueError("USGS row failed identity, date, or measurement validation")
        keys.add(key)
        rows.append((key, timestamp, timestamp.date(), magnitude, longitude, latitude, depth))
    return rows


def population_rows(payload):
    pagination, records = payload
    if (int(pagination["page"]) != 1 or int(pagination["pages"]) != 1
            or int(pagination["total"]) != 5 or len(records) != 5):
        raise ValueError("Expected one complete World Bank page with five years")
    rows = []
    years = set()
    for item in records:
        year = int(item["date"])
        value = item["value"]
        if (year not in range(2019, 2024) or year in years
                or item["countryiso3code"] != "IND" or item["indicator"]["id"] != "SP.POP.TOTL"
                or type(value) is not int or value <= 0):
            raise ValueError("World Bank row failed identity, year, or population validation")
        years.add(year)
        rows.append(("IND", year, value))
    return rows

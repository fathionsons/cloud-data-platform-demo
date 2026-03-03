from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pipeline.validators import normalize_open_meteo_payload


def test_normalize_open_meteo_payload() -> None:
    payload = {
        "latitude": 47.0,
        "longitude": 19.0,
        "timezone": "UTC",
        "hourly": {
            "time": ["2026-03-01T00:00", "2026-03-01T01:00"],
            "temperature_2m": [8.1, 7.8],
            "apparent_temperature": [7.5, 7.2],
            "precipitation": [0.0, 0.2],
            "wind_speed_10m": [12.0, 11.3],
        },
    }

    records = normalize_open_meteo_payload(
        payload,
        location_name="budapest",
        source_name="open_meteo",
        fetched_at=datetime(2026, 3, 1, 6, tzinfo=UTC),
    )

    assert len(records) == 2
    assert records[0].location_name == "budapest"
    assert records[0].source == "open_meteo"


def test_quality_checks_reject_duplicates() -> None:
    payload = {
        "latitude": 40.7,
        "longitude": -74.0,
        "timezone": "UTC",
        "hourly": {
            "time": ["2026-03-01T00:00", "2026-03-01T00:00"],
            "temperature_2m": [8.1, 7.8],
            "apparent_temperature": [7.5, 7.2],
            "precipitation": [0.0, 0.2],
            "wind_speed_10m": [12.0, 11.3],
        },
    }

    with pytest.raises(ValueError, match="Duplicate observation"):
        normalize_open_meteo_payload(
            payload,
            location_name="new_york",
            source_name="open_meteo",
            fetched_at=datetime(2026, 3, 1, 6, tzinfo=UTC),
        )

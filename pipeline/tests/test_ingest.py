from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from pipeline.config import Location
from pipeline.ingest import generate_synthetic_observations, write_local_bronze


def test_generate_synthetic_observations() -> None:
    rows = generate_synthetic_observations(
        location=Location(name="test_city", latitude=45.0, longitude=19.0),
        fetched_at=datetime(2026, 3, 1, 12, tzinfo=UTC),
    )
    assert len(rows) == 48
    assert rows[0].source == "synthetic_fallback"
    assert rows[0].location_name == "test_city"


def test_write_local_bronze(tmp_path: Path) -> None:
    rows = [
        {
            "source": "open_meteo",
            "fetched_at": "2026-03-01T12:00:00+00:00",
            "location_name": "test_city",
            "latitude": 45.0,
            "longitude": 19.0,
            "timezone": "UTC",
            "observation_time": "2026-03-01T10:00:00+00:00",
            "temperature_2m": 10.5,
            "apparent_temperature": 9.9,
            "precipitation": 0.0,
            "wind_speed_10m": 12.1,
        }
    ]
    path = write_local_bronze(
        local_dir=str(tmp_path),
        rows=rows,
        fetched_at=datetime(2026, 3, 1, 12, tzinfo=UTC),
    )
    output = Path(path)
    assert output.exists()
    with output.open("r", encoding="utf-8") as handle:
        first_line = handle.readline().strip()
    assert json.loads(first_line)["location_name"] == "test_city"

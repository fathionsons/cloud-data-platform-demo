from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field, ValidationError, model_validator


class HourlyData(BaseModel):
    time: list[str]
    temperature_2m: list[float]
    apparent_temperature: list[float]
    precipitation: list[float]
    wind_speed_10m: list[float]

    @model_validator(mode="after")
    def validate_aligned_lengths(self) -> HourlyData:
        lengths = {
            len(self.time),
            len(self.temperature_2m),
            len(self.apparent_temperature),
            len(self.precipitation),
            len(self.wind_speed_10m),
        }
        if len(lengths) != 1:
            raise ValueError("Open-Meteo hourly arrays are not aligned.")
        return self


class OpenMeteoResponse(BaseModel):
    latitude: float
    longitude: float
    timezone: str
    hourly: HourlyData


class NormalizedObservation(BaseModel):
    source: str
    fetched_at: datetime
    location_name: str
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    timezone: str
    observation_time: datetime
    temperature_2m: float = Field(ge=-90, le=60)
    apparent_temperature: float = Field(ge=-100, le=70)
    precipitation: float = Field(ge=0, le=500)
    wind_speed_10m: float = Field(ge=0, le=300)


def normalize_open_meteo_payload(
    payload: dict,
    *,
    location_name: str,
    source_name: str,
    fetched_at: datetime | None = None,
) -> list[NormalizedObservation]:
    try:
        parsed = OpenMeteoResponse.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(f"Schema validation failed for Open-Meteo response: {exc}") from exc

    fetched_at_utc = fetched_at or datetime.now(UTC)
    observations: list[NormalizedObservation] = []
    for idx, raw_time in enumerate(parsed.hourly.time):
        observation = NormalizedObservation(
            source=source_name,
            fetched_at=fetched_at_utc,
            location_name=location_name,
            latitude=parsed.latitude,
            longitude=parsed.longitude,
            timezone=parsed.timezone,
            observation_time=datetime.fromisoformat(raw_time).replace(tzinfo=UTC),
            temperature_2m=parsed.hourly.temperature_2m[idx],
            apparent_temperature=parsed.hourly.apparent_temperature[idx],
            precipitation=parsed.hourly.precipitation[idx],
            wind_speed_10m=parsed.hourly.wind_speed_10m[idx],
        )
        observations.append(observation)

    run_quality_checks(observations)
    return observations


def run_quality_checks(records: list[NormalizedObservation]) -> None:
    if not records:
        raise ValueError("No observations were produced by the ingestion step.")

    seen: set[tuple[str, datetime]] = set()
    for record in records:
        key = (record.location_name, record.observation_time)
        if key in seen:
            raise ValueError(
                "Duplicate observation detected for "
                f"{record.location_name} at {record.observation_time.isoformat()}."
            )
        seen.add(key)

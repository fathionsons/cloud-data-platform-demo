from __future__ import annotations

import json
import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Location:
    name: str
    latitude: float
    longitude: float


@dataclass(frozen=True)
class AppConfig:
    api_base_url: str = "https://api.open-meteo.com/v1/forecast"
    source_name: str = "open_meteo"
    request_timeout_seconds: int = 20
    max_retries: int = 3
    retry_backoff_seconds: int = 2
    past_days: int = 2
    forecast_days: int = 2
    bronze_local_dir: str = "data/bronze/open_meteo"
    aws_region: str = "us-east-1"
    s3_bucket: str = ""
    s3_prefix: str = "bronze/open_meteo"
    log_level: str = "INFO"
    locations: list[Location] = field(default_factory=list)

    @classmethod
    def from_env(cls) -> AppConfig:
        default_locations = [
            {"name": "new_york", "latitude": 40.7128, "longitude": -74.0060},
            {"name": "chicago", "latitude": 41.8781, "longitude": -87.6298},
            {"name": "seattle", "latitude": 47.6062, "longitude": -122.3321},
        ]

        raw_locations = os.getenv("LOCATIONS_JSON", json.dumps(default_locations))
        parsed_locations = json.loads(raw_locations)
        locations = [
            Location(
                name=item["name"],
                latitude=float(item["latitude"]),
                longitude=float(item["longitude"]),
            )
            for item in parsed_locations
        ]

        return cls(
            api_base_url=os.getenv("OPEN_METEO_API_URL", cls.api_base_url),
            source_name=os.getenv("SOURCE_NAME", cls.source_name),
            request_timeout_seconds=int(
                os.getenv("REQUEST_TIMEOUT_SECONDS", str(cls.request_timeout_seconds))
            ),
            max_retries=int(os.getenv("MAX_RETRIES", str(cls.max_retries))),
            retry_backoff_seconds=int(
                os.getenv("RETRY_BACKOFF_SECONDS", str(cls.retry_backoff_seconds))
            ),
            past_days=int(os.getenv("PAST_DAYS", str(cls.past_days))),
            forecast_days=int(os.getenv("FORECAST_DAYS", str(cls.forecast_days))),
            bronze_local_dir=os.getenv("BRONZE_LOCAL_DIR", cls.bronze_local_dir),
            aws_region=os.getenv("AWS_REGION", cls.aws_region),
            s3_bucket=os.getenv("S3_BUCKET", ""),
            s3_prefix=os.getenv("S3_PREFIX", cls.s3_prefix),
            log_level=os.getenv("LOG_LEVEL", cls.log_level).upper(),
            locations=locations,
        )

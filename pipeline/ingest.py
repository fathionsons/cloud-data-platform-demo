from __future__ import annotations

import argparse
import json
import logging
import math
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import requests

from pipeline.config import AppConfig, Location
from pipeline.s3_client import S3Client
from pipeline.validators import (
    NormalizedObservation,
    normalize_open_meteo_payload,
    run_quality_checks,
)


class JsonFormatter(logging.Formatter):
    _base_keys = {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        for key, value in record.__dict__.items():
            if key not in self._base_keys and not key.startswith("_"):
                payload[key] = value
        return json.dumps(payload, default=str)


def configure_logger(level: str) -> logging.Logger:
    logger = logging.getLogger("pipeline.ingest")
    logger.setLevel(level)
    logger.handlers.clear()

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    return logger


def fetch_open_meteo_payload(
    *,
    session: requests.Session,
    config: AppConfig,
    location: Location,
    logger: logging.Logger,
) -> dict[str, Any]:
    params = {
        "latitude": location.latitude,
        "longitude": location.longitude,
        "hourly": "temperature_2m,apparent_temperature,precipitation,wind_speed_10m",
        "timezone": "UTC",
        "past_days": config.past_days,
        "forecast_days": config.forecast_days,
    }

    last_error: Exception | None = None
    for attempt in range(1, config.max_retries + 1):
        try:
            response = session.get(
                config.api_base_url,
                params=params,
                timeout=config.request_timeout_seconds,
            )
            response.raise_for_status()
            logger.info(
                "Open-Meteo fetch succeeded.",
                extra={
                    "event": "fetch_success",
                    "location": location.name,
                    "attempt": attempt,
                    "status_code": response.status_code,
                },
            )
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            logger.warning(
                "Open-Meteo fetch failed.",
                extra={
                    "event": "fetch_retry",
                    "location": location.name,
                    "attempt": attempt,
                    "error": str(exc),
                },
            )
            if attempt < config.max_retries:
                time.sleep(config.retry_backoff_seconds**attempt)

    raise RuntimeError(f"Failed to fetch data for {location.name}: {last_error}") from last_error


def generate_synthetic_observations(
    *,
    location: Location,
    fetched_at: datetime,
) -> list[NormalizedObservation]:
    start = fetched_at - timedelta(hours=23)
    rows: list[NormalizedObservation] = []
    for hour_idx in range(48):
        observation_time = start + timedelta(hours=hour_idx)
        seasonal = 8 * math.sin((hour_idx / 24) * 2 * math.pi)
        baseline = 13 + (location.latitude - 35) * 0.3
        temperature = baseline + seasonal
        precipitation = max(0.0, 2.2 * math.sin(hour_idx / 5))
        wind_speed = 12 + abs(4 * math.cos(hour_idx / 3))

        rows.append(
            NormalizedObservation(
                source="synthetic_fallback",
                fetched_at=fetched_at,
                location_name=location.name,
                latitude=location.latitude,
                longitude=location.longitude,
                timezone="UTC",
                observation_time=observation_time,
                temperature_2m=round(temperature, 2),
                apparent_temperature=round(temperature - 1.4, 2),
                precipitation=round(precipitation, 2),
                wind_speed_10m=round(wind_speed, 2),
            )
        )
    return rows


def write_local_bronze(*, local_dir: str, rows: list[dict[str, Any]], fetched_at: datetime) -> str:
    directory = Path(local_dir)
    directory.mkdir(parents=True, exist_ok=True)
    filename = f"open_meteo_{fetched_at.strftime('%Y%m%dT%H%M%SZ')}.ndjson"
    output_path = directory / filename
    with output_path.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(json.dumps(row, default=str) for row in rows))
        handle.write("\n")
    return str(output_path)


def write_aws_bronze(
    *,
    config: AppConfig,
    rows: list[dict[str, Any]],
    fetched_at: datetime,
    logger: logging.Logger,
) -> str:
    if not config.s3_bucket:
        raise ValueError("S3_BUCKET must be set for --mode aws.")

    key = (
        f"{config.s3_prefix}/date={fetched_at.strftime('%Y-%m-%d')}/"
        f"open_meteo_{fetched_at.strftime('%Y%m%dT%H%M%SZ')}.ndjson"
    )
    client = S3Client(region_name=config.aws_region)
    client.put_json_lines(bucket=config.s3_bucket, key=key, rows=rows)
    logger.info(
        "Uploaded bronze dataset to S3.",
        extra={
            "event": "s3_upload",
            "bucket": config.s3_bucket,
            "key": key,
            "row_count": len(rows),
        },
    )
    return key


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Open-Meteo data to bronze storage.")
    parser.add_argument("--mode", choices=["local", "aws"], default="local")
    parser.add_argument(
        "--force-fallback",
        action="store_true",
        help="Skip API calls and generate synthetic weather data.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = AppConfig.from_env()
    logger = configure_logger(config.log_level)
    fetched_at = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)

    logger.info(
        "Starting ingestion run.",
        extra={
            "event": "ingestion_start",
            "mode": args.mode,
            "locations": [location.name for location in config.locations],
        },
    )

    session = requests.Session()
    all_records: list[NormalizedObservation] = []

    for location in config.locations:
        if args.force_fallback:
            observations = generate_synthetic_observations(location=location, fetched_at=fetched_at)
            logger.info(
                "Synthetic fallback enabled by flag.",
                extra={
                    "event": "fallback_forced",
                    "location": location.name,
                    "rows": len(observations),
                },
            )
            all_records.extend(observations)
            continue

        try:
            payload = fetch_open_meteo_payload(
                session=session,
                config=config,
                location=location,
                logger=logger,
            )
            observations = normalize_open_meteo_payload(
                payload,
                location_name=location.name,
                source_name=config.source_name,
                fetched_at=fetched_at,
            )
            all_records.extend(observations)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Location fetch failed; using synthetic fallback.",
                extra={"event": "fallback_auto", "location": location.name, "error": str(exc)},
            )
            all_records.extend(
                generate_synthetic_observations(location=location, fetched_at=fetched_at)
            )

    run_quality_checks(all_records)
    rows = [record.model_dump(mode="json") for record in all_records]

    if args.mode == "local":
        output_path = write_local_bronze(
            local_dir=config.bronze_local_dir, rows=rows, fetched_at=fetched_at
        )
        logger.info(
            "Wrote bronze dataset locally.",
            extra={"event": "local_write", "path": output_path, "row_count": len(rows)},
        )
    else:
        output_key = write_aws_bronze(
            config=config, rows=rows, fetched_at=fetched_at, logger=logger
        )
        logger.info(
            "Wrote bronze dataset to AWS.",
            extra={"event": "aws_write", "key": output_key, "row_count": len(rows)},
        )

    logger.info(
        "Ingestion run finished.", extra={"event": "ingestion_done", "row_count": len(rows)}
    )


if __name__ == "__main__":
    main()

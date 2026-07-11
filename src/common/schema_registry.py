import os

import requests


DEFAULT_SCHEMA_REGISTRY_TIMEOUT = 10


def get_schema_registry_url() -> str:
    """
    Get Schema Registry URL from environment variables.
    """

    url = os.getenv(
        "SCHEMA_REGISTRY_URL"
    )

    if not url:
        raise RuntimeError(
            "SCHEMA_REGISTRY_URL is not configured. "
            "Please set SCHEMA_REGISTRY_URL environment variable."
        )

    return url.rstrip("/")


def get_schema_registry_timeout() -> int:
    """
    Get HTTP timeout for Schema Registry requests.
    """

    return int(
        os.getenv(
            "SCHEMA_REGISTRY_TIMEOUT",
            DEFAULT_SCHEMA_REGISTRY_TIMEOUT,
        )
    )


def get_latest_schema(subject: str) -> str:
    """
    Fetch latest Avro schema from Confluent Schema Registry.
    """

    url = (
        f"{get_schema_registry_url()}"
        f"/subjects/{subject}/versions/latest"
    )

    response = requests.get(
        url,
        timeout=get_schema_registry_timeout(),
    )

    response.raise_for_status()

    data = response.json()

    return data["schema"]


def get_latest_schema_with_id(subject: str):
    """
    Fetch latest Avro schema and schema id.
    """

    url = (
        f"{get_schema_registry_url()}"
        f"/subjects/{subject}/versions/latest"
    )

    response = requests.get(
        url,
        timeout=get_schema_registry_timeout(),
    )

    response.raise_for_status()

    data = response.json()

    return (
        data["schema"],
        data["id"],
    )
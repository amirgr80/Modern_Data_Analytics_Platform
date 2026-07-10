import json
import os
import urllib.error
import urllib.request
from typing import Optional


DEFAULT_SCHEMA_REGISTRY_URL = "http://185.255.90.14:8081"


def get_schema_registry_url() -> str:
    """
    Returns the Schema Registry URL.

    It first checks the SCHEMA_REGISTRY_URL environment variable.
    If it is not set, it uses the default project server URL.
    """
    return os.getenv("SCHEMA_REGISTRY_URL", DEFAULT_SCHEMA_REGISTRY_URL)


def get_subject_from_topic(topic: str) -> str:
    """
    Confluent Schema Registry usually stores Kafka value schemas
    with this naming pattern:

        <topic-name>-value

    Example:
        behavioral.events -> behavioral.events-value
    """
    return f"{topic}-value"


def get_latest_schema(
    subject: str,
    registry_url: Optional[str] = None,
) -> Optional[str]:
    """
    Fetches the latest Avro schema for a subject from Schema Registry.

    Example subject:
        behavioral.events-value
    """
    base_url = registry_url or get_schema_registry_url()
    url = f"{base_url}/subjects/{subject}/versions/latest"

    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            payload = response.read().decode("utf-8")
            data = json.loads(payload)
            return data.get("schema")

    except urllib.error.HTTPError as error:
        print(
            f"Schema Registry HTTP error. "
            f"subject={subject}, url={url}, error={error}"
        )
        return None

    except urllib.error.URLError as error:
        print(
            f"Schema Registry connection error. "
            f"subject={subject}, url={url}, error={error}"
        )
        return None

    except json.JSONDecodeError as error:
        print(
            f"Schema Registry returned invalid JSON. "
            f"subject={subject}, url={url}, error={error}"
        )
        return None
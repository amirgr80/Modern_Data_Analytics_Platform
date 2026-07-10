import requests


def get_schema(subject, registry_url):
    url = f"{registry_url}/subjects/{subject}/versions/latest"

    response = requests.get(url)

    response.raise_for_status()

    return response.json()["schema"]
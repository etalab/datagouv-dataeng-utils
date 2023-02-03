import requests
from typing import Optional


def send_message(
    endpoint_url: str,
    text: str,
    image_url: Optional[str] = None,
):
    data = {}
    data["text"] = text
    if image_url:
        data["attachments"] = [{"image_url": image_url}]

    r = requests.post(endpoint_url, json=data)
    assert r.status_code == 200

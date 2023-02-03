from typing import Union, List, Optional, TypedDict
import requests
import os

DATAGOUV_URL = "https://www.data.gouv.fr"

class File(TypedDict):
    dest_name: str
    dest_path: str


def create_dataset(
    api_key: str,
    payload: TypedDict
):
    headers = {
        "X-API-KEY": api_key,
    }
    r = requests.post(
        "{}/api/1/datasets/".format(DATAGOUV_URL),
        json=payload,
        headers=headers
    )
    return r.json()

def get_resource(
    resource_id: str,
    file_to_store: File,
):
    with requests.get(
        f"{DATAGOUV_URL}/fr/datasets/r/{resource_id}",
        stream=True
    ) as r:
        r.raise_for_status()
        os.makedirs(os.path.dirname(file_to_store["dest_path"]), exist_ok=True)
        with open(f"{file_to_store['dest_path']}{file_to_store['dest_name']}",'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return True


def post_resource(
    api_key: str,
    file_to_upload: File,
    dataset_id: str,
    resource_id: Optional[str] = None,
):
    headers = {
        "X-API-KEY": api_key,
    }
    files = {
        'file': open("{}{}".format(
            file_to_upload["dest_path"],
            file_to_upload["dest_name"]
        ), 'rb')
    }
    if resource_id:
        url = "{}/api/1/datasets/{}/resources/{}/upload/".format(
            DATAGOUV_URL,
            dataset_id,
            resource_id,
        )
    else:
        url = "{}/api/1/datasets/{}/upload/".format(
            DATAGOUV_URL,
            dataset_id,
        )
    r = requests.post(
        url,
        files=files,
        headers=headers
    )
    return r.json()


def delete_dataset_or_resource(
    api_key: str,
    dataset_id: str,
    resource_id: Optional[str] = None,
):
    headers = {
        "X-API-KEY": api_key,
    }
    if resource_id:
        url = "{}/api/1/datasets/{}/resources/{}/".format(
            DATAGOUV_URL,
            dataset_id,
            resource_id,
        )
    else:
        url = "{}/api/1/datasets/{}/".format(
            DATAGOUV_URL,
            dataset_id,
        )

    r = requests.delete(
        url,
        headers=headers
    )
    if r.status_code == 204:
        return {"message": "ok"}
    else:
        return r.json()


def get_dataset_or_resource_metadata(
    dataset_id: str,
    resource_id: Optional[str] = None,
):
    if resource_id:
        url = "{}/api/1/datasets/{}/resources/{}/".format(
            DATAGOUV_URL,
            dataset_id,
            resource_id
        )
        print(url)
    else:
        url = "{}/api/1/datasets/{}".format(
            DATAGOUV_URL,
            dataset_id,
        )
    r = requests.get(url)
    if r.status_code == 200:
        return r.json()
    else:
        return { "message": "error", "status": r.status_code}


def update_dataset_or_resource_metadata(
    api_key: str,
    payload: TypedDict,
    dataset_id: str,
    resource_id: Optional[str] = None,
):
    headers = {
        "X-API-KEY": api_key,
    }
    if resource_id:
        url = "{}/api/1/datasets/{}/resources/{}/".format(
            DATAGOUV_URL,
            dataset_id,
            resource_id,
        )
    else:
        url = "{}/api/1/datasets/{}/".format(
            DATAGOUV_URL,
            dataset_id,
        )

    r = requests.put(
        url,
        json=payload,
        headers=headers
    )
    return r.json()


def update_dataset_or_resource_extras(
    api_key: str,
    payload: TypedDict,
    dataset_id: str,
    resource_id: Optional[str] = None,
):
    headers = {
        "X-API-KEY": api_key,
    }
    if resource_id:
        url = "{}/api/2/datasets/{}/resources/{}/extras/".format(
            DATAGOUV_URL,
            dataset_id,
            resource_id,
        )
    else:
        url = "{}/api/2/datasets/{}/extras/".format(
            DATAGOUV_URL,
            dataset_id,
        )

    r = requests.put(
        url,
        json=payload,
        headers=headers
    )
    return r.json()


def delete_dataset_or_resource_extras(
    api_key: str,
    extras: List,
    dataset_id: str,
    resource_id: Optional[str] = None,
):
    headers = {
        "X-API-KEY": api_key,
    }
    if resource_id:
        url = "{}/api/2/datasets/{}/resources/{}/extras/".format(
            DATAGOUV_URL,
            dataset_id,
            resource_id,
        )
    else:
        url = "{}/api/2/datasets/{}/extras/".format(
            DATAGOUV_URL,
            dataset_id,
        )

    r = requests.delete(
        url,
        json=extras,
        headers=headers
    )
    if r.status_code == 204:
        return {"message": "ok"}
    else:
        return r.json()

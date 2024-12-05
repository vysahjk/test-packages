import os
import json
import requests


def create(data: dict):
    api_key = "e745d628-6657-4755-b2ac-a3f2663f15a6"
    url = os.environ.get(
        "API_URL", "https://kubernetes.cosmotech.com/cosmotech-api/ndon-trkl-dev/v4"
    )
    token = "qsdqsdqsdqsd"
    response = requests.post(
        url=f"{url}/organizations",
        headers={
            "Content-Type": "application/json",
            "X-CSM-API-KEY": f"{api_key}",
        },
        json=data,
    )
    print(response.json())
    return response.json()


def main():
    data = {
        "name": "my org toto",
        "security": {
            "default": "none",
            "accessControlList": [
                {"id": "nibaldo.donoso@cosmotech.com", "role": "admin"}
            ],
        }
    }
    print(data)
    create(data=data)


if __name__ == "__main__":
    main()

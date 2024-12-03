import sys
import requests
import os
from kubernetes import client, config, watch
from azure.identity import ClientSecretCredential
from kubernetes.client.rest import ApiException


def get_azure_token(scope: str = "default") -> str:
    """Returns an azure token"""
    credentials = ClientSecretCredential(
        client_id=os.environ.get("CLIENT_ID"),
        tenant_id=os.environ.get("TENANT_ID"),
        client_secret=os.environ.get("CLIENT_SECRET"),
    )
    scope = os.environ.get("API_SCOPE")
    try:
        token = credentials.get_token(scope)
        return token.token
    except Exception:
        pass


def get_by_id(org_id: str):
    token = get_azure_token()
    url = os.environ.get("API_URL")
    try:
        response = requests.get(
            url=f"{url}/organizations/{org_id}",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
        )
        if response is None:
            print("An error occurred while getting of all organisations")
        myobj = response.json()
        return myobj.get("id")
    except Exception as e:
        print(e)


def delete_obj(org_id: str):
    token = get_azure_token()
    url = os.environ.get("API_URL")
    response = requests.delete(
        url=f"{url}/organizations/{org_id}",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    if response is None:
        print("An error occurred while getting of all organisations")
    return response.json()


def update(org_id: str, data: dict):
    token = get_azure_token()
    url = os.environ.get("API_URL")
    response = requests.patch(
        url=f"{url}/organizations/{org_id}",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        json=data,
    )
    if response is None:
        print("An error occurred while getting of all organisations")
    return response.json()


def create(data: dict):
    token = get_azure_token()
    url = os.environ.get("API_URL")
    response = requests.post(
        url=f"{url}/organizations",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        json=data,
    )
    if response is None:
        print("An error occurred while getting of all organisations")
    return response.json()


def main():
    api_instance = client.CustomObjectsApi()
    group = "api.cosmotech.com"  # Update to the correct API group
    version = "v1"  # Update to the correct API version
    namespace = "cosmotech"  # Assuming custom resource is in default namespace
    plural = "organizations"

    # Watch for events on custom resource
    resource_version = ""
    while True:
        stream = watch.Watch().stream(
            api_instance.list_namespaced_custom_object,
            group,
            version,
            namespace,
            plural,
            resource_version=resource_version,
        )
        for event in stream:
            custom_resource = event["object"]
            event_type = event["type"]
            # Extract custom resource name
            resource_name = custom_resource["metadata"]["name"]
            # Extract key-value pairs from the custom resource spec
            resource_data = custom_resource.get("spec", {})
            # Handle events of type ADDED (resource created)
            if event_type == "ADDED":
                res_ = None
                if not resource_data.get("id"):
                    res_ = create(data=resource_data)
                    custom_resource["spec"]["id"] = res_.get("id")
                try:
                    api_instance.patch_namespaced_custom_object(
                        group,
                        version,
                        namespace,
                        plural,
                        resource_name,
                        custom_resource,
                    )
                except ApiException as e:
                    print("Exception when calling patch: %s\n" % e)
            # Handle events of type DELETED (resource deleted)
            elif event_type == "DELETED":
                if resource_data.get("id"):
                    delete_obj(org_id=resource_data.get("id"))
            elif event_type == "MODIFIED":
                if resource_data.get("id"):
                    update(org_id=resource_data.get("id"), data=resource_data)
            # Update resource_version to resume watching from the last event
            resource_version = custom_resource["metadata"]["resourceVersion"]


def check_env():
    for e in [
        "CLIENT_ID",
        "CLIENT_SECRET",
        "TENANT_ID",
        "AZURE_SUBSCRIPTION",
        "RESOURCE_GROUP_NAME",
        "LOCATION",
        "API_SCOPE",
        "PLATFORM_PRINCIPAL_ID",
        "ADX_CLUSTER_NAME",
    ]:
        if e not in os.environ:
            print(f"{e} is missing in triskell secret")
            sys.exit(1)


if __name__ == "__main__":
    check_env()
    config.load_incluster_config()  # Use in-cluster configuration
    main()

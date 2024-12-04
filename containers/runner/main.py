import sys
import requests
import os
from kubernetes import client, config, watch
from azure.identity import ClientSecretCredential
from kubernetes.client.rest import ApiException


def get_azure_token(scope: str = "default") -> str:
    """Returns an azure token basic"""
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


def get_by_id(org_id: str, work_id: str, runner_id: str):
    token = get_azure_token()
    url = os.environ.get("API_URL")
    try:
        response = requests.get(
            url=f"{url}/organizations/{org_id}/workspaces/{work_id}/runners/{runner_id}",
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


def delete_obj(org_id: str, work_id: str, runner_id: str):
    token = get_azure_token()
    url = os.environ.get("API_URL")
    try:
        response = requests.delete(
            url=f"{url}/organizations/{org_id}/workspaces/{work_id}/runners/{runner_id}",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
        )
        if response is None:
            print("An error occurred while getting of all organisations")
        return response.json()
    except Exception as e:
        print(e)


def update(org_id: str, work_id: str, runner_id: str, data: dict):
    token = get_azure_token()
    url = os.environ.get("API_URL")
    try:
        response = requests.patch(
            url=f"{url}/organizations/{org_id}/workspaces/{work_id}/runners/{runner_id}",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            json=data,
        )
        if response is None:
            print("An error occurred while getting of all organisations")
        return response.json()
    except Exception as e:
        print(e)


def start_runner(org_id: str, work_id: str, runner_id: str):
    token = get_azure_token()
    url = os.environ.get("API_URL")
    try:
        response = requests.post(
            url=f"{url}/organizations/{org_id}/workspaces/{work_id}/runners/{runner_id}/start",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
        )
        return response.json()
    except Exception as e:
        print(e)


def create(org_id: str, work_id: str, data: dict):
    token = get_azure_token()
    url = os.environ.get("API_URL")
    try:
        response = requests.post(
            url=f"{url}/organizations/{org_id}/workspaces/{work_id}/runners",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            json=data,
        )
        if response is None:
            print("An error occurred while getting of all organisations")
        return response.json()
    except Exception as e:
        print(e)


def get_org_id_by_name(organization_name: str):
    api_instance = client.CustomObjectsApi()
    group = "api.cosmotech.com"  # Update to the correct API group
    version = "v1"  # Update to the correct API version
    namespace = "cosmotech"  # Assuming custom resource is in default namespace
    plural = "organizations"
    try:
        api_response = api_instance.get_namespaced_custom_object(
            group=group,
            namespace=namespace,
            name=organization_name,
            plural=plural,
            version=version,
        )
        return api_response
    except Exception as e:
        print("Exception: %s\n" % e)


def get_work_id_by_name(workspace_name: str):
    api_instance = client.CustomObjectsApi()
    group = "api.cosmotech.com"  # Update to the correct API group
    version = "v1"  # Update to the correct API version
    namespace = "cosmotech"  # Assuming custom resource is in default namespace
    plural = "workspaces"
    try:
        api_response = api_instance.get_namespaced_custom_object(
            group=group,
            namespace=namespace,
            name=workspace_name,
            plural=plural,
            version=version,
        )
        return api_response.get("spec").get("id")
    except Exception as e:
        print("Exception: %s\n" % e)


def main():
    api_instance = client.CustomObjectsApi()
    group = "api.cosmotech.com"  # Update to the correct API group
    version = "v1"  # Update to the correct API version
    namespace = "cosmotech"  # Assuming custom resource is in default namespace
    plural = "runners"

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
            organization_name = (
                custom_resource["spec"].get("selector", {}).get("organization", "")
            )
            workspace_name = (
                custom_resource["spec"].get("selector", {}).get("workspace", "")
            )
            # Extract key-value pairs from the custom resource spec
            resource_data = custom_resource.get("spec", {})
            # Handle events of type ADDED (resource created)
            if event_type == "ADDED":
                del resource_data["selector"]
                # retrieve workspace id
                work_id = get_work_id_by_name(workspace_name=workspace_name)
                custom_resource["spec"]["workspaceId"] = work_id
                # retrieve org
                org_object = get_org_id_by_name(organization_name=organization_name)
                custom_resource["spec"]["organizationId"] = org_object.get("spec").get(
                    "id"
                )
                if not resource_data.get("id"):
                    res_ = create(
                        org_id=org_object.get("spec").get("id"),
                        work_id=work_id,
                        data=resource_data,
                    )
                    custom_resource["spec"]["id"] = res_.get("id")
                    start_runner(
                        org_id=org_object.get("spec").get("id"),
                        work_id=work_id,
                        runner_id=res_.get("id"),
                    )
                try:
                    custom_resource["metadata"] = dict(
                        ownerReferences=[
                            dict(
                                name=org_object.get("metadata").get("name"),
                                apiVersion="api.cosmotech.com/v1",
                                kind="Organization",
                                uid=org_object.get("metadata").get("uid"),
                                blockOwnerDeletion=True,
                            )
                        ],
                        **custom_resource["metadata"],
                    )
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
                delete_obj(
                    org_id=resource_data.get("organizationId"),
                    work_id=work_id,
                    runner_id=resource_data.get("id"),
                )
            elif event_type == "MODIFIED":
                org_object = get_org_id_by_name(organization_name=organization_name)
                custom_resource["spec"]["organizationId"] = org_object.get("spec").get(
                    "id"
                )
                work_id = get_work_id_by_name(workspace_name=workspace_name)
                custom_resource["spec"]["workspaceId"] = work_id
                update(
                    org_id=resource_data.get("organizationId"),
                    work_id=work_id,
                    runner_id=resource_data.get("id"),
                    data=resource_data,
                )
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

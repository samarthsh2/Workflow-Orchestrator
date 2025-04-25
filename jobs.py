# Importing required python libraries
import requests

# Importing the tasks module
import tasks

def create_job(
    host: str,
    access_token: str,
    job_name: str,
    job_description: str,
    cluster_compute_config_path: str,
    warehouse_compute_config_path: str,
) -> str:
    """
    Create a new job in the workspace.

    :param host: The host of the workspace.
    :param access_token: The access token of the workspace.
    :param job_name: The name of the job.
    :param job_description: The description of the job.
    :param cluster_compute_config_path: The path to the cluster compute configuration.
    :param warehouse_compute_config_path: The path to the warehouse compute configuration.

    :return: The response of the POST request.
    """

    # Trying to fetch the tasks details and then creating the job
    try:
        # Fetching the tasks that is to be performed in the job
        tasks_details = tasks.get_task_details(
            host=host,
            access_token=access_token,
            folder_path="/Users/samarth.bhatt@shell.com/WorkflowOrchestration",
            cluster_config_path=cluster_compute_config_path,
            warehouse_config_path=warehouse_compute_config_path,
        )

        job_details = {
            "name": job_name,
            "description": job_description,
            "tasks": tasks_details,
            "queue": {
                "enabled": True
            }
        }

        # Sending the request to create the job
        job_creation_response = requests.post(
            f"{host}api/2.1/jobs/create",
            headers={"Authorization": "Bearer %s" % access_token},
            json=job_details,
        )

    except Exception as e:
        print(f"Could not send request to create job. Error: {e}")

    else:
        if job_creation_response.status_code == 200:
            return job_creation_response.json()['job_id']
        else:
            raise Exception(
                f"Job creation failed. Response: {job_creation_response.json()}"
            )

def update_job(
    host: str,
    access_token: str,
    job_id: str,
    cluster_compute_config_path: str,
    warehouse_compute_config_path: str,
):
    """
    Updates the job with the new set of details

    :param host: The host of the workspace.
    :param access_token: The access token of the workspace.
    :param job_name: The name of the job.
    :param job_description: The description of the job.
    :param cluster_compute_config_path: The path to the cluster compute configuration.
    :param warehouse_compute_config_path: The path to the warehouse compute configuration.

    :return: The response of the POST request.
    """

    try:
        # Fetching the tasks that is to be performed in the job
        tasks_details = tasks.get_task_details(
            host=host,
            access_token=access_token,
            folder_path="/Workspace/Users/samarth.bhatt@shell.com/WorkflowOrchestration",
            cluster_config_path=cluster_compute_config_path,
            warehouse_config_path=warehouse_compute_config_path,
        )

        job_details_update = {
            "job_id": job_id,
            "new_settings": {
                "tasks": tasks_details,
            },
            "fields_to_remove": ["tasks"]
        }

        # Sending the request to update the job
        job_updation_response = requests.post(
            f"{host}api/2.1/jobs/update",
            headers={"Authorization": "Bearer %s" % access_token},
            json=job_details_update,
        )
    except Exception as e:
        print(f"Could not send request to update job. Error: {e}")

    else:
        if job_updation_response.status_code == 200:
            return job_id
        else:
            raise Exception(
                f"Job updation failed. Response: {job_updation_response.json()}"
            )

def get_job_url(
    host: str,
    access_token: str,
    job_name: str,
    job_description: str,
    cluster_compute_config_path: str,
    warehouse_compute_config_path: str,
):
    """
    Gets the url of the created job and its job ID

    :param host: The host of the workspace.
    :param access_token: The access token of the workspace.
    :param job_name: The name of the job.
    :param job_description: The description of the job.
    :param cluster_compute_config_path: The path to the cluster compute configuration.
    :param warehouse_compute_config_path: The path to the warehouse compute configuration.

    :return: The response of the POST request.
    """

    # Trying to fetch the list of jobs in the workspace
    try:
        job_list_response = requests.get(
            f"{host}api/2.1/jobs/list",
            headers={"Authorization": "Bearer %s" % access_token},
        )

    except Exception as e:
        raise Exception(f"Failed to send request to fetch list of job as {e}")

    else:
        # Checking whether we were able to extract the list of jobs successfully or not
        if job_list_response.status_code == 200:

            try:
                    
                # Checking whether the job exists in the list of jobs
                for job in job_list_response.json()["jobs"]:
                    if job["settings"]["name"] == job_name:
                        return f"Job URL {host}jobs/{update_job(host, access_token, job['job_id'], cluster_compute_config_path, warehouse_compute_config_path)}"
                # Creating a new job if no such list is found
                return f"Job URL {host}jobs/{create_job(host, access_token, job_name, job_description, cluster_compute_config_path, warehouse_compute_config_path)}"
            except Exception as e:
                raise Exception(f"Failed to create or update job as {e}")

        else:
            raise Exception(f"Failed to fetch list of job as {job_list_response.text}")

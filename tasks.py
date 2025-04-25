# Importing required modules
from Tasks_dependency.dependency import file_and_dependecies
from compute.cluster_compute import get_compute_cluster_id
from compute.warehouse_compute import get_sql_warehouse_id


def get_task_details(
    host: str,
    access_token: str,
    folder_path: str,
    cluster_config_path: str,
    warehouse_config_path: str,
) -> list:
    """
    This function creates the tasks for the workflow

    :param host:                    Accepts the host of the workspace where workflow is to be created
    :param access_token:            Accepts the access token of the workspace where workflow is to be created
    :param folder_path:             Accpets the path of the folder from workflow
    :param cluster_config_path:     Accepts the path of the cluster configuration file
    :param warehouse_config_path:   Accepts the path of the warehouse configuration file

    :return :                       A list of tasks for the workflow
    """
    # creating an empty list of tasks
    tasks = []

    # Creating an empty dictionary to store the ID for cluster or warehouses that has already been created
    compute_ids = {}

    # Looping through all the notebooks list to create the tasks
    for files in file_and_dependecies:

        # Creating an empty list to store the dependent tasks
        dependent_tasks = []

        # Creating warehouse for the file and assigning the compute it requires
        if files["format"] == "sql":
            try:
                warehouse_compute_id = get_sql_warehouse_id(
                    warehouse_config_path, host, access_token, files["warehouse_name"]
                )

                # Storing the cluster ID of the created compute cluster
                compute_ids[files["warehouse_name"]] = warehouse_compute_id

            except Exception as e:
                raise Exception(f"Could not create warehouse compute as {e}")

            else:
                warehouse_compute_id = compute_ids[files["warehouse_name"]]

            # Looping through all the dependent files and adding them in the dependent_tasks list
            for dependency in files["dependent_on"]:
                dependent_tasks.append({"task_key": dependency})

            # Appending the task in the tasks list
            tasks.append(
                {
                    "task_key": files["file_name"],
                    "description": "This is a test description of the SQL task",
                    "depends_on": dependent_tasks,
                    "run_if": "ALL_SUCCESS",
                    "sql_task": {
                        "file": {
                            "path": f"{folder_path}/notebooks/{files['file_name']}.sql",
                            "source": "WORKSPACE"
                        },
                        "warehouse_id": warehouse_compute_id,
                    },
                }
            )
        else:
            # Checking if compute is already created or not and creating it if not
            if files["compute_name"] not in compute_ids:
                try:
                    cluster_compute_id = get_compute_cluster_id(
                        files["compute_name"], cluster_config_path, host, access_token
                    )

                    # Storing the cluster ID of the created compute cluster
                    compute_ids[files["compute_name"]] = cluster_compute_id
                
                except Exception as e:
                    raise Exception(f"Could not create cluster compute as {e}")
            
            else:
                cluster_compute_id = compute_ids[files["compute_name"]]

            # Looping through all the dependent files and adding them in the dependent_tasks list
            for dependency in files["dependent_on"]:
                dependent_tasks.append({"task_key": dependency})

            # Appending the task in the tasks list
            tasks.append(
                {
                    "task_key": files["file_name"],
                    "description": "This is a test description of the task",
                    "depends_on": dependent_tasks,
                    "run_if": "ALL_SUCCESS",
                    "spark_python_task": {
                        "python_file": f"{folder_path}/notebooks/{files['file_name']}.{files['format']}",
                        "source": "WORKSPACE"
                    },
                    "existing_cluster_id": cluster_compute_id,
                }
            )


    # Returning the list of tasks to be created in a job.
    return tasks
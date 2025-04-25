# Importing necessary libraries that might be required for warehouse creation
import requests
import json


def create_compute_cluster(host: str, cluster_config: dict, access_token: str, cluster_name: str) -> str:
    """
    This function creates a new compute cluster in the workspace for the given configuration.

    :param host:           Access URL of the workspace where the cluster needs to be created.
    :param access_token:   Access token of the user who is creating the cluster.
    :param cluster_config: Configuration of the cluster to be created.
    :param cluster_name:   Name of the cluster to be created

    :return:               ID of the newly created cluster or an error message if the creation fails.
    """

    # Trying to create a cluster in the workspace using the provided configuration.
    try:
        cluster_config["cluster_name"] = cluster_name
        cluster_creation_request = requests.post(
            f"{host}api/2.1/clusters/create",
            headers={"Authorization": "Bearer %s" % access_token},
            json=cluster_config,
        )

    # If the request fails, print the error message.
    except Exception as e:
        raise Exception(f"Failed to send cluster creation request: {e}")

    # If the request is successful, return the cluster ID or an error message.
    else:
        # If the cluster creation request was successful, return the cluster ID, else return the error text
        if cluster_creation_request.status_code == 200:
            return cluster_creation_request.json()["cluster_id"]
        else:
            raise Exception(f"Failed to create cluster: {cluster_creation_request.text}")


def compare_compute_cluster_config(
    required_compute_cluster_config: dict, current_compute_cluster_config: dict
) -> dict:
    """
    Compares the current warehouse configuration with the new configuration and returns the new configuration if there is any change

    :param required_compute_cluster_config:  Accepts a dictionary of required compute cluster configuration
    :param current_compute_cluster_config:   Accepts a dictionary of current compute cluster configuration

    :return :                                Returns a new configuration if there is any change in the current configuration
    """

    # Creating a new configuration dictionary to store the new configuration
    new_compute_cluster_conf = {}

    for key, value in required_compute_cluster_config.items():

        # Checking if the type is bool or not, if its a bool then we compare it directly or we convert it to string and then compare as the dictionary that we read from the config file stores all the values in string except bool
        if key in current_compute_cluster_config:
            if type(current_compute_cluster_config[key]) == bool or type(current_compute_cluster_config[key]) == int:
                current_value = current_compute_cluster_config[key]
            else:
                current_value = str(current_compute_cluster_config[key])

        # If the config key is not in the exitsting cluster or current value is not equal to the required value then we add it to the new configuration dictionary
        if key not in current_compute_cluster_config or required_compute_cluster_config[key] != current_value:
            new_compute_cluster_conf[key] = required_compute_cluster_config[key]

    # Returning the new configuration dictionary
    return new_compute_cluster_conf


def update_compute_cluster_config(
    host: str, access_token: str, compute_cluster_config: dict, cluster_id: str, cluster_name: str
) -> str:
    """
    This function is used to update the cluster compute configuration of the existing warehouse

    :param host:                   Accepts a string of host link where the cluster compute is to be updated
    :param access_token:           Accepts a string of access token which will be used to generate request for the cluster compute updation
    :param compute_cluster_config: Accepts a dictionary of cluster compute configuration that is to be used while updating
    :param cluster_id:             Accepts a string of cluster compute id that is to be updated
    :param cluster_name:           Accepts a string of cluster compute name that is to be updated

    :return :                      Returns cluster compute ID or string mentioning why it the cluster compute updation request failed
    """

    #  Trying to send a request to update the cluster compute configuration using the requests library. It accepts the API url, access token and cluster compute configuration in dictionary format which will be used to update cluster compute
    try:
        compute_cluster_config[cluster_name]["cluster_id"] = cluster_id
        
        compute_cluster_updation_request = requests.post(
            f"{host}api/2.1/clusters/edit",
            headers={"Authorization": "Bearer %s" % access_token},
            json=compute_cluster_config[cluster_name],
        )

    # Catching any exception thrown by the requests library when it failed to send the request
    except Exception as e:
        raise Exception(f"Failed to update cluster compute configuration due to {e}")

    # If the request was sent successfully we check for the successful updation or failed creation of the cluster compute and returning accordingly
    else:
        if compute_cluster_updation_request.status_code == 200:
            return cluster_id
        else:
            raise Exception(f"Failed to update cluster compute configuration as {compute_cluster_updation_request.content}")

def get_compute_cluster_list(host: str, access_token: str):
    """
    This function is used to list all the compute clusters in the workspace

    :paran host:               Accepts a string of host link where the compute cluster is to be listed
    :param access_token:       Accepts a string of access token which will be used to generate request for the compute cluster list

    :return :                  Returns a list of compute clusters
    """
    # Declaring to store next page token
    next_page_token = ""
    has_next_page = True

    # Creating an empty list to store the list of compute clusters
    cluster_list = []

    # Looping through the pages until we get all the clustes in the workspace
    while has_next_page:
        try:
            cluster_list_request = requests.get(
                    f"{host}api/2.1/clusters/list",
                    headers={"Authorization": "Bearer %s" % access_token},
                    json={"page_token": next_page_token},
            )
        
        except Exception as e:
            raise Exception(f"Failed to send request to fetch list of compute clusters due to {e}")
        
        else:
            if cluster_list_request.status_code == 200:
                cluster_list += cluster_list_request.json()["clusters"]
                if cluster_list_request.json()["next_page_token"] != "":
                    next_page_token = cluster_list_request.json()["next_page_token"]
                else:
                    has_next_page = False
            else:
                raise Exception(f"Failed to fetch list of compute clusters as {cluster_list_request.content}")
    
    return cluster_list

def restart_cluster(host: str, access_token: str, cluster_id: str):
    """
    This function is used to restart the compute cluster

    :param host:               Accepts a string of host link where the compute cluster is to be restarte
    :param access_token:       Accepts a string of access token which will be used to generate request for the compute cluster restart
    :param cluster_id:         Accepts a string of cluster compute id that is to be restarte

    :return :                  Returns compute cluster ID or string mentioning why it the compute cluster restart request faile
    """
    try:
        restart_cluster_request = requests.post(f"{host}api/2.1/clusters/start", headers={"Authorization": "Bearer %s" % access_token}, json={"cluster_id": cluster_id})
    
    except Exception as e:
        raise Exception(f"Failed to send request to restart compute cluster due to {e}")

    else:
        if restart_cluster_request.status_code == 200:
            return cluster_id
        else:
            raise Exception(f"Failed to restart compute cluster as {restart_cluster_request.content}")


def get_compute_cluster_id(
    cluster_name: str, cluster_config_path: str, host: str, access_token: str
) -> str:
    """
    This function is used to get the compute cluster id of the existing compute cluster or create a new one

    :param cluster_name:        Accepts the name of the cluster who's ID is to be returned
    :param cluster_config_path: Accepts file path of the JSON file containing the required compute cluster configuration.
    :param host:                Accepts a string of host link where the compute cluster is to be created.
    :param access_token:        Accepts a string of access token which will be used to generate request for the compute cluster creation, updation and listing all the compute clusters.

    :returns :                  Returns compute cluster ID or string mentioning why it the compute cluster creation request failed.
    """
    # Reading the JSON file containing the required compute cluster configuration and storing the config
    with open(cluster_config_path, "r") as f:
        cluster_config = json.load(f)

    #  Trying to send a request to fetch the list of all the compute cluster using the requests library. It accepts the API url and access token in the header format which will be used to fetch the list of all the compute cluster
    try:
        compute_clusters_list = get_compute_cluster_list(host, access_token)

    # Catching any exception thrown by the requests library when it failed to send the request
    except Exception as e:
        raise Exception(f"Failed to fetch compute cluster list due to {e}")

    # If the request was sent successfully we check for an existing compute cluster or not. If there exists a compute cluster then we check for its config and update it if required and if no such compute cluster is found then we create a new one.
    else:
        # If we were able to fetch the list of compute clusters then we check for the required compute cluster in the list of compute clusters
        if compute_clusters_list != []:
            name_of_required_compute_cluster = cluster_name

            # Iterating through the list of compute clusters to find whether the required compute cluster is already present
            for compute_cluster in compute_clusters_list:

                # Checking if the compute cluster name matches with the required compute cluster name
                if name_of_required_compute_cluster == compute_cluster["cluster_name"]:

                    cluster_config[cluster_name]["cluster_name"] = cluster_name

                    new_compute_cluster_config = compare_compute_cluster_config(
                        cluster_config[cluster_name], compute_cluster
                    )

                    # If there are no changes in the compute cluster configuration then we return the cluster compute ID
                    if new_compute_cluster_config == {}:
                        # Restarting cluster if terminated
                        if compute_cluster["state"] != "RUNNING":
                            restart_cluster(host, access_token, compute_cluster["cluster_id"])
                        # Returning the cluster ID
                        return compute_cluster["cluster_id"]
                    
                    else:
                        # If there are any changes in the compute cluster configuration then we update it and return the cluster compute ID
                        return update_compute_cluster_config(
                            host,
                            access_token,
                            cluster_config,
                            compute_cluster["cluster_id"],
                            cluster_name
                        )
    
        return create_compute_cluster(host, cluster_config[cluster_name], access_token, cluster_name)
# Importing necessary libraries that might be required for warehouse creation
import requests
import json


def create_warehouse_compute(host: str, warehouse_conf: dict, access_token: str, warehouse_name: str) -> str:
    """
    This function helps in creating a warehouse for compute sql files

    :param host:           Accepts a string of host link where the warehouse is to be created
    :param warehouse_conf: Accepts a dictionary of warehouse configuration that is to be used while creating warehouse
    :param access_token:   Accepts a string of access token which will be used to generate request for the warehouse creation
    :param warehouse_name: Accepts a string of warehouse name that is to be created

    :return :              Returns warehouse ID or string mentioning why it the warehouse creation request failed
    """

    # Trying to create a warehouse creation request using the requests library. It accepts the API url, access token and warehouse configuration in dictionary format which will be used to create warehouse
    try:
        warehouse_conf["name"] = warehouse_name

        warehouse_creation_request = requests.post(
            f"{host}api/2.0/sql/warehouses",
            headers={"Authorization": "Bearer %s" % access_token},
            json=warehouse_conf,
        )

    # Catching any exception thrown by the requests library when it failed to send the request
    except Exception as e:
        raise Exception(f"Failed to send warehouse creation request due to {e}")

    # If the request was sent successfully we check for the successful creation or failed creation of the warehouse and returning accordingly
    else:
        if warehouse_creation_request.status_code == 200:
            return warehouse_creation_request.json()["id"]
        else:
            raise Exception(f"Failed to create warehouse as {warehouse_creation_request.content}")


def compare_warehouse_config(
    required_warehouse_config: dict, current_warehouse_config: dict
) -> dict:
    """
    Compares the current warehouse configuration with the new configuration and returns the new configuration if there is any change

    :param required_warehouse_config:  Accepts a dictionary of required warehouse configuration
    :param current_warehouse_config:   Accepts a dictionary of current warehouse configuration

    :return :                          Returns a new configuration if there is any change in the current configuration
    """

    # Creating a new configuration dictionary to store the new configuration
    new_warehouse_conf = {}

    for key, value in required_warehouse_config.items():

        # Checking if the type is bool or not, if its a bool then we compare it directly or we convert it to string and then compare as the dictionary that we read from the config file stores all the values in string except bool
        if type(current_warehouse_config[key]) == bool:
            current_value = current_warehouse_config[key]
        else:
            current_value = str(current_warehouse_config[key])

        # If the current value is not equal to the required value then we add it to the new configuration dictionary
        if required_warehouse_config[key] != current_value:
            new_warehouse_conf[key] = required_warehouse_config[key]

    # Returning the new configuration dictionary
    return new_warehouse_conf


def update_warehouse_config(
    host: str, access_token: str, warehouse_config: dict, warehouse_id: str, warehouse_name: str
) -> str:
    """
    This function is used to update the warehouse configuration of the existing warehouse

    :param host:             Accepts a string of host link where the warehouse is to be updated
    :param access_token:     Accepts a string of access token which will be used to generate request for the warehouse updation
    :param warehouse_config: Accepts a dictionary of warehouse configuration that is to be used while updating
    :param warehouse_id:     Accepts a string of warehouse id that is to be updated
    :param warehouse_name:   Accepts a string of warehouse name that is to be updated

    :return :                Returns warehouse ID or string mentioning why it the warehouse updation request failed
    """

    #  Trying to send a request to update the warehouse configuration using the requests library. It accepts the API url, access token and warehouse configuration in dictionary format which will be used to update warehouse
    try:
        warehouse_config["name"] = warehouse_name
        compute_cluster_config[cluster_name]["cluster_id"] = cluster_id
        warehouse_updation_request = requests.post(
            f"{host}api/2.0/sql/warehouses/{warehouse_id}/edit",
            headers={"Authorization": "Bearer %s" % access_token},
            json=warehouse_config,
        )

    # Catching any exception thrown by the requests library when it failed to send the request
    except Exception as e:
        raise Exception(f"Failed to update warehouse configuration due to {e}")

    # If the request was sent successfully we check for the successful updation or failed updation of the warehouse and returning accordingly
    else:
        if warehouse_updation_request.status_code == 200:
            return warehouse_id
        else:
            raise Exception(f"Failed to update warehouse configuration as {warehouse_updation_request.content}")


def get_sql_warehouse_id(
    warehouse_config_path: str, host: str, access_token: str, warehouse_name: str
) -> str:
    """
    This function is used to get the SQL warehouse id of the existing warehouse or create a new one

    :param warehouse_config_path: Accepts file path of the JSON file containing the required warehouse configuration
    :param host:                  Accepts a string of host link where the warehouse is to be created
    :param access_token:          Accepts a string of access token which will be used to generate request for the warehouse creation, updation and listing all the warehouses
    :param warehouse_name:        Accepts a string of warehouse name that is to be created or updated

    :returns :                    Returns warehouse ID or string mentioning why it the warehouse creation request failed
    """

    # Reading the JSON file containing the required warehouse configuration and storing the config
    with open(warehouse_config_path, "r") as f:
        warehouse_conf = json.load(f)

    #  Trying to send a request to fetch the list of all the warehouses using the requests library. It accepts the API url and access token in the header format which will be used to fetch the list of all the warehouses
    try:
        warehouses_list_request = requests.get(
            f"{host}api/2.0/sql/warehouses",
            headers={"Authorization": "Bearer %s" % access_token},
        )

    # Catching any exception thrown by the requests library when it failed to send the request
    except Exception as e:
        raise Exception(f"Failed to fetch warehouse list due to {e}")

    # If the request was sent successfully we check for an existing warehouse or not. If there exists a warehouse then we check for its config and update it if required and if no such warehouse is found then we create a new one.
    else:
        if warehouses_list_request.status_code == 200:
            list_of_warehouses = warehouses_list_request.json()["warehouses"]
            name_of_required_warehouse = warehouse_name

            for warehouse in list_of_warehouses:
                if name_of_required_warehouse == warehouse["name"]:
                    warehouse_conf[warehouse_name]["name"] = warehouse_name
                    new_warehouse_config = compare_warehouse_config(
                        warehouse_conf[warehouse_name], warehouse
                    )

                    if new_warehouse_config == {}:
                        return warehouse["id"]

                    else:
                        return update_warehouse_config(
                            host, access_token, new_warehouse_config, warehouse["id"], warehouse_name
                        )
        else:
            raise Exception(f"Failed to fetch the list of warehouses as {warehouses_list_request.content}")

        return create_warehouse_compute(host, warehouse_conf[warehouse_name], access_token, warehouse_name)
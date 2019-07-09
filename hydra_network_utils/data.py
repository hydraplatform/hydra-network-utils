import pandas
from hydra_base.lib.objects import Dataset
import json


def make_dataframe_dataset_value(existing_value, df, data_type, column):

    if data_type.lower() == 'dataframe':
        existing_df = pandas.read_json(existing_value)
        # Update the dataframe
        existing_df[column] = df
        # Embed data as strings of datetimes rather than timestamps.
        existing_df.index = existing_df.index.astype(str)
        value = existing_df.to_json(orient='columns')
    elif data_type.lower() == 'pywr_dataframe':
        value = json.loads(existing_value)

        if "data" in value:
            existing_df = pandas.read_json(json.dumps(value["data"]))
            existing_df[column] = df
            # Embed data as strings of datetimes rather than timestamps.
            existing_df.index = existing_df.index.astype(str)
            value["data"] = json.loads(existing_df.to_json())
        else:
            # Embed data as strings of datetimes rather than timestamps.
            df.index = df.index.astype(str)
            value["data"] = json.loads(df.to_json())
        value = json.dumps(value)
    else:
        raise NotImplementedError(f'Datatype "{data_type.upper()}" not supported.')

    return value


def import_dataframe(client, dataframe, network_id, scenario_id, attribute_id, column,
                     create_new=False, data_type='PYWR_DATAFRAME'):

    # Find all the nodes in the network

    node_data = {}
    for node_name in dataframe:
        # An exception is raised by hydra if the node name does not exist.
        node = client.get_node_by_name(network_id, node_name)

        # Fetch the node's data
        resource_scenarios = client.get_resource_data('NODE', node['id'], scenario_id)
        for resource_scenario in resource_scenarios:
            resource_attribute_id = resource_scenario['resource_attr_id']
            resource_attribute = client.get_resource_attribute(resource_attribute_id)

            if resource_attribute['attr_id'] != attribute_id:
                continue  # Skip the wrong attribute data

            dataset = resource_scenario['dataset']

            if dataset['type'].lower() != data_type.lower():
                raise ValueError(f'Node "{node_name}" datatset for attribute_id "{attribute_id}" must be'
                                 f' type "{data_type.upper()}", not type "{dataset["type"]}".')

            dataset['value'] = make_dataframe_dataset_value(dataset['value'], dataframe[node_name], data_type, column)

            node_data[node_name] = {
                'node_id': node['id'],
                'resource_attribute_id': resource_attribute['id'],
                'dataset': dataset,
            }

        if node_name not in node_data:
            if not create_new:
                # No resource attribute found!
                raise ValueError(f'Node "{node_name}" does not contain a resource attribute '
                                 f'for the attribute "{attribute_id}".')
            else:
                resource_attribute = client.add_resource_attribute('NODE', node['id'], attribute_id, 'N',
                                                                   error_on_duplicate=False)

                df = dataframe[node_name].to_frame()
                df.columns = [column]

                value = {
                    "type": "dataframeparameter",
                    "pandas_kwargs": {"parse_dates": True}
                }

                dataset = Dataset({
                    'name': "data",
                    'value': make_dataframe_dataset_value(value, df, data_type, column),
                    "hidden": "N",
                    "type": data_type.upper(),
                    "unit": "-",
                })

                node_data[node_name] = {
                    'node_id': node['id'],
                    'resource_attribute_id': resource_attribute['id'],
                    'dataset': dataset,
                }

    # Now update the database with the new data
    for node_name, data in node_data.items():
        client.add_data_to_attribute(scenario_id, data['resource_attribute_id'], data['dataset'])


def export_dataframes(client, network_id, scenario_id, attribute_ids=None):

    nodes = client.get_nodes(network_id)

    node_dataframes = {}

    for node in nodes:
        dataframes = {}  # Dataframes from this node
        # Fetch the node's data
        resource_scenarios = client.get_resource_data('NODE', node['id'], scenario_id)
        for resource_scenario in resource_scenarios:
            resource_attribute_id = resource_scenario['resource_attr_id']
            resource_attribute = client.get_resource_attribute(resource_attribute_id)

            if attribute_ids is not None and resource_attribute['attr_id'] not in attribute_ids:
                continue  # Skip the wrong attribute data

            attribute = client.get_attribute_by_id(resource_attribute['attr_id'])
            attribute_name = attribute['name']

            dataset = resource_scenario['dataset']

            if dataset['type'].lower() != 'dataframe':
                continue  # Skip non-datasets

            df = pandas.read_json(dataset['value'])
            yield node['name'], attribute_name, df



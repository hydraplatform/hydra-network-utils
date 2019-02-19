import pandas
from hydra_base.lib.objects import Dataset


def import_dataframe(client, dataframe, network_id, scenario_id, attribute_id, column,
                     create_new=False):

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

            if dataset['type'].lower() != 'dataframe':
                raise ValueError(f'Node "{node_name}" datatset for attribute_id "{attribute_id}" must be'
                                 f' type "DATAFRAME", not type "{dataset["type"]}".')

            existing_df = pandas.read_json(dataset['value'])
            # Update the dataframe
            existing_df[column] = dataframe[node_name]
            # .. and the dataset
            dataset['value'] = existing_df.to_json(orient='columns')

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

                dataset = Dataset({
                    'name': "data",
                    'value': df.to_json(orient='columns'),
                    "hidden": "N",
                    "type": "DATAFRAME",
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



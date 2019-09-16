import pandas
from hydra_base.lib.objects import JSONObject, ResourceScenario, Dataset
from hydra_base.exceptions import HydraError
import json

import logging
log = logging.getLogger(__name__)

def json_to_df(json_dataframe):
    """
     Create a pandas dataframe from a json string.
     Pandas does not maintain the order of the index; it sorts it.
     To manage this, we avail of python 3.6+ inhernet dict ordering to identify
     the correct index order, and then reindex the dataframe to this after
     it has been created
    """

    #Load the json dataframe into a native dict
    data_dict = json.loads(json_dataframe)
    
    #load the json dataframe into a pandas dataframe
    df = pandas.read_json(json_dataframe)

    #extraxt the ordered index from the dict
    ordered_index = list(data_dict[df.columns[0]].keys())

    #extract the order of columns
    ordered_cols = list(data_dict.keys())
    
    #Make the df index and columns a string so it is comparable to the dict index (which must be string based)
    df.index = df.index.astype(str)
    df.columns = df.columns.astype(str)
    
    #set the column ordering as per the incoming json data
    df = df[ordered_cols]

    #reindex the dataframe to have the correct order
    df = df.reindex(ordered_index)
    
    return df

def make_dataframe_dataset_value(existing_value, df, data_type, column=None):
    
    #Turn the target dataframe's index into a string so it is comparable to
    #the index of the dataframe coming from existing_value
    df.index = df.index.astype(str)
    
    if data_type.lower() == 'dataframe':
        if column is not None:
            # Update the dataframe
            existing_df = json_to_df(existing_value)
            existing_df[column] = df
        else:
            existing_df = df
        # Embed data as strings of datetimes rather than timestamps.
        existing_df.index = existing_df.index.astype(str)
        value = existing_df.to_json(orient='columns')
    elif data_type.lower() == 'pywr_dataframe':
        value = json.loads(existing_value)

        if "data" in value:
            if column is not None:
                # Update the dataframe
                existing_df = json_to_df(json.dumps(value["data"]))
                existing_df[column] = df
            else:
                existing_df = df
            # Embed data as strings of datetimes rather than timestamps.
            existing_df.index = existing_df.index.astype(str)
            value["data"] = json.loads(existing_df.to_json())
        else:
            # Embed data as strings of datetimes rather than timestamps.
            log.critical("Updating the value as a PYWR dataframe.")
            df.index = df.index.astype(str)
            value["data"] = json.loads(df.to_json())
        value = json.dumps(value)
    else:
        raise NotImplementedError(f'Datatype "{data_type.upper()}" not supported.')

    return value


def import_dataframe(client, dataframe, network_id, scenario_id, attribute_id, column=None,
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

                if data_type.lower() == 'dataframe':
                    # Embed data as strings of datetimes rather than timestamps.
                    df.index = df.index.astype(str)
                    value = df.to_json(orient='columns')
                else:
                    default_value = json.dumps({
                        "type": "dataframeparameter",
                        "pandas_kwargs": {"parse_dates": True}
                    })

                    value = make_dataframe_dataset_value(default_value, df, data_type, column)

                dataset = Dataset({
                    'name': "data",
                    'value': value,
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

            df = json_to_df(dataset['value'])
            yield node['name'], attribute_name, df


def get_resource_scenario(client, resource_attr_id, scenario_id):
    """
        Retrieve a resource scenario object (including dataset) using a
        resource_attr_id and scenario_id.
    """

    rs_i = client.get_resource_scenario(resource_attr_id, scenario_id)

    return ResourceScenario(rs_i)

def get_matching_resource_scenarios(client, resource_attr_id, scenario_id, scenario_ids):
    """
        Find the equivalent RS objects from a list of scenarios:
        These scenarios can exist in other networks. These networks should have a resource (node / link) that
        is equivalent to the network being searched from -- same name, type and attribute.

        The *source* is the network being searched from

        The *targets* are the other networks where the matching resource scenarios are being searched
    """

    source_rs = get_resource_scenario(client, resource_attr_id, scenario_id)

    #Identify the node name, type and attribute ID to use as search criteria in the other scenarios
    source_ra = client.get_resource_attribute(resource_attr_id)

    source_attr_id = source_ra.attr_id
    source_node_id = source_ra.node_id

    source_node = client.get_node(source_node_id)
    
    #Identify the network IDS from the target scenario IDS
    target_network_ids = []
    for s_id in scenario_ids:
        scenario_network = client.get_scenario(s_id, include_data=False)
        target_network_ids.append(scenario_network.network_id)
    
    #Using the network IDS and node name, find the equivalent node in each of the
    #target networks.
    target_nodes = []
    for  network_id in target_network_ids:
        try:
            target_node = client.get_node_by_name(network_id, source_node.name)
            target_nodes.append(target_node)
        except HydraError:
            raise Exception("Network %s doesn't have a node with the name %s".format(network_id, source_node.name))
    
    #Now find the resource attr ID for each of the target nodes.
    target_ra_ids = [] 
    for target_node in target_nodes:
        for ra in target_node.attributes:
            if ra.attr_id == source_attr_id:
                target_ra_ids.append(ra.id)
                break
        else:
            raise Exception("Unable to find attribute {} on node {}".format(source_attr_id, target_node.name))
    
    #Now that we have the RA IDS and scenario IDS, find the RSs from each scenario
    target_rs = []
    for i, target_ra_id in enumerate(target_ra_ids):
        #these have been kept in the same order, so find the scenario id from the index
        target_scenario_id = scenario_ids[i]
        target_network_id = target_network_ids[i]

        try:
            target_rs_i = client.get_resource_scenario(target_ra_id, target_scenario_id)
        except HydraError:
            raise Exception("Scenario {0} in network {1} does not have data for attribute {2}".format(target_scenario_id, target_network_id, source_attr_id))

        target_rs_j = JSONObject(target_rs_i)

        target_rs.append(target_rs_j)

    return target_rs

def extract_dataframes(rs_list):
    """
        Given a list of resource scenarios, extract the dataframe value from the dataset within the RS.
    """

    dataframes = []
    for rs in rs_list:
        dataset = rs.dataset
        if dataset.type.lower() != 'dataframe':
            raise Exception("Value in scenario {} isn't a dataframe".format(rs.scenario_id))
        try:
            pandas_df = json_to_df(dataset.value)
        except:
            raise Exception("Unable to read dataframe from scenario {0}".format(rs.scenario_id))

        #before saving the dataframe, add the scenario ID to the column names so they can be unique
        original_cols = pandas_df.columns
        new_cols = []
        for c in original_cols:
            new_cols.append("{}_{}".format(c, rs.scenario_id))

        pandas_df.columns = new_cols
            
        dataframes.append(pandas_df)

    return dataframes

def combine_dataframes(dataframes):
    """
        Take a list of pandas dataframes with the same index and combine them into a single multi-column
        dataframe.
    """
    #merge the datframes, assuming they have the same inndex (axis=1 does that)
    concat_df = pandas.concat(dataframes, axis=1)

    dataset = Dataset({
        'name'  : 'Combined Dataframe',
        'type'  : 'dataframe',
        'value' : concat_df.to_json()
    })

    return dataset

def update_resource_scenario(client, resource_attribute_id, scenario_id, combined_dataframe):
    """
        set the value of an RA on a scenario to the specified dataframe value.
    """

    rs = JSONObject({
        'resource_attr_id' : resource_attribute_id,
        'scenario_id' : scenario_id,
        'dataset' : combined_dataframe
    })

    client.update_resourcedata(scenario_id, [rs])

def assemble_dataframes(client, resource_attribute_ids, scenario_id, source_scenario_ids):
    """
        Create a single data frame into a resource attribute by finding
        equivalent resource attributes on other specified networks (identified through
        scenario IDS)
    """

    assembled_dataframes = []
    log.info("Retrieving data for resource attributes %s into %s ", resource_attribute_ids, source_scenario_ids)
    for resource_attribute_id in resource_attribute_ids: 
        matching_rs_list = get_matching_resource_scenarios(client,
                                                       resource_attribute_id,
                                                       scenario_id,
                                                       source_scenario_ids)
        
        log.info("[RA %s] [Scenario IDS %s] [RS IDs %s]",
                                                    resource_attribute_id,
                                                    source_scenario_ids,
                                                    resource_attribute_id)
        dataframes = extract_dataframes(matching_rs_list)

        combined_dataframe = combine_dataframes(dataframes)

        update_resource_scenario(client, resource_attribute_id, scenario_id, combined_dataframe)

        assembled_dataframes.append(combined_dataframe)

    return assembled_dataframes


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
    ordered_index = list(data_dict[str(df.columns[0])].keys())

    #extract the order of columns
    ordered_cols = list(data_dict.keys())

    #Make the df index and columns a string so it is comparable to the dict index (which must be string based)
    df.index = df.index.astype(str)
    df.columns = df.columns.astype(str)

    #set the column ordering as per the incoming json data
    df = df[ordered_cols]

    #reindex the dataframe to be properly ordered but only if it's not a timeseries
#    if not isinstance(df.index, pandas.DatetimeIndex):
#        #reindex the dataframe to have the correct order
#        df = df.reindex(ordered_index)

    return df

def make_dataframe_dataset_value(existing_value, df, data_type,
                                 column=None, node_name=None, overwrite=False):

    #Turn the target dataframe's index into a string so it is comparable to
    #the index of the dataframe coming from existing_value
    df.index = df.index.astype(str)

    #if it's not a dataframe, it's probably a series,
    #so turn it into a dataframe
    if not isinstance(df, pandas.DataFrame):
        df = df.to_frame(name='0')

    if data_type.lower() == 'dataframe':

        if overwrite is False:


            value = _update_dataframe(existing_value, df, column=column)

        else:
            #Set the value directly (overwriting any existing value)
            value = df.to_json(orient='columns')

    elif data_type.lower() == 'pywr_dataframe':

        if overwrite is False:

            value = _update_pywr_dataframe(existing_value, df,
                                           column=column,
                                           node_name=node_name)

        else:
            df.index = df.index.astype(str)
            value = {
                "type": "dataframeparameter",
                "data": json.loads(df.to_json(orient='columns')),
                "pandas_kwargs": {"parse_dates": True}
            }
        value = json.dumps(value)
    else:
        raise NotImplementedError(f'Datatype "{data_type.upper()}" not supported.')

    return value

def _update_pywr_dataframe(existing_value, new_df, column=None, node_name=None):
    """
        Update an existing pywr dataframe. A pywr dataframe is a dict containing
        a 'data' entry, which is a json-representaion of a pandas dataframe.
    """
    value = json.loads(existing_value)

    if "data" in value:
        #default to null in case the data in Hydra isn't a dataframe which
        #can be updated, and needs to be overwritten
        existing_df = None
        try:
            existing_df = json_to_df(json.dumps(value["data"]))
        except Exception as err:
            log.warning(f"Unable to convert {node_name} value to a dataframe.\n"+
                        " This value must already be a dataframe.\n"+
                        f" Error was {err}\n")
            raise

        if column is not None:
            # Update the specified column of the existing dataframe
            existing_df[column] = new_df
        else:

            if existing_df is None:
                existing_df = new_df
            #if not column is specified, overwrite the dataframe.
            #This can only be done if the existing dataframe has a single column,
            #as otherwise we won't know which column to update
            elif len(existing_df.columns) == 1:
                existing_df = new_df
            else:
                raise Exception(f"Can't set value on node {node_name}. "+
                                "Existing value has more than one column."+
                                "Please specify which column to update with"+
                                " the --column argument")
        # Embed data as strings of datetimes rather than timestamps.
        existing_df.index = existing_df.index.astype(str)
        value["data"] = json.loads(existing_df.to_json())
    else:
        # Embed data as strings of datetimes rather than timestamps.
        log.warning("Value on %s has no 'data' entry. Updating the value as a PYWR dataframe.", node_name)
        new_df.index = new_df.index.astype(str)
        value["data"] = json.loads(new_df.to_json())

    return value

def _update_dataframe(existing_value, new_df, column=None):
    """
        Update an existing dataframe (which is input as a JSON string)
        with the data from a newe DF. The new DF may only contain a subset
        of the columns in existing_value, in which case, only update the column
        specified
    """

    #Try to update an existing value.
    #if there's only one column, then ignore the column parameter and just
    #use the existing one in the dataset
    existing_df = json_to_df(existing_value)
    if len(existing_df.columns) == 1:
        #If the incoming data has more or less rows than the existing
        #dataframe, then the existing one must be reindexed
        if len(new_df.index) != len(existing_df.index):
            existing_df = existing_df.reindex(new_df.index)
        existing_df[existing_df.columns[0]] = new_df
    elif column is not None:
        #If the incoming data has more or less rows than the existing
        #dataframe, then the existing one must be reindexed
        if len(new_df.index) != len(existing_df.index):
            existing_df = existing_df.reindex(new_df.index)
        existing_df[column] = new_df
    else:
        existing_df = new_df

    # Embed data as strings of datetimes rather than timestamps.
    existing_df.index = existing_df.index.astype(str)
    value = existing_df.to_json(orient='columns')

    return value

def import_dataframe(client, dataframe, network_id, scenario_id, attribute_id, column=None,
                     create_new=False, data_type='DATAFRAME', overwrite=False):
    """
    args:
        client: (JSONConnection): The hydra client object
        dataframe (pandas dataframe): pandas dataframe read from excel
        network_id (int): the network ID
        scenario_id (int): THe scenario ID
        attribute_id (int): The attribute ID to update.
        column (string): The name of the specific colum to use. If None, uses all of them.
        create_new (bool): default False : If an node attribute doesn't exist, create it.
        data_type (ENUM (PYWR_DATAFRAME, DATAFRAME)): The data type the new dataset should be.
        overwrite (bool): If true, it overwrites an existing valuye with the new one. If false
                          it will try to update the existing value. The data type of the existing
                          value must match that of the updating value
    """
    # Find all the nodes in the network

    scenario = client.get_scenario(scenario_id, include_data=False)
    network_id = scenario.network_id


    attribute = client.get_attribute_by_id(attribute_id)

    node_data = {}
    for node_name in dataframe:
        # An exception is raised by hydra if the node name does not exist.
        try:
            node = client.get_node_by_name(network_id, node_name)
        except Exception as e:
            log.warning(e)

        # Fetch the node's data
        resource_scenarios = client.get_resource_data('NODE', node['id'], scenario_id)
        for resource_scenario in resource_scenarios:
            resource_attribute_id = resource_scenario['resource_attr_id']
            resource_attribute = client.get_resource_attribute(resource_attribute_id)

            if resource_attribute['attr_id'] != attribute_id:
                continue  # Skip the wrong attribute data

            dataset = resource_scenario['dataset']

            if dataset['type'].lower() != data_type.lower() and overwrite == False:
                raise ValueError(f'Node "{node_name}" datatset for attribute_id'
                                 f' {attribute_id}" must be'
                                 f' type "{dataset["type"]}", not type "{data_type.upper()}".')

            dataset['value'] = make_dataframe_dataset_value(dataset['value'],
                                                            dataframe[node_name],
                                                            data_type,
                                                            column,
                                                            node_name,
                                                            overwrite=overwrite)
            #update the data type if necessary
            dataset['type'] = dataset['type'] if overwrite is False else data_type

            node_data[node_name] = {
                'node_id': node['id'],
                'resource_attribute_id': resource_attribute['id'],
                'dataset': dataset,
            }

        if node_name not in node_data:
            if not create_new:
                # No resource attribute found!
                raise ValueError(f'Node "{node_name}" does not contain a resource attribute '
                                 f'for the attribute "{attribute["name"]}".')
            else:
                resource_attribute = client.add_resource_attribute('NODE',
                                                                   node['id'],
                                                                   attribute_id, 'N',
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

                    value = make_dataframe_dataset_value(default_value,
                                                         df,
                                                         data_type,
                                                         column,
                                                         node_name,
                                                         overwrite=overwrite)

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
            raise Exception(f"Network {network_id} doesn't have a node with the"+
                            f" name {source_node.name}")

    #Now find the resource attr ID for each of the target nodes.
    target_ra_ids = []
    for target_node in target_nodes:
        for ra in target_node.attributes:
            if ra.attr_id == source_attr_id:
                target_ra_ids.append(ra.id)
                break
        else:
            raise Exception(f"Unable to find attribute {source_attr_id} on "+
                            f"node { target_node.name}")

    #Now that we have the RA IDS and scenario IDS, find the RSs from each scenario
    target_rs = []
    for i, target_ra_id in enumerate(target_ra_ids):
        #these have been kept in the same order, so find the scenario id from the index
        target_scenario_id = scenario_ids[i]
        target_network_id = target_network_ids[i]

        try:
            target_rs_i = client.get_resource_scenario(target_ra_id, target_scenario_id)
        except HydraError:
            raise Exception(f"Scenario {target_scenario_id} in network"+
                            f" {target_network_id} does not have data for"+
                            f" attribute {source_attr_id}")

        target_rs_j = JSONObject(target_rs_i)

        target_rs.append(target_rs_j)

    return target_rs

def extract_dataframes(rs_list):
    """
        Given a list of resource scenarios, extract the dataframe value
        from the dataset within the RS.
    """

    dataframes = []
    for rs in rs_list:
        dataset = rs.dataset
        if dataset.type.lower() != 'dataframe':
            raise Exception("Value in scenario {} isn't a dataframe".format(rs.scenario_id))
        try:
            log.info(dataset.value)
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
        Take a list of pandas dataframes with the same index and combine
        them into a single multi-column dataframe.
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

    log.info("Retrieving data for resource attributes %s into %s ",
        resource_attribute_ids,
        source_scenario_ids)

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

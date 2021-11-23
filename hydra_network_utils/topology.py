"""
A library of functions relating to manipulating or storing the topology of hydra networks
"""
import os
import pandas as pd
import json

def export_coordinates(client, network_ids, data_dir='/tmp'):
    """
        Extract the coordinates from a list of networks, put them into a dataframe
        and export the dataframe to a csv, compatible with the apply_coordinates function
    """

    data = {}
    for network_id in network_ids:
        print(f"Getting nodes for network {network_id}")
        nodes = client.get_nodes(network_id)
        for node in nodes:
            data[node.name] = {'Lat': node.y, 'Lon': node.x}

    df = pd.DataFrame.from_dict(data).T.fillna(0)

    df.index.name = 'Name'

    output_filename = os.path.join(data_dir, 'node_coordinates.csv')

    df.to_csv(output_filename)

    print(f"Node coordinates written to {output_filename}")

def apply_coordinates(client, filename, network_id):
    """
        Apply coordinates specified in a file to the nodes in the specified network
    """

    if filename.endswith('csv'):
        coordinate_df = pd.read_csv(filename)
    elif filename.endswith('xlsx'):
        coordinate_df = pd.read_excel(filename)
    else:
        raise Exception("Unrecognised file type. It should be .csv or .xlsx")

    nodes = client.get_nodes(network_id)
    node_dict = dict((n.name.lower(), n.id) for n in nodes)

    node_coordinates = []

    coordinate_df.columns = [c.lower() for c in coordinate_df.columns]
    for row in coordinate_df.itertuples():
        normalised_name = row.name.strip().lower()
        if normalised_name in node_dict:
            node_id = node_dict[normalised_name]

            node_coordinates.append({
                'id': node_id,
                'x': row.lat,
                'y': row.lon
            })

    if len(node_coordinates) > 0:
        client.update_nodes(node_coordinates)
    print("Coordinates applied to %s nodes"%len(node_coordinates))


def apply_layouts(client, filename, network_id):

    #filename = os.path.basename(filename)
    #fn = os.path.join(UPLOAD_DIR, filename)
    fn = filename

    # Open the layoutso
    with open(fn) as fh:
        layouts = json.load(fh)

    nodes = client.get_nodes(network_id)
    node_ids = {n['name']: n['id'] for n in nodes}

    links = client.get_links(network_id)
    link_ids = {l['name']: l['id'] for l in links}

    node_layouts = []
    for node_name, layout in layouts.get('nodes', {}).items():
        node_id = node_ids[node_name]

        node_layouts.append({
            'id': node_id,
            'layout': layout
        })

    link_layouts = []
    for link_name, layout in layouts.get('links', {}).items():
        link_id = link_ids[link_name]

        link_layouts.append({
            'id': link_id,
            'layout': layout
        })

    if len(node_layouts) > 0:
        client.update_nodes(node_layouts)
    if len(link_layouts) > 0:
        # TODO Missing `update_links` function in hydra-base: https://github.com/hydraplatform/hydra-base/issues/66
        for link_layout in link_layouts:
            client.update_link(link_layout)

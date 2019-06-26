import fiona
import os
import math
from shapely.geometry import Polygon, shape


def nearby_node(nodes, coordinates, distance):
    """ Return a node that is within distance of coordinates. """
    x1, y1 = coordinates
    for node in nodes:
        x2, y2 = node['x'], node['y']
        d = math.sqrt((x1 - x2)**2 + (y1 - y2)**2)
        if d <= distance:
            return node
    return None


def import_links_from_shapefile(client, shapefile, network_id, node_template_type_id,
                                link_template_type_id, node_merge_distance=None):

    nodes = []
    links = []

    base, ext = os.path.splitext(os.path.basename(shapefile))

    node_name = f'{base}-node'
    link_name = f'{base}-link'

    node_id = -1
    link_id = -1

    with fiona.open(shapefile) as src:
        for feature in src:
            geometry = feature['geometry']

            if geometry['type'].lower() == "linestring":
                coordinates = geometry['coordinates']
            else:
                raise ValueError('Only single part "linestring" geometries are supported!')

            first_coordinate = coordinates.pop(0)
            last_coordinate = coordinates.pop()

            first_node = None
            if node_merge_distance is not None:
                first_node = nearby_node(nodes, first_coordinate, node_merge_distance)

            if first_node is None:
                first_node = {
                    'id': node_id,
                    'name': f'{node_name}-{-node_id}',
                    'description': '',
                    'layout': None,
                    'x': first_coordinate[0],
                    'y': first_coordinate[1],
                    'attributes': [],
                    'types': [{'id': node_template_type_id}]
                }
                node_id -= 1
                nodes.append(first_node)

            last_node = None
            if node_merge_distance is not None:
                last_node = nearby_node(nodes, last_coordinate, node_merge_distance)

            if last_node is None:
                last_node = {
                    'id': node_id,
                    'name': f'{node_name}-{-node_id}',
                    'description': '',
                    'layout': None,
                    'x': last_coordinate[0],
                    'y': last_coordinate[1],
                    'attributes': [],
                    'types': [{'id': node_template_type_id}]
                }
                node_id -= 1
                nodes.append(last_node)

            if last_node == first_node:
                raise ValueError('First nodes and last nodes are the same. The `node_merge_distance`'
                                 ' is likely too high and has merged the nodes at the start and end'
                                 ' of a link. Try lowering this value.')

            link = {
                'id': link_id,
                'name': f'{link_name}-{-link_id}',
                'description': None,
                'layout': {
                    'geojson': {
                        'coordinates': coordinates
                    }
                },
                'node_1': first_node,
                'node_2': last_node,
                'attributes': [],
                'types': [{'id': link_template_type_id}]
            }
            link_id -= 1
            links.append(link)

    # Add the nodes to the network
    all_nodes = client.add_nodes(network_id, nodes)

    # Update the nodes with the correct database ids
    for node in nodes:
        for hydra_node in all_nodes:
            if hydra_node['name'] == node['name']:
                node['id'] = hydra_node['id']
                break
        else:
            raise ValueError('Node name "{}" not found in returned nodes '
                             'from the database.'.format(node['name']))

    for link in links:
        node_1 = link.pop('node_1')
        link['node_1_id'] = node_1['id']
        node_2 = link.pop('node_2')
        link['node_2_id'] = node_2['id']

    client.add_links(network_id, links)


def import_nodes_from_shapefile(shapefile, node_template_type_id, name_attributes=None):

    nodes = []

    base, ext = os.path.splitext(os.path.basename(shapefile))

    node_id = -1

    with fiona.open(shapefile) as src:

        try:
            projection = src.crs['proj']
        except KeyError:
            projection = None

        for feature in src:

            geometry = feature['geometry']
            geometry_type = geometry['type'].lower()

            if name_attributes is None:
                name = f'{base}-node-{-node_id}'
            else:
                name = '_'.join(str(feature['properties'][a]) for a in name_attributes)
                if name == '':
                    name = f'{base}-node-{-node_id}'

            if geometry_type == "point":
                point = geometry['coordinates']
                coordinates = None
                geometry_type = None
            elif geometry_type in ("polygon", "multipolygon"):
                coordinates = geometry['coordinates']
                point = shape(geometry).representative_point()
                point = point.coords[0]

            else:
                continue
                #raise NotImplementedError('Geometry type "{}" not supported!'.format(geometry['type']))

            print(feature['properties'])

            node = {
                'id': node_id,
                'name': name,
                'description': '',
                'layout': None,
                'x': point[0],
                'y': point[1],
                'attributes': [],
                'types': [{'id': node_template_type_id}]
            }
            node_id -= 1

            nodes.append(node)

            if coordinates is not None:
                node['layout'] = {
                    'geojson': {
                        'type': geometry_type,
                        'coordinates': coordinates
                    }
                }

    return nodes, projection


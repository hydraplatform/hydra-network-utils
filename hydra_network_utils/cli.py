import click
import os
from hydra_base import config
from hydra_client.connection import JSONConnection
from hydra_client.click import hydra_app, make_plugins, write_plugins
import json
from collections import defaultdict
import pandas
import re
from .gis import import_nodes_from_shapefile, import_links_from_shapefile
from . import data
from . import topology

UPLOAD_DIR = config.get('plugin', 'upload_dir', '/tmp/uploads')
UPLOAD_DIR = config.get('plugin', 'output_dir', '/tmp/uploads')

def get_client(hostname, **kwargs):
    return JSONConnection(app_name='Pywr GIS App', db_url=hostname, **kwargs)


def get_logged_in_client(context, user_id=None):
    session = context['session']
    client = get_client(context['hostname'], session_id=session, user_id=user_id)
    if client.user_id is None:
        client.login(username=context['username'], password=context['password'])
    return client


def start_cli():
    cli(obj={}, auto_envvar_prefix='HYDRA_GIS')


@click.group()
@click.pass_obj
@click.option('-u', '--username', type=str, default=None)
@click.option('-p', '--password', type=str, default=None)
@click.option('-h', '--hostname', type=str, default=None)
@click.option('-s', '--session', type=str, default=None)
def cli(obj, username, password, hostname, session):
    """ CLI for the Pywr-GIS application. """

    obj['hostname'] = hostname
    obj['username'] = username
    obj['password'] = password
    obj['session'] = session



@hydra_app(category='network_utility', name='Merge Scenarios')
@cli.command(name='merge-scenarios')
@click.pass_obj
@click.option('--source', type=int, default=None)
@click.option('--target', type=int, default=None)
@click.option('--allow-unmatched-names', default=False, is_flag=True)
@click.option('--ignore-missing-attributes', default=False, is_flag=True)
@click.option('-u', '--user-id', type=int, default=None)
def merge_scenarios(obj, source, target, allow_unmatched_names, ignore_missing_attributes, user_id):
    """
    merge data from one scenario to another, for any node name and attribute
    that matches
    """
    client = get_logged_in_client(obj, user_id=user_id)

    match_all_names = allow_unmatched_names is not True
    ignore_missing_attributes = ignore_missing_attributes is True

    client.merge_scenarios(
        source,
        target,
        match_all_names=match_all_names,
        ignore_missing_attributes=ignore_missing_attributes)


@hydra_app(category='network_utility', name='Set Link Layouts')
@cli.command(name='set-link-layouts')
@click.pass_obj
def import_links(obj):
    """
    """
    client = get_logged_in_client(obj, user_id=2)

    with open("/home/stephen/git/models/wre/WRE_Simulator/submodels/ruthamford/advanced/Ruthamford.json") as f:
        data = json.load(f)

    ruthamford_nodes = []
    for n in data['nodes']:
        ruthamford_nodes.append(n['name'])

    net = client.get_network(5026)
    ruthamford_node_ids = []
    for n in net.nodes:
        if n.name in ruthamford_nodes:
            ruthamford_node_ids.append(n.id)
    ruthamford_links = []
    for l in net.links:
        if l.node_1_id in ruthamford_node_ids or l.node_2_id in ruthamford_node_ids:
            l.layout = {'color': 'red'}
            ruthamford_links.append(l)
    client.update_links(ruthamford_links)

@hydra_app(category='network_utility', name='Import links from GIS')
@cli.command(name='import-links')
@click.pass_obj
@click.option('--filename', type=click.Path(file_okay=True, dir_okay=False))
@click.option('-n', '--network-id', type=int, default=None)
@click.option('--node-template-type-id', type=int, default=None)
@click.option('--link-template-type-id', type=int, default=None)
@click.option('--node-merge-distance', type=float, default=None)
@click.option('-u', '--user-id', type=int, default=None)
def import_links(obj, filename, network_id, user_id, node_template_type_id, link_template_type_id, node_merge_distance):
    """Import nodes and links from a GIS file.

    This app searches the GIS file for LINESTRING features. It extracts the first and last
    coordinates for each feature. These coordinates are used to create new nodes at which
    a new link is created for each feature. Nodes within the node merge distance are assumed
    to be the same node and merged together.
    """
    client = get_logged_in_client(obj, user_id=user_id)

    import_links_from_shapefile(client, filename, network_id, node_template_type_id,
                                link_template_type_id, node_merge_distance=node_merge_distance)


@hydra_app(category='network_utility', name='Import nodes from GIS')
@cli.command(name='import-nodes')
@click.pass_obj
@click.option('--filename', type=click.Path(file_okay=True, dir_okay=False))
@click.option('-n', '--network-id', type=int, default=None)
@click.option('-a', '--node-name-attribute', type=str, default=None, multiple=True)
@click.option('--node-template-type-id', type=int, default=None)
@click.option('-u', '--user-id', type=int, default=None)
def import_nodes(obj, filename, network_id, user_id, node_template_type_id, node_name_attribute):
    """Import nodes from a GIS file.

    This app searches a GIS file for POINT, POLYGON or MULTIPOLYGON features. It creates a new
    node for each of these features. For polygon or multi-polygon features a representative
    point is used for the coordinate of the node.
    """
    client = get_logged_in_client(obj, user_id=user_id)

    nodes, projection = import_nodes_from_shapefile(filename, node_template_type_id,
                                                    name_attributes=node_name_attribute)

    client.add_nodes(network_id, nodes)


@hydra_app(category='import', name='Create network from GIS.')
@cli.command(name='create-network')
@click.pass_obj
@click.option('--filename', type=click.Path(file_okay=True, dir_okay=False))
@click.option('-p', '--project-id', type=int)
@click.option('--name', type=str, default=None)
@click.option('-u', '--user-id', type=int, default=None)
@click.option('--node-name-attribute', type=str, default=None)
@click.option('--node-template-type-id', type=int, default=None)
@click.option('--network-template-type-id', type=int, default=None)
def import_network(obj, filename, project_id, name, user_id, node_template_type_id,
                   network_template_type_id, node_name_attribute):
    """Create a new network from a GIS file.

    This app searches a GIS file for POINT, POLYGON or MULTIPOLYGON features. It creates a new
    node for each of these features. For polygon or multi-polygon features a representative
    point is used for the coordinate of the node. These nodes are added to a new network. The
    app creates no links between the nodes.
    """
    client = get_logged_in_client(obj, user_id=user_id)

    nodes, projection = import_nodes_from_shapefile(filename, node_template_type_id,
                                                    name_attributes=[node_name_attribute])

    if name is None:
        name, _ = os.path.splitext(os.path.basename(filename))

    network = {
        "name": name,
        "description": "",
        "project_id": project_id,
        "links": [],
        "nodes": nodes,
        "layout": None,
        "scenarios": [],
        "projection": projection,
        "attributes": [],
        'types': [{'id': network_template_type_id}]
    }

    client.add_network(network)

@hydra_app(category='network_utility', name='Export Coordinates')
@cli.command(name='export-coordinates')
@click.pass_obj
@click.option('-n', '--network-id', type=int, default=None, multiple=True)
@click.option('--data-dir', default='/tmp')
@click.option('-u', '--user-id', type=int, default=None)
def export_coordinates(obj, network_id, data_dir, user_id):
    """Apply layouts from JSON file to network."""
    client = get_logged_in_client(obj, user_id=user_id)

    if not hasattr(network_id, '__iter__'):
        network_id = [network_id]
    topology.export_coordinates(client, network_id, data_dir)

    print("Done exporting coordinates")

@hydra_app(category='network_utility', name='Apply Coordinates')
@cli.command(name='apply-coordinates')
@click.pass_obj
@click.option('--filename', type=click.Path(file_okay=True, dir_okay=False))
@click.option('-n', '--network-id', type=int, default=None, multiple=True)
@click.option('-u', '--user-id', type=int, default=None)
def apply_coordinates(obj, filename, network_id, user_id):
    """Apply layouts from JSON file to network."""
    client = get_logged_in_client(obj, user_id=user_id)

    if not hasattr(network_id, '__iter__'):
        network_id = [network_id]

    topology.apply_coordinates(client, filename, network_id)

    print("Done applying coordinates")

@hydra_app(category='network_utility', name='Apply Coordinates')
@cli.command(name='copy-coordinates')
@click.pass_obj
@click.option('-n1', '--from-network-id', type=int, default=None, multiple=True)
@click.option('-n2', '--to-network-id', type=int, default=None, multiple=True)
@click.option('-u', '--user-id', type=int, default=None)
def apply_coordinates(obj, from_network_id, to_network_id, user_id):
    """Apply layouts from JSON file to network."""
    client = get_logged_in_client(obj, user_id=user_id)

    topology.copy_coordinates(client, from_network_id, to_network_id)

    print("Done copying coordinates")


@hydra_app(category='network_utility', name='Apply Layouts')
@cli.command(name='apply-layouts')
@click.pass_obj
@click.option('--filename', type=click.Path(file_okay=True, dir_okay=False))
@click.option('-n', '--network-id', type=int, default=None)
@click.option('-u', '--user-id', type=int, default=None)
def apply_layouts(obj, filename, network_id, user_id):
    """Take a CSV file containing 3 columns: Name, Lat, Long and scan through the network's nodes
        to pick out any matching node names. If it finds one, set the x to lat and the y to long
    """
    client = get_logged_in_client(obj, user_id=user_id)


    topology.apply_layouts(client, filename, network_id)

@hydra_app(category='network_utility', name='Import dataframes from Excel')
@cli.command(name='import-dataframe-excel')
@click.pass_obj
@click.option('--filename', type=click.Path(file_okay=True, dir_okay=False))
@click.option('--column', type=str, default=None)
@click.option('--sheet-name', type=str, default=0)
@click.option('--index-col', type=str, default=0)
@click.option('--data-type', type=str, default='DATAFRAME')
@click.option('--create-new/--no-create-new', default=False)
@click.option('--overwrite/--no-overwrite', default=False)
@click.option('-n', '--network-id', type=int, default=None)
@click.option('-s', '--scenario-id', type=int, default=None)
@click.option('-a', '--attribute-id', type=int, default=None)
@click.option('-u', '--user-id', type=int, default=None)
def import_dataframe_excel(obj, filename, column, sheet_name, index_col, data_type,
                           create_new, overwrite,
                           network_id, scenario_id, attribute_id, user_id):
    """Import dataframes from Excel."""

    client = get_logged_in_client(obj, user_id=user_id)


    if filename.endswith('csv'):
        dataframe = pandas.read_csv(filename, index_col=index_col, parse_dates=True)
    elif filename.endswith('xlsx') or filename.endswith('xls'):
        dataframe = pandas.read_excel(filename, sheet_name=sheet_name, index_col=index_col, parse_dates=True)
        if isinstance(dataframe, dict):
            dataframe = list(dataframe.values())[0]
    else:
        raise Exception("Unrecognised file extention. Must be csv or xlsx.")

    data.import_dataframe(client, dataframe, network_id, scenario_id, attribute_id, column,
                          create_new=create_new, data_type=data_type, overwrite=overwrite)


@hydra_app(category='network_utility', name='Import dataframes from CSV')
@cli.command(name='import-dataframe-csv')
@click.pass_obj
@click.option('--filename', type=click.Path(file_okay=True, dir_okay=False))
@click.option('--column', type=str, default=None)
@click.option('--index-col', type=str, default=None)
@click.option('--create-new/--no-create-new', default=False)
@click.option('--overwrite/--no-overwrite', default=False)
@click.option('-n', '--network-id', type=int, default=None)
@click.option('-s', '--scenario-id', type=int, default=None)
@click.option('-a', '--attribute-id', type=int, default=None)
@click.option('-u', '--user-id', type=int, default=None)
@click.option('-u', '--user-id', type=int, default=None)
def import_dataframe_csv(obj, filename, column, index_col, create_new, overwrite,
                         network_id, scenario_id, attribute_id, user_id, ):
    """Import dataframes from CSV."""
    client = get_logged_in_client(obj, user_id=user_id)
    dataframe = pandas.read_csv(filename, index_col=index_col, parse_dates=True)
    data.import_dataframe(client,
                          dataframe,
                          network_id,
                          scenario_id,
                          attribute_id,
                          column,
                          create_new=create_new,
                          overwrite=overwrite)


@hydra_app(category='network_utility', name='Export dataframes to Excel')
@cli.command(name='export-dataframes-excel')
@click.pass_obj
@click.option('-n', '--network-id', type=int, default=None)
@click.option('-s', '--scenario-id', type=int, default=None)
@click.option('-a', '--attribute-id', type=int, default=None)
@click.option('-u', '--user-id', type=int, default=None)
@click.option('--data-dir', default='/tmp')
def export_dataframes_excel(obj, network_id, scenario_id, attribute_id, user_id, data_dir):
    """Export dataframes to Excel."""
    client = get_logged_in_client(obj, user_id=user_id)

    attribute_ids = None
    if attribute_id is not None:
        attribute_ids = [attribute_id]

    dataframes = defaultdict(dict)
    for node_name, attr_name, df in data.export_dataframes(client, network_id, scenario_id, attribute_ids=attribute_ids):
        dataframes[attr_name][node_name] = df

    # TODO make the filename configurable or based on the network name
    fn = os.path.join(data_dir, 'export.xlsx')
    writer = pandas.ExcelWriter(fn)
    for key, dfs in dataframes.items():
        df = pandas.concat(dfs, axis=1)
        # replace non-alphanumeric characters with underscore
        sheet_name = re.sub('[^0-9a-zA-Z|+\-@#$^()_,.!]+', '_', key)
        df.to_excel(writer, sheet_name=sheet_name)
    writer.save()

@hydra_app(category='network_utility', name='Combine dataframes from multiple networks at once')
@cli.command(name='assemble-dataframes')
@click.pass_obj
@click.option('-a', '--resource-attribute-ids', type=int, default=None, multiple=True)
@click.option('-s', '--scenario-id', type=int, default=None)
@click.option('-t', '--source-scenario-ids', type=int, default=None, multiple=True)
@click.option('-u', '--user-id', type=int, default=None)
def assemble_dataframes(obj, resource_attribute_ids, scenario_id, source_scenario_ids, user_id):
    """
        Create a single data frame into a resource attribute by finding
        equivalent resource attributes on other specified networks (identified through
        scenario IDS)
    """
    client = get_logged_in_client(obj, user_id=user_id)

    data.assemble_dataframes(client, resource_attribute_ids, scenario_id, source_scenario_ids)

@hydra_app(category='network_utility', name='Combine dataframes from multiple networks at once')
@cli.command(name='un-hide-nodes')
@click.pass_obj
@click.option('-n', '--network-id', type=int, default=None)
@click.option('--name', type=str)
@click.option('-u', '--user-id', type=int, default=None)
def un_hide_nodes(obj, network_id, name, user_id):
    """
        Remove the specified flag from all nodes in a network
    """
    client = get_logged_in_client(obj, user_id=user_id)

    net = client.get_network(network_id)

    label = name

    for node in net.nodes:
        print(node.layout)
        if node.layout is None:
            continue
        layout = node.layout
        if isinstance(layout, str):
            layout = json.loads(layout)
        if layout.get(label) is None:
            continue

        del(layout[name])
        node['layout'] = layout
        print(f"Layout changed on node {node.name} ({node.id})")
        client.update_node(node)
        print(f"Node {node.name} ({node.id}) Updated.")

@hydra_app(category='network_utility', name='Set a the hidden flag on all the types in a template')
@cli.command(name='unset-type-layout')
@click.pass_obj
@click.option('-t', '--template-id', type=int, default=None)
@click.option('--name', type=str)
@click.option('-u', '--user-id', type=int, default=None)
def unset_type_layout(obj, template_id, name, user_id):
    """
        Remove the specified flag from all types in a template
    """
    client = get_logged_in_client(obj, user_id=user_id)


    template = client.get_template(template_id)

    label = name

    for tt in template.templatetypes:
        print(tt.layout)
        if tt.layout is None:
            continue
        layout = tt.layout
        if isinstance(layout, str):
            layout = json.loads(layout)
        if layout.get(label) is None:
            continue

        del(layout[label])
        tt['layout'] = layout
        print(f"Layout changed on tt {tt.name} ({tt.id})")
        client.update_templatetype(tt)
        print(f"tt {tt.name} ({tt.id}) Updated.")

@cli.command()
@click.pass_obj
@click.argument('docker-image', type=str)
def register(obj, docker_image):
    """ Register the app with the Hydra installation. """
    plugins = make_plugins(cli, 'hydra-network-utils', docker_image=docker_image)
    app_name = docker_image.replace('/', '-').replace(':', '-')
    write_plugins(plugins, app_name)

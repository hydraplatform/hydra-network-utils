import click
import os
from hydra_client.connection import JSONConnection
from hydra_client.click import hydra_app, make_plugins, write_plugins
import json


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


@hydra_app(category='network-util')
@cli.command(name='import-links')
@click.pass_obj
@click.argument('filename', type=click.Path(file_okay=True, dir_okay=False))
@click.option('-n', '--network-id', type=int, default=None)
@click.option('--node-template-type-id', type=int, default=None)
@click.option('--link-template-type-id', type=int, default=None)
@click.option('--node-merge-distance', type=float, default=None)
@click.option('-u', '--user-id', type=int, default=None)
def import_links(obj, filename, network_id, user_id, node_template_type_id,
                     link_template_type_id, node_merge_distance):

    from .gis import import_links_from_shapefile

    client = get_logged_in_client(obj, user_id=user_id)

    import_links_from_shapefile(client, filename, network_id, node_template_type_id,
                                link_template_type_id, node_merge_distance=node_merge_distance)


@hydra_app(category='network-util')
@cli.command(name='import-nodes')
@click.pass_obj
@click.argument('filename', type=click.Path(file_okay=True, dir_okay=False))
@click.option('-n', '--network-id', type=int, default=None)
@click.option('-a', '--node-name-attribute', type=str, default=None, multiple=True)
@click.option('--node-template-type-id', type=int, default=None)
@click.option('-u', '--user-id', type=int, default=None)
def import_nodes(obj, filename, network_id, user_id, node_template_type_id, node_name_attribute):

    from .gis import import_nodes_from_shapefile

    client = get_logged_in_client(obj, user_id=user_id)

    print(node_name_attribute)
    nodes, projection = import_nodes_from_shapefile(filename, node_template_type_id,
                                                    name_attributes=node_name_attribute)

    client.add_nodes(network_id, nodes)


@hydra_app(category='import')
@cli.command(name='create-network')
@click.pass_obj
@click.argument('filename', type=click.Path(file_okay=True, dir_okay=False))
@click.argument('project_id', type=int)
@click.option('--name', type=str, default=None)
@click.option('-u', '--user-id', type=int, default=None)
@click.option('--node-name-attribute', type=str, default=None)
@click.option('--node-template-type-id', type=int, default=None)
@click.option('--network-template-type-id', type=int, default=None)
def import_network(obj, filename, project_id, name, user_id, node_template_type_id,
                   network_template_type_id, node_name_attribute):
    from .gis import import_nodes_from_shapefile
    client = get_logged_in_client(obj, user_id=user_id)

    nodes, projection = import_nodes_from_shapefile(filename, node_template_type_id,
                                                    name_attribute=node_name_attribute)

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
        "projection": 'EPSG:27700',
        "attributes": [],
        'types': [{'id': network_template_type_id}]
    }

    client.add_network(network)


@hydra_app(category='network-util')
@cli.command(name='apply-layouts')
@click.pass_obj
@click.argument('filename', type=click.Path(file_okay=True, dir_okay=False))
@click.option('-n', '--network-id', type=int, default=None)
@click.option('-u', '--user-id', type=int, default=None)
def apply_layouts(obj, filename, network_id, user_id):

    client = get_logged_in_client(obj, user_id=user_id)

    # Open the layouts
    with open(filename) as fh:
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


@cli.command()
@click.pass_obj
@click.argument('docker-image', type=str)
def register(obj, docker_image):
    """ Register the app with the Hydra installation. """
    plugins = make_plugins(cli, 'hydra-network-utils', docker_image=docker_image)
    app_name = docker_image.replace('/', '-').replace(':', '-')
    write_plugins(plugins, app_name)


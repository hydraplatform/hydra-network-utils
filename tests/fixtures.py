

import hydra_base

from hydra_base.db import DeclarativeBase as _db
from hydra_base.util.hdb import create_default_users_and_perms, make_root_user, create_default_units_and_dimensions
from hydra_base.util import testing

from hydra_client.connection import JSONConnection

import sqlite3
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import six

import datetime

import logging
log = logging.getLogger(__name__)

@pytest.fixture(scope='function')
def client(testdb_uri, session):
    client = JSONConnection(app_name='Hydra Network Utilities App', session=hydra_base.db.DBSession)
    #fake a login using the test's session
    client.user_id = 1
    return client


def pytest_namespace():
    return {'root_user_id': 1}

@pytest.fixture()
def dateformat():
    return hydra_base.config.get('DEFAULT', 'datetime_format', "%Y-%m-%dT%H:%M:%S.%f000Z")


@pytest.fixture()
def testdb_uri(db_backend, tmpdir):
    if db_backend == 'sqlite':
        # Use a :memory: database for the tests.
        return 'sqlite:///{}/test.db'.format(tmpdir)
    elif db_backend == 'postgres':
        # This is designed to work on Travis CI
        return 'postgresql://postgres@localhost:5432/hydra_base_test'
    elif db_backend == 'mysql':
        return 'mysql+mysqldb://root@localhost/hydra_base_test'
    else:
        raise ValueError('Database backend "{}" not supported when running the tests.'.format(db_backend))


@pytest.fixture(scope='function')
def engine(testdb_uri):
    engine = create_engine(testdb_uri, encoding='utf-8')
    return engine


@pytest.fixture(scope='function')
def db(engine, request):
    """ Test database """
    _db.metadata.create_all(engine)
    return _db


@pytest.fixture(scope='function')
def session(client, db, engine, request):
    """Creates a new database session for a test."""

    db.metadata.bind = engine

    DBSession = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    # A DBSession() instance establishes all conversations with the database
    # and represents a "staging zone" for all the objects loaded into the
    # database session object. Any change made against the objects in the
    # session won't be persisted into the database until you call
    # session.commit(). If you're not happy about the changes, you can
    # revert all of them back to the last commit by calling
    session = DBSession()

    # Patch the global session in hydra_base
    hydra_base.db.DBSession = session

    if six.PY2 and isinstance(session.connection().connection.connection,sqlite3.Connection):
        session.connection().connection.connection.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')

    # Now apply the default users and roles
    #hydra_base.db.DBSession.begin_nested()
    create_default_users_and_perms()

    root_user_id = make_root_user()

    create_default_units_and_dimensions()

    pytest.root_user_id = root_user_id
    client.testutils = testing.TestUtil(client)
    pytest.user_a = client.testutils.create_user("UserA")
    pytest.user_b = client.testutils.create_user("UserB")
    pytest.user_c = client.testutils.create_user("UserC", role='developer')

    # Tear down the session
    #???
    hydra_base.db.close_session()
    # First make sure everything can be and is committed.
    try:
        session.commit()
        # Finally drop all the tables.
        hydra_base.db.DeclarativeBase.metadata.drop_all()
    except:
        session.rollback()


@pytest.fixture()
def networkmaker(client):
    class NetworkMaker:
        def __init__(self, client):
            self.client = client
        def create(self, project_id=None, num_nodes=10, ret_full_net=True, new_proj=True, map_projection='EPSG:4326'):
            return self.client.testutils.create_network_with_data(project_id, num_nodes, ret_full_net, new_proj, map_projection)
    return NetworkMaker()


@pytest.fixture()
def projectmaker(client):
    class ProjectMaker:
        def __init__(self, client):
            self.client = client
        def create(self, name=None, share=True):
            if name is None:
                name = 'Project %s' % (datetime.datetime.now())
            return self.client.testutils.create_project(name=name, share=share)

    return ProjectMaker()

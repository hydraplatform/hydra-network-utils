
def get_hostname():
    return 'http://localhost:8080'

@pytests.fixture(scope='function')
def client(username='root', password=''):
    client = JSONConnection(app_name='Hydra Network Utilities App', db_url=get_hostname(), **kwargs)
    client.login(username=username, password=password)
    return client

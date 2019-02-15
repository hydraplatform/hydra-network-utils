#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name='hydra-network-utils',
    version='0.1',
    description='General utilities for interacting with networks in Hydra.',
    packages=find_packages(),
    entry_points='''
    [console_scripts]
    hydra-network-utils=hydra_network_utils.cli:start_cli
    ''',
)

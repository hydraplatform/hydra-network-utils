#!/usr/bin/env python
# -*- coding: utf-8 -*-

from fixtures import *
from hydra_network_utils import data
import pytest
import pandas as pd

class TestAssembleDataframes:
    def test_assemble_dataframes(self, session, client, projectmaker, networkmaker):
        """
        """

        project = projectmaker.create('Update COordinate Project')

        target_network = networkmaker.create(project_id=project.id)

        print([n.name for n in target_network.nodes])        
        

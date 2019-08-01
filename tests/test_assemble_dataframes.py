#!/usr/bin/env python
# -*- coding: utf-8 -*-

from fixtures import *
from hydra_base.util.testing import create_dataframe
from hydra_network_utils import data
import pytest

class TestAssembleDataframes:
    def test_assemble_dataframes(self, session, client, projectmaker, networkmaker):
        """
        """

        project = projectmaker.create('Assemble Project')

        target_network = networkmaker.create(project_id=project.id)

        source_network_1 = networkmaker.create(project_id=project.id)
        
        source_scenario_1 = source_network_1.scenarios[0]

        source_ra_1 = source_network_1.nodes[0].attributes[0]

        source_df_1 = create_dataframe(source_ra_1, 
                                       dataframe_value = {"test_column": 
                                                            {
                                                                'key1': 1,
                                                                'key2': 2,
                                                                'key3': 3
                                                            }
                                                         }
                                      )

        hydra_base.update_resourcedata(source_scenario_1.id, [source_df_1], user_id=pytest.root_user_id)
        
        source_network_2 = networkmaker.create(project_id=project.id)
        
        source_scenario_2 = source_network_2.scenarios[0]

        source_ra_2 = source_network_2.nodes[0].attributes[0]

        source_df_2 = create_dataframe(source_ra_2, 
                                       dataframe_value = {"test_column": 
                                                            {
                                                                'key1': 1,
                                                                'key2': 2,
                                                                'key3': 3
                                                            }
                                                         }
                                      )

        hydra_base.update_resourcedata(source_scenario_2.id, [source_df_2], user_id=pytest.root_user_id)

        
        target_ra = target_network.nodes[0].attributes[0]
        target_scenario = target_network.scenarios[0]

        combined_dataframe = data.assemble_dataframes(client,
                                 target_ra.id,
                                 target_scenario.id,
                                 [source_scenario_1.id, source_scenario_2.id])

        updated_scenario = client.get_scenario(target_scenario.id)

        for rs in updated_scenario.resourcescenarios:
            if rs.resource_attr_id == target_ra.id:
                assert rs.dataset.value == combined_dataframe.value

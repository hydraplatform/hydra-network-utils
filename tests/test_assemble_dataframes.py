#!/usr/bin/env python
# -*- coding: utf-8 -*-

from hydra_base.tests.fixtures import *
from fixtures import *
from hydra_base.tests.util import create_dataframe
from hydra_network_utilities import data
import pytest

class TestAssembleDataframes:
    def test_assemble_dataframes(self, client, projectmaker, networkmaker):
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

        hc.update_resorucedata(source_scenario_1.id, [source_df_1], user_id=pytest.root_user_id)
        
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

        hc.update_resorucedata(source_scenario_2.id, [source_df_2], user_id=pytest.root_user_id)



        matching_rs_list = data.get_matching_resource_scenarios(client,
                                                       resource_attribute_id,
                                                       scenario_id,
                                                       source_scenario_ids)

        dataframes = data.extract_dataframes(rs_list)

        combined_dataframe = data.combine_dataframes(dataframes)

        data.update_resource_scenario(resource_attribute_id, scenario_id, resource_attr_id)


        #Assemble the dataframes
        
        matching_rs_list = data.get_matching_resource_scenarios(client,
                                                       resource_attribute_id,
                                                       scenario_id,
                                                       source_scenario_ids)

        dataframes = data.extract_dataframes(rs_list)

        combined_dataframe = data.combine_dataframes(dataframes)

        data.update_resource_scenario(resource_attribute_id, scenario_id, resource_attr_id)




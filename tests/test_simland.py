#!/usr/bin/python
"""
Unit tests for peacekeeping.

"""
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from simland import Simland


class TestSimland:
    dataset = {
        "title": "Test - Subnational Population Statistics",
        "name": "cod-ps-test",
        "notes": "Afghanistan administrative levels 0 (country), and 1 (province) population statistics. REFERENCE YEAR: 2021",
        "dataset_source": "National Statistic and Information Authority (NSIA) Afghanistan",
        "methodology": "Other",
        "methodology_other": "Based on micro-census and remote sensing data.",
        "caveats": "Population figures from the original data have been rounded off to the nearest integer.",
        "maintainer": "84e567b6-1d09-4f7e-96f5-b69c09028cbc",
        "owner_org": "b3a25ac4-ac05-4991-923c-d25f47bef1ec",
        "data_update_frequency": "365",
        "subnational": "1",
        "cod_level": "cod-standard",
        "groups": [{"name": "afg"}],
        "tags": [
            {"name": "baseline population", "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1"}
        ],
        "dataset_date": "[2024-01-01T00:00:00 TO 2024-12-31T23:59:59]",
    }
    resource = {
        "name": "afg_admpop_adm1_2021_v2.csv",
        "description": "2021 population estimates for Afghanistan administrative level 1 (province).",
        "format": "csv",
        "url": "https://data.humdata.org/dataset/17acb541-9431-409a-80a8-50eda7e8ebab/resource/dc7a5656-d557-404f-8b1d-494c7bbd0112/download/afg_admpop_adm1_2021_v2.csv",
        "resource_type": "api",
        "url_type": "api",
    }

    @pytest.fixture(scope="function")
    def fixtures(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=join("config", "project_configuration.yaml"),
        )
        UserAgent.set_global("test")
        tags = (
            "baseline population",
            "administrative boundaries-divisions",
        )
        Vocabulary._tags_dict = {tag: {"Action to Take": "ok"} for tag in tags}
        tags = [{"name": tag} for tag in tags]
        Vocabulary._approved_vocabulary = {
            "tags": tags,
            "id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            "name": "approved",
        }
        Locations.set_validlocations(
            [
                {"name": "afg", "title": "Afghanistan"},
            ]
        )
        Country.countriesdata(use_live=False)
        return Configuration.read()

    def test_generate_dataset(self, configuration, fixtures):
        with temp_dir(
            "test_simland", delete_on_success=True, delete_on_failure=False
        ) as folder:
            with Download() as downloader:
                retriever = Retrieve(downloader, folder, fixtures, folder, False, True)
                simland = Simland(configuration, retriever, folder, ErrorsOnExit())
                dataset_names = simland.get_data()
                assert dataset_names == [{"name": "cod-ps-test"}]

                dataset = simland.generate_dataset("cod-ps-test")
                assert dataset == self.dataset
                resources = dataset.get_resources()
                assert resources[0] == self.resource

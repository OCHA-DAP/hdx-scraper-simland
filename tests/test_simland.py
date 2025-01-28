from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from src.hdx.scraper.simland.simland import Simland


class TestSimland:
    dataset = {
        "title": "Test - Subnational Population Statistics",
        "name": "cod-ps-test",
        "notes": "Afghanistan administrative levels 0 (country), and 1 (province) "
        "population statistics. REFERENCE YEAR: 2021",
        "dataset_source": "National Statistic and Information Authority (NSIA) "
        "Afghanistan",
        "methodology": "Other",
        "methodology_other": "Based on micro-census and remote sensing data.",
        "caveats": "Population figures from the original data have been rounded off to "
        "the nearest integer.",
        "maintainer": "9429fda5-d84f-42e4-890d-e03bf8297f7b",
        "owner_org": "b3a25ac4-ac05-4991-923c-d25f47bef1ec",
        "data_update_frequency": "365",
        "subnational": "1",
        "cod_level": "cod-standard",
        "groups": [{"name": "afg"}],
        "tags": [
            {
                "name": "baseline population",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            }
        ],
        "dataset_date": "[2024-01-01T00:00:00 TO 2024-12-31T23:59:59]",
    }
    resource = {
        "name": "afg_admpop_adm1_2021_v2.csv",
        "description": "2021 population estimates for Afghanistan administrative level 1 "
        "(province).",
        "format": "csv",
        "resource_type": "file.upload",
        "url_type": "upload",
    }

    @pytest.fixture(scope="function")
    def configuration(self, config_dir):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join(config_dir, "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "simland", "config")

    def test_simland(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "test_simland", delete_on_success=True, delete_on_failure=False
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                simland = Simland(configuration, retriever, ErrorsOnExit())
                dataset_names = simland.get_data()
                assert dataset_names == [{"name": "cod-ps-test"}]

                dataset = simland.generate_dataset("cod-ps-test")
                assert dataset == self.dataset
                resources = dataset.get_resources()
                assert resources[0] == self.resource

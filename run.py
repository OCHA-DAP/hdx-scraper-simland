#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import progress_storing_folder, wheretostart_tempdir_batch
from hdx.utilities.retriever import Retrieve

from simland import Simland

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-simland"
updated_by_script = "HDX Scraper: Simland"


def main(save: bool = False, use_saved: bool = False) -> None:
    """Generate datasets and create them in HDX"""

    with ErrorsOnExit() as errors:
        with wheretostart_tempdir_batch(lookup) as info:
            folder = info["folder"]
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, "saved_data", folder, save, use_saved
                )
                folder = info["folder"]
                batch = info["batch"]
                configuration = Configuration.read()
                simland = Simland(configuration, retriever, folder, errors)
                dataset_names = simland.get_data()
                logger.info(f"Number of datasets to upload: {len(dataset_names)}")

                for _, nextdict in progress_storing_folder(info, dataset_names, "name"):
                    dataset_name = nextdict["name"]
                    if dataset_name == "cod-ps-test":
                        continue
                    dataset = simland.generate_dataset(dataset_name)
                    if dataset:
                        dataset.update_from_yaml()
                        dataset["notes"] = dataset["notes"].replace(
                            "\n", "  \n"
                        )  # ensure markdown has line breaks
                        try:
                            dataset.create_in_hdx(
                                remove_additional_resources=True,
                                hxl_update=False,
                                updated_by_script=updated_by_script,
                                batch=batch,
                                skip_validation=True,
                                ignore_check=True,
                            )
                        except HDXError as error:
                            errors.add(f"Could not upload {dataset_name}")
                            continue


if __name__ == "__main__":
    facade(
        main,
        hdx_url="https://blue.demo.data-humdata-org.ahconu.org",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yaml")
    )

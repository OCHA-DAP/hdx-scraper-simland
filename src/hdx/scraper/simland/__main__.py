#!/usr/bin/python
"""simland scraper"""

import argparse
import logging
from os.path import dirname, expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import (
    progress_storing_folder,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.simland.simland import Simland

logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-simland"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: Simland"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-rm", "--review_mode", default=False, action="store_true", help="Review mode"
    )
    args = parser.parse_args()
    return args


def main(
    review_mode: bool, save: bool = False, use_saved: bool = False, **ignore
) -> None:
    """Generate datasets and create them in HDX"""

    with ErrorsOnExit() as errors:
        with wheretostart_tempdir_batch(folder=_USER_AGENT_LOOKUP) as info:
            temp_dir = info["folder"]
            with Download() as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=temp_dir,
                    saved_dir="saved_data",
                    temp_dir=temp_dir,
                    save=save,
                    use_saved=use_saved,
                )
                configuration = Configuration.read()
                simland = Simland(configuration, retriever, errors, review_mode)
                dataset_names = simland.get_data()
                logger.info(f"Number of datasets to upload: {len(dataset_names)}")

                for _, nextdict in progress_storing_folder(info, dataset_names, "name"):
                    dataset_name = nextdict["name"]
                    if dataset_name == "cod-ps-test":
                        continue
                    dataset = simland.generate_dataset(dataset_name)
                    if dataset:
                        dataset.update_from_yaml(
                            path=join(
                                dirname(__file__), "config", "hdx_dataset_static.yaml"
                            )
                        )
                        dataset["notes"] = dataset["notes"].replace(
                            "\n", "  \n"
                        )  # ensure markdown has line breaks
                        try:
                            dataset.create_in_hdx(
                                remove_additional_resources=True,
                                hxl_update=False,
                                updated_by_script=_UPDATED_BY_SCRIPT,
                                batch=info["batch"],
                                skip_validation=True,
                                ignore_check=True,
                            )
                        except HDXError:
                            errors.add(f"Could not upload {dataset_name}")
                            continue


if __name__ == "__main__":
    args = parse_args()
    review_mode = args.review_mode
    if review_mode:
        facade(
            main,
            hdx_site="dev",
            user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
            user_agent_lookup=_USER_AGENT_LOOKUP,
            project_config_yaml=join(
                dirname(__file__), "config", "project_configuration.yaml"
            ),
            review_mode=review_mode,
        )
    else:
        facade(
            main,
            hdx_url="https://blue.demo.data-humdata-org.ahconu.org",
            user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
            user_agent_lookup=_USER_AGENT_LOOKUP,
            project_config_yaml=join(
                dirname(__file__), "config", "project_configuration.yaml"
            ),
            review_mode=review_mode,
        )

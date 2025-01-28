#!/usr/bin/python
"""
Simland:
------------

Reads Simland inputs and creates datasets.

"""

import logging
from typing import Dict, List, Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.utilities.dictandlist import dict_of_dicts_add
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Simland:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        errors: ErrorsOnExit,
        review_mode: bool = False,
    ):
        self.configuration = configuration
        self.retriever = retriever
        self.errors = errors
        self.review_mode = review_mode
        self.metadata = {}

    def get_data(self, datasets: Optional[List] = None) -> List[Dict[str, str]]:
        base_url = self.configuration["metadata_url"]
        _, iterator = self.retriever.get_tabular_rows(
            base_url, format="csv", dict_form=True
        )

        for row in iterator:
            dataset_id = row["Dataset"]
            if datasets and dataset_id not in datasets:
                continue
            dict_of_dicts_add(self.metadata, dataset_id, row["Field"], row["Value"])

        return [{"name": dataset_name} for dataset_name in sorted(self.metadata)]

    def generate_dataset(self, dataset_name: str) -> Dataset | None:
        metadata = self.metadata[dataset_name]

        dataset = Dataset(
            {
                "title": metadata["title"],
                "name": dataset_name,
                "notes": metadata["notes"],
                "dataset_source": metadata["dataset_source"],
                "methodology": metadata["methodology"],
                "caveats": metadata["caveats"],
            }
        )
        if metadata["methodology"] == "Other":
            dataset["methodology_other"] = metadata["methodology_other"]

        dataset.set_maintainer("9429fda5-d84f-42e4-890d-e03bf8297f7b")
        if metadata["organization"] == "OCHA Field Information Services Section (FISS)":
            dataset.set_organization("b3a25ac4-ac05-4991-923c-d25f47bef1ec")
        elif metadata["organization"] == "UNFPA":
            dataset.set_organization("95aa8d05-b110-4607-9330-f2a779885493")
        else:
            self.errors.add(f"Could not find organization for {dataset_name}")
            return None

        dataset.set_expected_update_frequency(metadata["data_update_frequency"])
        dataset.set_subnational(True)
        locations = {
            "eastland": "etl",
            "northland": "ntl",
            "simland": "sld",
            "southland": "stl",
            "westland": "wtl",
        }
        location = metadata["groups"].lower()
        location = locations.get(location, location)
        if self.review_mode:
            dataset.add_country_location("can")
        else:
            dataset.add_other_location(location)

        tags = metadata["tags"]
        if tags:
            tags = tags.split(",")
            tags = [t.rstrip() for t in tags]
            dataset.add_tags(tags)

        dataset.set_time_period_year_range(2024, 2024)
        if metadata["cod_level"]:
            dataset["cod_level"] = metadata["cod_level"]

        resources = list()
        resource_dict = dict()
        for key in metadata:
            if key.split("_")[0] == "resource":
                resource_name = "_".join(key.split("_")[:2])
                resource_item = "_".join(key.split("_")[2:])
                dict_of_dicts_add(
                    resource_dict, resource_name, resource_item, metadata[key]
                )
        for key in resource_dict:
            if resource_dict[key]["format"] == "Geoservice":
                continue
            if resource_dict[key]["format"] == "CSV":
                filepath = self.retriever.download_file(
                    url=resource_dict[key]["url"],
                    filename=resource_dict[key]["name"],
                )
                resource_dict[key].pop("url")
            resource = Resource(resource_dict[key])
            if resource_dict[key]["format"] == "CSV":
                resource.set_file_to_upload(filepath)
            resources.append(resource)

        try:
            dataset.add_update_resources(resources)
        except HDXError as ex:
            self.errors.add(
                f"Dataset: {dataset['name']} resources could not be added. Error: {ex}"
            )

        return dataset

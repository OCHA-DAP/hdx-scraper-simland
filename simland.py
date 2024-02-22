#!/usr/bin/python
"""
Simland:
------------

Reads Simland inputs and creates datasets.

"""
import logging

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.utilities.dictandlist import dict_of_dicts_add

logger = logging.getLogger(__name__)


class Simland:
    def __init__(self, configuration, retriever, folder, errors):
        self.configuration = configuration
        self.retriever = retriever
        self.folder = folder
        self.errors = errors
        self.metadata = {}

    def get_data(self):
        base_url = self.configuration["metadata_url"]
        _, iterator = self.retriever.get_tabular_rows(base_url, format="csv", dict_form=True)

        for row in iterator:
            dataset_id = row["Dataset"]
            dict_of_dicts_add(self.metadata, dataset_id, row["Field"], row["Value"])

        return [{"name": dataset_name} for dataset_name in sorted(self.metadata)]

    def generate_dataset(self, dataset_name):
        metadata = self.metadata[dataset_name]

        dataset = Dataset(
            {
                "title": metadata["title"],
                "name": dataset_name,
                "notes": metadata["notes"],
                "dataset_source": metadata["dataset_source"],
                "methodology": metadata["methodology"],
                "methodology_other": metadata["methodology_other"],
                "caveats": metadata["caveats"],
                "license_title": metadata["license_title"],
            }
        )

        dataset.set_maintainer("84e567b6-1d09-4f7e-96f5-b69c09028cbc")
        dataset.set_organization("b3a25ac4-ac05-4991-923c-d25f47bef1ec")
        dataset.set_expected_update_frequency(metadata["data_update_frequency"])
        dataset.set_subnational(True)
        dataset.add_country_locations(metadata["groups"].split(","))

        theme = dataset_name[:6]
        if theme == "cod-ps":
            tags = ["baseline population"]
        if theme in ["cod-ab", "cod-em"]:
            tags = ["administrative boundaries-divisions"]
        dataset.add_tags(tags)

        start_date = metadata["dataset_start_date"]
        end_date = metadata["dataset_end_date"]
        ongoing = True
        if end_date:
            ongoing = False
        dataset.set_time_period(start_date, end_date, ongoing)

        resources = list()
        resource_dict = dict()
        for key in metadata:
            if key.split("_")[0] == "resource":
                resource_name = "_".join(key.split("_")[:2])
                resource_item = "_".join(key.split("_")[2:])
                dict_of_dicts_add(resource_dict, resource_name, resource_item, metadata[key])
        for key in resource_dict:
            resource = Resource(resource_dict[key])
            resources.append(resource)

        try:
            dataset.add_update_resources(resources)
        except HDXError as ex:
            self.errors.add(f"Dataset: {dataset['name']} resources could not be added. Error: {ex}")

        return dataset

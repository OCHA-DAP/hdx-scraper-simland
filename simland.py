#!/usr/bin/python
"""
Simland:
------------

Reads Simland inputs and creates datasets.

"""
import logging

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.dictandlist import dict_of_dicts_add
from slugify import slugify

logger = logging.getLogger(__name__)


class Simland:
    def __init__(self, configuration, retriever, folder, errors):
        self.configuration = configuration
        self.retriever = retriever
        self.folder = folder
        self.errors = errors
        self.metadata = {}

    def get_data(self):
        base_url = self.configuration["base_url"]
        metadata = self.retriever.download_csv(base_url)

        for row in metadata:
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
        dataset.set_organization(metadata["organization"])
        dataset.set_expected_update_frequency(metadata["update_frequency"])
        dataset.set_subnational(True)
        dataset.add_country_locations(metadata["groups"])

        tags = metadata["tags"].split(", ")
        dataset.add_tags(tags)

        start_date = metadata["dataset_date_start"]
        end_date = metadata["dataset_date_end"]
        ongoing = True
        if end_date:
            ongoing = False
        dataset.set_time_period(start_date, end_date, ongoing)

        resources = list()
        try:
            dataset.add_update_resources(resources)
        except HDXError as ex:
            self.errors.add(f"Dataset: {dataset['name']} resources could not be added. Error: {ex}")

        return dataset

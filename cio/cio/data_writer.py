from __future__ import annotations

import os.path
from abc import ABC, abstractmethod

import pandas as pd

import cio.constants as c


class BaseWriter(ABC):
    def __init__(self, config: dict):
        """Base Writer class to write data to different sources based on config.

        Args:
            config (dict): Dictionary with values related to the source and other details
                of the data writer
        """
        self.config = config

    @abstractmethod
    def write_data(self):
        pass


class ParquetWriter(BaseWriter):
    """Writes data to Parquet files.

    Example Config:
    {
        "writer_class": "ParquetWriter",
        "filename": "/Users/praneshbalekai/Desktop/MK2/data/test.parquet",
        "writer_params": {
            "append_if_exists": True,
            "sort_index": True,
            "deduplicate_index": True
        }
    }
    """

    def write_data(self, data):
        if "writer_params" in self.config:
            if (
                "append_if_exists" in self.config["writer_params"]
                and self.config["writer_params"]["append_if_exists"]
            ):
                if os.path.isfile(self.config["filename"]):
                    df = pd.read_parquet(self.config["filename"])
                    data = pd.concat([df, data])
            if (
                "sort_index" in self.config["writer_params"]
                and self.config["writer_params"]["sort_index"]
            ):
                data = data.sort_index()
            if (
                "deduplicate_index" in self.config["writer_params"]
                and self.config["writer_params"]["deduplicate_index"]
            ):
                data = data[~data.index.duplicated(keep="first")]

        data.to_parquet(self.config["filename"])

        return


def write_data(data, config: dict):
    """Based on config, call relevant data writer function.

    Args:
        config (dict): Config Dict object
        data (any): input data to write
    """
    target = config[c.writer_class]
    if target == "ParquetWriter":
        writer = ParquetWriter(config)
    else:
        raise ValueError("Not a valid target for data writer")

    writer.write_data(data)
    return

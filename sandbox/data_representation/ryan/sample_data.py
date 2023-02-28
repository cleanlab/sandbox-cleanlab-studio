import pathlib
from typing import Dict

import requests


DATA_DIR: pathlib.Path = pathlib.Path("./data/")

SAMPLE_DATASETS: Dict[str, str] = {
    "Tweets-1M.csv": "https://s.anish.io/cleanlab/datasets/Tweets-1M.csv",
    "Tweets-100M.csv": "https://s.anish.io/cleanlab/datasets/Tweets-100M.csv",
    "amazon-text-demo": "https://s.cleanlab.ai/amazon-text-demo.csv",
}


def fetch_dataset(dataset_to_use: str) -> pathlib.Path:
    """Fetches dataset and returns local path to it."""
    dataset_path = DATA_DIR / pathlib.Path(dataset_to_use)

    assert dataset_to_use in SAMPLE_DATASETS, f"Invalid dataset {dataset_to_use}"
    resp = requests.get(SAMPLE_DATASETS[dataset_to_use], stream=True)

    with open(dataset_path, "wb") as dataset_file:
        for data in resp.iter_content(chunk_size=8192):
            dataset_file.write(data)

    return dataset_path

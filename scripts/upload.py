import argparse
import json

import requests


def upload(data_fi: str) -> None:
    """
    Update Zenodo bucket with new data version
    :param data_fi: Path to data to upload
    :return: None
    """
    with open(".settings.txt") as f:
        token = f.read().strip()
    params = {"access_token": token}
    with open(data_fi, mode="rb") as f:
        r = requests.put(
            f"https://sandbox.zenodo.org/api/files/1aaf0645-0560-4ce8-9d44-7aae54a81855/{data_fi}",
            params=params,
            data=f,
        )
    print(json.dumps(r.json()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", default="data.zip")
    args = parser.parse_args()

    upload(args.input_file)

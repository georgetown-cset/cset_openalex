import argparse
import json

import requests


def upload(data_fi: str, deposition_id: str, version: str) -> None:
    """
    Update Zenodo bucket with new data version
    :param data_fi: Path to data to upload
    :param deposition_id: Zenodo deposition id
    :param version: New version string for the delivery
    :return: None
    """
    with open(".settings.txt") as f:
        token = f.read().strip()
    params = {"access_token": token}
    # create a new version
    new_version_response = requests.post(
        f"https://zenodo.org/api/deposit/depositions/{deposition_id}/actions/newversion",
        params=params,
    )
    new_version_metadata = new_version_response.json()["metadata"]
    new_version_metadata["version"] = version
    record_id = new_version_response.json()["record_id"]

    try:
        # get new version's bucket link
        new_version_metadata_response = requests.get(
            f"https://www.zenodo.org/api/deposit/depositions/{record_id}", params=params
        )

        # upload data
        bucket = new_version_metadata_response.json()["links"]["bucket"]
        with open(data_fi, mode="rb") as f:
            upload_response = requests.put(
                f"{bucket}/{data_fi}",
                data=f,
                params=params,
            )
            # add metadata
            requests.put(
                f"https://zenodo.org/api/deposit/depositions/{record_id}",
                data=json.dumps({"metadata": new_version_metadata}),
                params=params,
            )
        assert (
            upload_response.status_code == 201
        ), f"Invalid status code: {upload_response.status_code}"

        # publish new version
        requests.post(
            f"https://zenodo.org/api/deposit/depositions/{record_id}/actions/publish",
            params=params,
        )

    except Exception as e:
        # Try to get rid of the in-progress draft so it doesn't block retries. If this doesn't
        # work for some reason, you'll need to manually delete the new version in the Zenodo UI
        requests.post(
            f"https://zenodo.org/api/deposit/depositions/{record_id}/actions/discard",
            params=params,
        )
        raise e


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--export_file", default="cset_openalex.zip")
    parser.add_argument("--deposition_id", default="11034261")
    parser.add_argument("--version", required=True)
    args = parser.parse_args()

    upload(args.export_file, args.deposition_id, args.version)

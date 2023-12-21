# CSET OpenAlex metadata

This project augments OpenAlex works with additional metadata prepared by CSET.
The dataset is available at [Zenodo](link tk).

## Metadata fields

For the complete list of metadata fields and their types, see `schemas/metadata.json`. Below, we describe how
each field was collected in more detail:

### Language ID

### Fields of Study

### Subject relevance predictions

## Updating the dataset

The dataset is updated monthly through the pipeline in `cset_oepnalex_augmentation_dag.py`. This pipeline runs
the query in `sql/metadata.sql` to aggregate CSET metadata associated with each OpenAlex work, backs the
results up within our internal data warehouse, and updates the data on Zenodo.

(For CSET staff) To update the artifacts used by this pipeline, run `bash push_to_airflow.sh`.

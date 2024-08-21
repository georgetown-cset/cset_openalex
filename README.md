# CSET OpenAlex metadata

This project augments OpenAlex works with additional metadata prepared by CSET.
The dataset is available at [Zenodo](https://zenodo.org/records/11034261).

## Metadata fields

For the complete list of metadata fields and their types, see `schemas/metadata.json`. Below, we describe how
each field was collected in more detail:

### Language ID

Our article linkage pipeline [generates language ID labels](https://github.com/georgetown-cset/article-linking/blob/master/utils/run_lid.py)
for titles and abstracts using [PYCLD2](https://pypi.org/project/pycld2/). We only include language IDs where PYCLD2
successfully output a language and marked the output as reliable.

### Subject relevance predictions

We share outputs for subject classifiers (for more information on how these classifiers were trained
and deployed, see [our documentation](https://eto.tech/dataset-docs/mac/#identifying-relevance-to-ai-and-other-emerging-topics))
in the following fields:

* `is_cv` - True if a computer vision classifier predicted the work was relevant
* `is_nlp` - True if a natural language processing classifier predicted the work was relevant
* `is_robotics` - True if a robotics classifier predicted the work was relevant
* `is_ai` - True if an artificial intelligence classifier predicted the work was relevant, or if any of the computer vision, natural language processing, or robotics classifiers predicted the work was relevant
* `is_cyber` - True if a cybersecurity classifier predicted the work was relevant

## Updating the dataset

The dataset is updated monthly through the pipeline in `cset_openalex_augmentation_dag.py`. This pipeline runs
the query in `sql/metadata.sql` to aggregate CSET metadata associated with each OpenAlex work, backs the
results up within our internal data warehouse, and updates the data on Zenodo.

(For CSET staff) To update the artifacts used by this pipeline, run `bash push_to_airflow.sh`.

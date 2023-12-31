# CSET OpenAlex metadata

This project augments OpenAlex works with additional metadata prepared by CSET.
The dataset is available at [Zenodo](link tk).

## Metadata fields

For the complete list of metadata fields and their types, see `schemas/metadata.json`. Below, we describe how
each field was collected in more detail:

### Language ID

Our article linkage pipeline [generates language ID labels](https://github.com/georgetown-cset/article-linking/blob/master/utils/run_lid.py)
for titles and abstracts using [PYCLD2](https://pypi.org/project/pycld2/). We only include language IDs where PYCLD2
successfully output a language and marked the output as reliable.

### Fields of Study

We include the top three level 1 field of study labels generated using the method described in
[Multi-label Classification of Scientific Research Documents Across Domains and Languages](https://aclanthology.org/2022.sdp-1.12) (Toney & Dunham, sdp 2022).
We only include these labels for records with a non-null English title and abstract (as detected by PYCLD2), where the
abstract is over 500 words in length.

### Subject relevance predictions

We share outputs for subject classifiers (for more information on how these classifiers were trained
and deployed, see [our documentation](https://eto.tech/dataset-docs/mac/#identifying-relevance-to-emerging-technology-topics))
in the following fields:

* `is_cv` - True if a computer vision classifier predicted the work was relevant
* `is_nlp` - True if a natural language processing classifier predicted the work was relevant
* `is_ro` - True if a robotics classifier predicted the work was relevant
* `is_ai` - True if an artificial intelligence classifier predicted the work was relevant, or if any of the computer vision, natural language processing, or robotics classifiers predicted the work was relevant
* `is_ai_safety` - True if the artificial intelligence classifier predicted the work was relevant to AI, and a separate AI safety classifier also predicted the work was relevant to AI safety

## Updating the dataset

The dataset is updated monthly through the pipeline in `cset_oepnalex_augmentation_dag.py`. This pipeline runs
the query in `sql/metadata.sql` to aggregate CSET metadata associated with each OpenAlex work, backs the
results up within our internal data warehouse, and updates the data on Zenodo.

(For CSET staff) To update the artifacts used by this pipeline, run `bash push_to_airflow.sh`.

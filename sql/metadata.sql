WITH

ai_pubs AS (
  SELECT
    sources.orig_id,
    predictions.ai
    OR predictions.nlp
    OR predictions.cv
    OR predictions.robotics AS is_ai,
    predictions.nlp AS is_nlp,
    predictions.cv AS is_cv,
    predictions.robotics AS is_robotics,
    predictions.cyber AS is_cyber
  FROM
    literature.sources
  INNER JOIN
    article_classification.predictions
    USING (merged_id)
  WHERE sources.dataset = "openalex"
),

ai_safety_pubs AS (
  SELECT
    sources.orig_id,
    ai_safety_predictions.preds_str AS is_ai_safety
  FROM
    literature.sources
  INNER JOIN
    ai_safety_datasets.ai_safety_predictions
    USING (merged_id)
  WHERE sources.dataset = "openalex"
),

chip_pubs AS (
  SELECT
    sources.orig_id,
    if(chip_predictions.merged_id IS NULL, FALSE, TRUE)
    AS is_chip_design_fabrication
  FROM
    literature.sources
  LEFT JOIN
    almanac_classifiers.chip_predictions
    USING (merged_id)
  WHERE sources.dataset = "openalex"
),

llm_pubs AS (
  SELECT
    sources.orig_id,
    if(llm_predictions.merged_id IS NULL, FALSE, TRUE) AS is_llm
  FROM
    ai_pubs
  -- Get the merged_id for each OA orig_id from sources, which should be 1:1
  INNER JOIN
    literature.sources
    ON ai_pubs.orig_id = sources.orig_id
  -- llm_predictions contains merged_ids for predicted-true pubs
  LEFT JOIN
    almanac_classifiers.llm_predictions
    ON sources.merged_id = llm_predictions.merged_id
  WHERE sources.dataset = "openalex"
),

language_id AS (
  SELECT DISTINCT
    id,
    lower(
      if(
        title_cld2_lid_success AND title_cld2_lid_is_reliable,
        title_cld2_lid_first_result,
        NULL
      )
    ) AS title_language,
    lower(if(
      abstract_cld2_lid_success AND abstract_cld2_lid_is_reliable,
      abstract_cld2_lid_first_result,
      NULL
    )) AS abstract_language
  FROM
    staging_literature.all_metadata_with_cld2_lid
)

SELECT
  works.id,
  language_id.title_language,
  language_id.abstract_language,
  --  top_level1_fields,
  ai_pubs.is_ai,
  ai_pubs.is_nlp,
  ai_pubs.is_cv,
  ai_pubs.is_robotics,
  ai_pubs.is_cyber,
  ai_safety_pubs.is_ai_safety,
  chip_pubs.is_chip_design_fabrication,
  llm_pubs.is_llm
FROM
  openalex.works
LEFT JOIN
  ai_pubs
  ON works.id = ai_pubs.orig_id
LEFT JOIN
  ai_safety_pubs
  ON works.id = ai_safety_pubs.orig_id
LEFT JOIN
  chip_pubs
  ON works.id = chip_pubs.orig_id
LEFT JOIN
  llm_pubs
  ON works.id = llm_pubs.orig_id
LEFT JOIN
  language_id
  ON works.id = language_id.id

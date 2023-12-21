WITH
field_name_scores AS (
  SELECT
    merged_id,
    name,
    field.score AS score
  FROM
    fields_of_study_v2.field_scores
  CROSS JOIN
    UNNEST(fields) AS field
  LEFT JOIN
    fields_of_study_v2.field_meta
    ON
      field_id = field.id
  WHERE
    (level = 1)),

field_order AS (
  SELECT
    merged_id,
    name,
    score,
    ROW_NUMBER() OVER(PARTITION BY merged_id ORDER BY score DESC) AS row_num
  FROM
    field_name_scores),

top_fields AS (
  SELECT
    merged_id,
    ARRAY_AGG(name ORDER BY score DESC) AS top_level1_fields
  FROM
    field_order
  WHERE
    (
      row_num < 4
    ) AND (
      merged_id IN (
        SELECT merged_id
        FROM
          literature.papers
        WHERE
          (
            title_english IS NOT NULL
          ) AND (abstract_english IS NOT NULL) AND (LENGTH(abstract_english) > 500) AND (year > 2010)
      )
    )
  GROUP BY merged_id
),

ai_pubs AS (
  SELECT
    merged_id,
    ai_filtered OR nlp_filtered OR cv_filtered OR robotics_filtered AS is_ai,
    nlp_filtered AS is_nlp,
    cv_filtered AS is_cv,
    robotics_filtered AS is_robotics
  FROM
    article_classification.predictions
  WHERE
    ai_filtered IS TRUE
    OR nlp_filtered IS TRUE
    OR cv_filtered IS TRUE
    OR robotics_filtered IS TRUE
),

ai_safety_pubs AS (
  SELECT
    merged_id,
    preds_str AS is_ai_safety
  FROM
    ai_safety_datasets.ai_safety_predictions
),

language_id AS (
  SELECT DISTINCT
    id,
    IF(title_cld2_lid_success AND title_cld2_lid_is_reliable, title_cld2_lid_first_result, NULL) AS title_language,
    IF(
      abstract_cld2_lid_success AND abstract_cld2_lid_is_reliable, abstract_cld2_lid_first_result, NULL
    ) AS abstract_language
  FROM
    staging_literature.all_metadata_with_cld2_lid
)

SELECT
  id,
  title_language,
  abstract_language,
  top_level1_fields,
  is_ai,
  is_nlp,
  is_cv,
  is_robotics,
  is_ai_safety
FROM
  openalex.works
INNER JOIN
  literature.sources
  ON id = orig_id
LEFT JOIN
  top_fields
  USING (merged_id)
LEFT JOIN
  ai_pubs
  USING (merged_id)
LEFT JOIN
  ai_safety_pubs
  USING (merged_id)
LEFT JOIN
  language_id
  USING (id)

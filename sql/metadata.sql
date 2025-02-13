WITH
-- field_name_scores AS (
--   SELECT
--     merged_id,
--     name,
--     field.score AS score
--   FROM
--     fields_of_study_v2.field_scores
--   CROSS JOIN
--     UNNEST(fields) AS field
--   LEFT JOIN
--     fields_of_study_v2.field_meta
--     ON
--       field_id = field.id
--   WHERE
--     (level = 1)),
--
-- field_order AS (
--   SELECT
--     merged_id,
--     name,
--     score,
--     ROW_NUMBER() OVER(PARTITION BY merged_id ORDER BY score DESC) AS row_num
--   FROM
--     field_name_scores),
--
-- top_fields AS (
--   SELECT
--     merged_id,
--     ARRAY_AGG(name ORDER BY score DESC) AS top_level1_fields
--   FROM
--     field_order
--   WHERE
--     (
--       row_num < 4
--     ) AND (
--       merged_id IN (
--         SELECT merged_id
--         FROM
--           literature.papers
--         WHERE
--           (
--             title_english IS NOT NULL
--           ) AND (abstract_english IS NOT NULL) AND (LENGTH(abstract_english) > 500) AND (year > 2010)
--       )
--     )
--   GROUP BY merged_id
-- ),

ai_pubs AS (
  SELECT
    orig_id,
    ai OR nlp OR cv OR robotics AS is_ai,
    nlp AS is_nlp,
    cv AS is_cv,
    robotics AS is_robotics,
    cyber AS is_cyber
  FROM
    openalex_article_classification.predictions
),

ai_safety_pubs AS (
  SELECT
    orig_id,
    preds_str AS is_ai_safety
  FROM
    ai_safety_openalex.ai_safety_predictions
),

chip_pubs AS (
  SELECT
    orig_id,
    label AS is_chip_design_fabrication
  FROM
    vertex_batches_us.chip_classifier_predictions
  INNER JOIN
    literature.sources
    USING (merged_id)
  WHERE dataset = "openalex"
),

llm_pubs AS (
  SELECT
    orig_id,
    label AS is_llm
  FROM
    almanac_classifiers.llm_classifier_predictions
  INNER JOIN
    literature.sources
    USING (merged_id)
  WHERE dataset = "openalex"
),

language_id AS (
  SELECT DISTINCT
    id,
    LOWER(
      IF(title_cld2_lid_success AND title_cld2_lid_is_reliable, title_cld2_lid_first_result, NULL)
    ) AS title_language,
    LOWER(IF(
      abstract_cld2_lid_success AND abstract_cld2_lid_is_reliable, abstract_cld2_lid_first_result, NULL
    )) AS abstract_language
  FROM
    staging_literature.all_metadata_with_cld2_lid
)

SELECT
  id,
  title_language,
  abstract_language,
  --  top_level1_fields,
  is_ai,
  is_nlp,
  is_cv,
  is_robotics,
  is_cyber,
  is_ai_safety,
  is_chip_design_fabrication,
  is_llm
FROM
  openalex.works
-- LEFT JOIN
--   top_fields
--   USING (merged_id)
LEFT JOIN
  ai_pubs
  ON id = ai_pubs.orig_id
LEFT JOIN
  ai_safety_pubs
  ON id = ai_safety_pubs.orig_id
LEFT JOIN
  chip_pubs
  ON id = chip_pubs.orig_id
LEFT JOIN
  llm_pubs
  ON id = llm_pubs.orig_id
LEFT JOIN
  language_id
  USING (id)

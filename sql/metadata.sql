with
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
  select
    merged_id,
    ai_filtered or nlp_filtered or cv_filtered or robotics_filtered as is_ai,
    nlp_filtered as is_nlp,
    cv_filtered as is_cv,
    robotics_filtered as is_robotics
  from
    article_classification.predictions
  where
    ai_filtered is true 
       or nlp_filtered is true 
       or cv_filtered is true 
       or robotics_filtered is true
),

ai_safety_pubs AS (
  select
    merged_id,
    preds_str as is_ai_safety
  from
    ai_safety_datasets.ai_safety_predictions
),

language_id AS (
  select
    id,
    if(title_cld2_lid_success and title_cld2_lid_is_reliable, title_cld2_lid_first_result, null) as title_language,
    if(abstract_cld2_lid_success and abstract_cld2_lid_is_reliable, abstract_cld2_lid_first_result, null) as abstract_language,
  from
    staging_literature.all_metadata_with_cld2_lid
)

select
  id,
  title_language,
  abstract_language,
  top_level1_fields,
  is_ai,
  is_nlp,
  is_cv,
  is_robotics,
  is_ai_safety,
from 
  openalex.works
inner join
  literature.sources
on id = orig_id
left join
  top_fields
using(merged_id) 
left join
  ai_pubs
using(merged_id)
left join
  ai_safety_pubs
using(merged_id)
left join
  language_id
using(id)


/*
Official BQ ML tutorial supplementary queries
*/

-- Data source:
select *
from `bigquery-public-data.ml_datasets.penguins`
limit 10
;


-- Create & Train BigQuery ML model:
CREATE OR REPLACE MODEL `bqml_tutorial.penguin_weight_model`
OPTIONS (
  model_type="linear_reg",
  input_label_cols=["body_mass_g"]
)
AS (
  SELECT
    -- X:
    species,
    island,
    culmen_length_mm,
    culmen_depth_mm,
    flipper_length_mm,
    sex,
    -- y:
    body_mass_g
  FROM `bigquery-public-data.ml_datasets.penguins`
  WHERE true
    and body_mass_g is not null
)
;


-- Evaluate the model created:
SELECT *
FROM ML.EVALUATE(
  MODEL `bqml_tutorial.penguin_weight_model`,
  (
      SELECT
        -- X:
        species,
        island,
        culmen_length_mm,
        culmen_depth_mm,
        flipper_length_mm,
        sex,
        -- y:
        body_mass_g
      FROM `bigquery-public-data.ml_datasets.penguins`
      WHERE true
        and body_mass_g is not null
  )
)
;

-- Model inference:
SELECT *
FROM ML.PREDICT(
  MODEL `bqml_tutorial.penguin_weight_model`,
  (
    SELECT *
    FROM `bigquery-public-data.ml_datasets.penguins`
    WHERE true
      AND body_mass_g is not null
  )
)
;


-- Explain specific prediction:
SELECT *
FROM ML.EXPLAIN_PREDICT
(
  MODEL `bqml_tutorial.penguin_weight_model`,
  (
    SELECT *
    FROM `bigquery-public-data.ml_datasets.penguins`
    WHERE
      body_mass_g IS NOT NULL
      AND island = "Biscoe"
  ),
  STRUCT(3 as top_k_features)
)
;


-- Global explainer (average for all dataset) on train dataset:

-- 1). retrain model with enable_global_explain=TRUE
CREATE OR REPLACE MODEL `bqml_tutorial.penguin_weight_model`
OPTIONS (
  model_type="linear_reg",
  input_label_cols=["body_mass_g"],
  enable_global_explain=TRUE
)
AS (
  SELECT
    -- X:
    species,
    island,
    culmen_length_mm,
    culmen_depth_mm,
    flipper_length_mm,
    sex,
    -- y:
    body_mass_g
  FROM `bigquery-public-data.ml_datasets.penguins`
  WHERE true
    and body_mass_g is not null
)
;

-- 2). show feature weights:
SELECT *
FROM ML.GLOBAL_EXPLAIN(MODEL `bqml_tutorial.penguin_weight_model`)
;

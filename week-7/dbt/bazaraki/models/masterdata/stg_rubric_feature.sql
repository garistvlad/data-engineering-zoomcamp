{{ config(materialized='table') }}

select
    rubric_id,
    feature_name,
    feature_verbose_name,
    feature_type,
    feature_type_id,
    feature_choices,
    filter_feature,
    required,
    measure_unit

from
    {{ source('bazaraki', 'external_rubric_feature') }}

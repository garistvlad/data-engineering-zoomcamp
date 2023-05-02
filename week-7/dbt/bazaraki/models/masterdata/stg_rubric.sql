{{ config(materialized='table') }}

select
    id as rubric_id,
    name,
    slug,
    path as url
from
    {{ source('bazaraki', 'external_rubric') }}

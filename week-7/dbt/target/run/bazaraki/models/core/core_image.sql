

  create or replace view `hip-plexus-374912`.`bazaraki`.`core_image`
  OPTIONS()
  as 

select
    image_id,
    compressed_url,
    origiral_url,
    is_flatplan

from `hip-plexus-374912`.`bazaraki`.`stg_image_incremental`

union all

select
    image_id,
    compressed_url,
    origiral_url,
    is_flatplan

from `hip-plexus-374912`.`bazaraki`.`stg_image_full`
where true
  -- not presented in incremental data
  and image_id not in (
    select image_id
    from `hip-plexus-374912`.`bazaraki`.`stg_image_incremental`
  );


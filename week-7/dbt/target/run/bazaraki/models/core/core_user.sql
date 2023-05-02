

  create or replace view `hip-plexus-374912`.`bazaraki`.`core_user`
  OPTIONS()
  as 

select
    user_id,
    phone,
    name,
    registration_date,
    has_email,
    verified,
    type_new
from `hip-plexus-374912`.`bazaraki`.`stg_user_incremental`

union all

select
    user_id,
    phone,
    name,
    registration_date,
    has_email,
    verified,
    type_new
from `hip-plexus-374912`.`bazaraki`.`stg_user_full`
where true
  -- not presented in incremental data
  and user_id not in (
    select user_id
    from `hip-plexus-374912`.`bazaraki`.`stg_user_incremental`
  );


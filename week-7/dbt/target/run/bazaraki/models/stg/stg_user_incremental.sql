
  
    

    create or replace table `hip-plexus-374912`.`bazaraki`.`stg_user_incremental`
    
    

    OPTIONS()
    as (
      

select
  user_id,
  phone,
  name,
  registration_date,
  has_email,
  verified,
  type_new
from
    `hip-plexus-374912`.`bazaraki`.`external_user_incremental`
    );
  
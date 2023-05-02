
  
    

    create or replace table `hip-plexus-374912`.`bazaraki`.`stg_image_full`
    
    

    OPTIONS()
    as (
      

select
    image_id,
    compressed_url,
    origiral_url,
    is_flatplan
from
    `hip-plexus-374912`.`bazaraki`.`external_image_full`
    );
  
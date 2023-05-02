
  
    

    create or replace table `hip-plexus-374912`.`bazaraki`.`stg_image_to_advertisement_mapping_full`
    
    

    OPTIONS()
    as (
      

select
    image_id,
    advertisement_id
from
    `hip-plexus-374912`.`bazaraki`.`external_image_to_advertisement_mapping_full`
    );
  
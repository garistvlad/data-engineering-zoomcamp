

select
    image_id,
    advertisement_id
from `hip-plexus-374912`.`bazaraki`.`stg_image_to_advertisement_mapping_incremental`

union distinct

select
    image_id,
    advertisement_id
from `hip-plexus-374912`.`bazaraki`.`stg_image_to_advertisement_mapping_full`
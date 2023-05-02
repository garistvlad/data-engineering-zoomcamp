

select
    advertisement_id,
    title,
    slug,
    rubric_id,
    description,
    city_id,
    price,
    hit_count,
    phone_hitcount,
    currency,
    created_dt,
    raise_dt,
    owner_advert_count,
    phone_hide,
    coordinates,
    zoom,
    negotiable_price,
    exchange,
    imei_checked,
    price_description,
    in_top,
    in_premium,
    is_editable,
    is_favorite,
    video_link,
    cloudinary_video,
    all_images,
    templated_title,
    credit_type,
    credit_attrs,
    credit_link,
    flatplan,
    virtual_tour_link,
    is_carcheck,
    new_in_stock_label,
    new_to_order_label,
    price_from,
    price_short,
    user_id,
    district_id

from
    `hip-plexus-374912`.`bazaraki`.`external_advertisement_incremental`
-- STAGING: products
-- ─────────────────────────────────────────────────────────────────────────────

with source as (
    select * from {{ source('olist', 'products') }}
),

translated as (
    -- Join category translation so downstream models get English names
    select
        p.product_id,
        coalesce(t.product_category_name_english, p.product_category_name)
            as product_category,
        p.product_category_name              as product_category_portuguese,
        cast(p.product_name_lenght    as int) as product_name_length,
        cast(p.product_description_lenght as int) as product_description_length,
        cast(p.product_photos_qty     as int) as product_photos_qty,
        cast(p.product_weight_g       as numeric(10, 2)) as weight_grams,
        cast(p.product_length_cm      as numeric(10, 2)) as length_cm,
        cast(p.product_height_cm      as numeric(10, 2)) as height_cm,
        cast(p.product_width_cm       as numeric(10, 2)) as width_cm
    from source p
    left join {{ source('olist', 'product_category_name_translation') }} t
        on p.product_category_name = t.product_category_name
    where p.product_id is not null
)

select * from translated

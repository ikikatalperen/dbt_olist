{{ config(
    materialized = 'view'
) }}

SELECT
    -- Kimlik
    product_id,

    -- Kategori: boş stringleri NULL yap, küçük harfe çevir
    NULLIF(LOWER(product_category_name), '') AS product_category_name,

    -- Yazım hatalı kolon adlarını düzeltiyoruz
    product_name_lenght         AS product_name_length,
    product_description_lenght  AS product_description_length,

    -- Sayısal kolonlar zaten INTEGER tipinde, sadece alias veriyoruz
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm

FROM {{ source('raw', 'products_dataset') }}
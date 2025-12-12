with 

source as (

    select * from {{ source('raw', 'product_category_name_translation') }}

),

renamed as (

    select
        string_field_0 as product_category_name_pt,
        string_field_1 as product_category_name_english

    from source

)

select * from renamed
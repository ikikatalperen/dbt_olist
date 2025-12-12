with 

source as (

    select * from {{ source('raw', 'order_reviews_dataset') }}

),

renamed as (

    select
        review_id,
        order_id,
        review_score,
        review_creation_date,
        review_answer_timestamp

    from source

)

select * from renamed
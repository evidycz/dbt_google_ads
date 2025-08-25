with source as (

    select *
    from {{ source('google_ads', 'accounts') }}
),

final as (

    select
        id as account_id,
        descriptive_name as account_name,

        currency_code,
        time_zone,

        optimization_score,
        optimization_score_weight,

        auto_tagging_enabled,

    from source
)

select * from final


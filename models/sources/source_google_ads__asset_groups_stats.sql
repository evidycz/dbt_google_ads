with source as (

    select *
    from {{ source('google_ads', 'asset_groups_stats') }}
),

final as  (

    select

         {{ adapter.quote('date') }} as date_day,

         {{ extract_resource_id('resource_name', "'customers'") }} as account_id,
         {{ extract_resource_id('campaign', "'campaigns'") }} as campaign_id,
         id as id,

        device as device,

        coalesce(cast(impressions as int), 0) as impressions,
        coalesce(cast(clicks as int), 0) as clicks,
        coalesce(conversions, 0) as conversions,
        coalesce(conversions_value, 0) as conversions_value,
        round(coalesce(cast(cost_micros as numeric), 0) / 1000000, 2) as spend_czk,
    from source
)

select * from final
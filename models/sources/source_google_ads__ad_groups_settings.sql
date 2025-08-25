with source as (

    select *
    from {{ source('google_ads', 'ad_groups') }}
),

final as (

    select

        {{ extract_resource_id('resource_name', "'customers'") }} as account_id,
        {{ extract_resource_id('campaign', "'campaigns'") }} as campaign_id,

        id as ad_group_id,
        name as ad_group_name,
        status as ad_group_status,

        round(coalesce(cast(cpc_bid_micros as float) / 1000000, 0), 2) as set_cpc,
        round(coalesce(cast(cpm_bid_micros as float) / 1000000, 0), 2) as set_cpm,
        round(coalesce(cast(cpv_bid_micros as float) / 1000000, 0), 2) as set_cpv,
    from source
)

select * from final
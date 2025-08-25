with source as (

    select *
    from {{ source('google_ads', 'asset_groups') }}
),

final as (

    select
        {{ extract_resource_id('resource_name', "'customers'") }} as account_id,
        {{ extract_resource_id('campaign', "'campaigns'") }} as campaign_id,

        id as asset_group_id,
        name as asset_group_name,
        status as asset_group_status,

        ad_strength as ad_strength,
    from source
)

select * from final
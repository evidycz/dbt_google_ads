with source as (

    select *
    from {{ source('google_ads', 'campaigns') }}
),

final as (

    select

        {{ extract_resource_id('resource_name', "'customers'") }} as account_id,

        id as campaign_id,
        name as campaign_name,
        advertising_channel_type as campaign_type,
        status as campaign_status,

    from source
)

select * from final
with ad_groups_settings as (

    select *
    from {{ ref('source_google_ads__ad_groups_settings') }}
),

asset_groups_settings as (

    select *
    from {{ ref('source_google_ads__asset_groups_settings') }}
),

campaigns_settings as (

    select
        campaign_id,
        campaign_name,
        campaign_status,
    from {{ ref('source_google_ads__campaigns_settings') }}
),

accounts_settings as (

    select
        account_id,
        account_name,
    from {{ ref('source_google_ads__accounts_settings') }}
),

ad_groups_stats as (

    select *
    from {{ ref('int_google_ads__ad_groups') }}
),

joined_settings_and_stats as (

    select

        ad_groups_stats.date_day,

        accounts_settings.account_id,
        accounts_settings.account_name,

        campaigns_settings.campaign_id,
        campaigns_settings.campaign_name,
        campaigns_settings.campaign_status,

        ad_groups_settings.ad_group_name,
        ad_groups_settings.ad_group_status,

        ad_groups_settings.set_cpc,
        ad_groups_settings.set_cpm,
        ad_groups_settings.set_cpv,

        ad_groups_stats.device,

        ad_groups_stats.impressions,
        ad_groups_stats.clicks,
        ad_groups_stats.conversions,
        ad_groups_stats.conversions_value,
        ad_groups_stats.spend_czk,
    from ad_groups_stats
    left join ad_groups_settings on ad_groups_stats.id = ad_groups_settings.ad_group_id
    left join campaigns_settings on ad_groups_stats.campaign_id = campaigns_settings.campaign_id
    left join accounts_settings on ad_groups_stats.account_id = accounts_settings.account_id
)

select * from joined_settings_and_stats
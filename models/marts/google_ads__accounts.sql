with accounts_settings as (

    select
        account_id,
        account_name,
    from {{ ref('source_google_ads__accounts_settings') }}
),

ad_groups_stats as (

    select *
    from {{ ref('int_google_ads__ad_groups') }}
),

aggeregated_stats as (
    select

        date_day,
        account_id,

        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(conversions) as conversions,
        sum(conversions_value) as conversions_value,
        sum(spend_czk) as spend_czk,
    from ad_groups_stats
    {{ dbt_utils.group_by(n=2) }}
),

joined_settings_and_stats as (

    select

        aggeregated_stats.date_day,

        accounts_settings.account_id,
        accounts_settings.account_name,

        aggeregated_stats.impressions,
        aggeregated_stats.clicks,
        aggeregated_stats.conversions,
        aggeregated_stats.conversions_value,
        aggeregated_stats.spend_czk,
    from aggeregated_stats
    left join accounts_settings on aggeregated_stats.account_id = accounts_settings.account_id
)

select * from joined_settings_and_stats
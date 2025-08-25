{{ dbt_utils.union_relations(
    relations=[ref('source_google_ads__ad_groups_stats'), ref('source_google_ads__asset_groups_stats')]
) }}


-- models/silver/stg_repositories.sql

{{ config(materialized='view') }}

{% set rel = source('bronze', 'raw_repositories') %}
{% set cols = adapter.get_columns_in_relation(rel) %}
{% set colnames = cols | map(attribute='name') | map('lower') | list %}

with source as (
    select * from {{ rel }}
),

cleaned as (
    select
        full_name as repo_id,

        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(pushed_at as timestamp) as pushed_at,

        cast(stargazers_count as integer) as stargazers_count,
        cast(watchers_count as integer) as watchers_count,
        cast(open_issues_count as integer) as open_issues_count,
        cast(forks_count as integer) as forks_count,
        cast(size as integer) as size,

        coalesce(description, 'No description') as description,
        coalesce(language, 'Unknown') as language,

        {% if 'license_name' in colnames %}
          cast(license_name as varchar) as license_name,
        {% elif 'license_spdx_id' in colnames %}
          cast(license_spdx_id as varchar) as license_name,
        {% elif 'license_key' in colnames %}
          cast(license_key as varchar) as license_name,
        {% elif 'license' in colnames %}
          cast(license as varchar) as license_name,
        {% else %}
          cast(null as varchar) as license_name,
        {% endif %}

        {% if 'default_branch' in colnames %}
          cast(default_branch as varchar) as default_branch,
        {% else %}
          cast(null as varchar) as default_branch,
        {% endif %}

        {% if 'has_wiki' in colnames %}
          cast(has_wiki as boolean) as has_wiki,
        {% else %}
          cast(null as boolean) as has_wiki,
        {% endif %}

        {% if 'has_pages' in colnames %}
          cast(has_pages as boolean) as has_pages,
        {% else %}
          cast(null as boolean) as has_pages,
        {% endif %}

        datediff('day', cast(created_at as timestamp), current_timestamp) as repo_age_days,

        cast(archived as boolean) as archived

    from source
    where cast(archived as boolean) = false
)

select * from cleaned
{{ 
config(
    materialized='incremental',
    schema='silver',
    unique_key=['repo_full_name','pr_number'],
    incremental_strategy='merge'
) 
}}

{% set rel = source('bronze', 'raw_pull_requests') %}
{% set cols = adapter.get_columns_in_relation(rel) %}
{% set colnames = cols | map(attribute='name') | map('lower') | list %}

with source as (

    select *
    from {{ rel }}

),

cleaned as (

    select

        cast(pr_number as integer) as pr_number,

        cast(created_at as timestamp) as created_at,
        cast(merged_at as timestamp) as merged_at,
        cast(closed_at as timestamp) as closed_at,

        case
            when closed_at is not null then 'closed'
            else 'open'
        end as pr_state,

        (merged_at is not null) as is_merged,
        cast(draft as boolean) as is_draft,

        case
            when merged_at is not null then datediff('hour', created_at, merged_at)
            when closed_at is not null then datediff('hour', created_at, closed_at)
            else null
        end as time_to_close_hours,

        {% if 'user_login' in colnames %}
          cast(user_login as varchar) as user_login,
        {% elif 'author_login' in colnames %}
          cast(author_login as varchar) as user_login,
        {% elif 'login' in colnames %}
          cast(login as varchar) as user_login,
        {% else %}
          cast(null as varchar) as user_login,
        {% endif %}

        {% if 'repo_full_name' in colnames %}
          cast(repo_full_name as varchar) as repo_full_name
        {% elif 'full_name' in colnames %}
          cast(full_name as varchar) as repo_full_name
        {% elif 'repo_name' in colnames and 'owner_login' in colnames %}
          cast(owner_login as varchar) || '/' || cast(repo_name as varchar) as repo_full_name
        {% else %}
          cast(null as varchar) as repo_full_name
        {% endif %}

    from source
    where pr_number is not null

)

select *
from cleaned

{% if is_incremental() %}

where created_at >
(
    select max(created_at)
    from {{ this }}
)

{% endif %}
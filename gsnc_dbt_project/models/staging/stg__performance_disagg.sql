-- this is the staging model / table for consolidating all disaggregated performance data for nc schools
-- to get the schema for all tables within nc_school_database (a "dataset" within google bigquery) spit out automatically, you can run:
		--dbt run-operation generate_base_model --args "{'source_name': 'nc_school_database', 'table_name': 'raw_nc_schools_perf_disagg_sy_21_22'}" --profiles-dir . --profile gsnc_dbt_project

{% set schema = target.dataset %}

{{ log("Debug: schema = " ~ schema, info=True) }}

{% set table_list_query %}
  SELECT table_name
  FROM `{{ target.project }}.{{ schema }}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name LIKE 'raw_nc_schools_perf_disagg_sy_%'
{% endset %}

{{ log("Debug: table_list query = " ~ table_list_query, info=True) }}

{% set results = run_query(table_list_query) %}

{% if execute %}
  {% set tables = results.columns[0].values() %}
  {{ log("Debug: tables found = " ~ tables | join(", "), info=True) }}
{% else %}
  {% set tables = [] %}
{% endif %}

WITH raw_source AS (
  {% for table in tables %}
    SELECT
      *,
      SUBSTR('{{ table }}', -5, 2) || '-' || SUBSTR('{{ table }}', -2, 2) AS reporting_year
    FROM {{ source('nc_school_database', table) }}
    {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
)

SELECT * FROM raw_source
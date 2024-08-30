-- staging environment for dbt data model / table for charter school enrollment by race and gender

-- this creates staging data model / table that stacks enrollment by race and gender for all schools


{% set schema = target.dataset %}

{{ log("Debug: schema = " ~ schema, info=True) }}

{% set table_list_query %}
  SELECT table_name
  FROM `{{ target.project }}.{{ schema }}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name LIKE 'raw_nc_schools_enrollment_charter_race_gender_sy_%'
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
      {% for column in adapter.get_columns_in_relation(source('nc_school_database', table)) %}
        CAST({{ column.name }} AS STRING) AS {{ column.name }}{% if not loop.last %},{% endif %}
      {% endfor %},
      CAST(SUBSTR('{{ table }}', -5, 2) || '-' || SUBSTR('{{ table }}', -2, 2) AS STRING) AS reporting_year,
	  'Charter or Regional' as school_type
    FROM {{ source('nc_school_database', table) }}
    {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
)

SELECT * FROM raw_source
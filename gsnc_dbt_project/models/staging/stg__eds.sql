
-- staging environment for dbt data model / table for all schools' percent of enrollment that is EDS
--Column Name	Meaning
--den	denominator, number of students in the school
--pct_eds	Percent of students who are Economically Disadvantaged
--pct_nslp	Percent of students who are eligible for free and reduced price lunch
--pct_nslp_adj	pct_nslp X 1.6 (number of eligible students multiplied by 1.6 to calculate the percentage of meals reimbursed at the federal free rate.)

-- this creates staging data model / table that stacks enrollment by race and gender for all schools


{% set schema = target.dataset %}

{{ log("Debug: schema = " ~ schema, info=True) }}

{% set table_list_query %}
  SELECT table_name
  FROM `{{ target.project }}.{{ schema }}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name LIKE 'raw_nc_schools_eds_sy_%'
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
      CAST(SUBSTR('{{ table }}', -5, 2) || '-' || SUBSTR('{{ table }}', -2, 2) AS STRING) AS reporting_year_use,
	CASE
    	when length(school_code) < 6 THEN concat('0', school_code)
        else school_code
    END as school_code_use,
    FROM {{ source('nc_school_database', table) }}
    {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
)

SELECT * FROM raw_source
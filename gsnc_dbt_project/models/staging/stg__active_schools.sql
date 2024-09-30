-- staging dbt data model / table that holds the list of active schools across years of avialable dasta
--data model starts with data from school year 24-25 and builds on


{% set schema = target.dataset %}

{{ log("Debug: schema = " ~ schema, info=True) }}

{% set table_list_query %}
  SELECT table_name
  FROM `{{ target.project }}.{{ schema }}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name LIKE 'raw_nc_schools_active_report_sy_%'
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
      SUBSTR('{{ table }}', -5, 2) || '-' || SUBSTR('{{ table }}', -2, 2) AS reporting_year,
	CASE
    	when length(School_Number) < 6 THEN concat('0', School_Number)
        else School_Number
    END as school_code,
    FROM {{ source('nc_school_database', table) }}
    {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
),

raw_source2 as (

	SELECT
		*,
		CASE
			when school_code like '%+%' then concat(SUBSTR(Primary_Key, 1, 3),"000") -- pine lake prep because of 49E with 000, turns into exponential form of a number
			else school_code
		END as school_code_use
	FROM raw_source

)

select * from raw_source2 where length(school_code_use) is not null


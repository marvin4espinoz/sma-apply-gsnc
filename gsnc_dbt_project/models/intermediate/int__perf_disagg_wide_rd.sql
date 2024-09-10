-- intermediate dbt data model / table that widens the long form of performance disaggregated

with math as (

	SELECT
	reporting_year,
	school_code,
	school_name,
	grade_span,
	subject,
	grade,
	type,
	MAX(CASE WHEN subgroup = 'ALL' THEN num_tested ELSE NULL END) AS ALL_num_tested,
	MAX(CASE WHEN subgroup = 'ALL' THEN pct_notprof ELSE NULL END) AS ALL_pct_notprof,
	MAX(CASE WHEN subgroup = 'ALL' THEN pct_l3 ELSE NULL END) AS ALL_pct_l3,
	MAX(CASE WHEN subgroup = 'ALL' THEN pct_l4 ELSE NULL END) AS ALL_pct_l4,
	MAX(CASE WHEN subgroup = 'ALL' THEN pct_l5 ELSE NULL END) AS ALL_pct_l5,
	MAX(CASE WHEN subgroup = 'ALL' THEN pct_ccr ELSE NULL END) AS ALL_pct_ccr,
	MAX(CASE WHEN subgroup = 'ALL' THEN pct_glp ELSE NULL END) AS ALL_pct_glp,
	MAX(CASE WHEN subgroup = 'BLCK' THEN num_tested ELSE NULL END) AS BLCK_num_tested,
	MAX(CASE WHEN subgroup = 'BLCK' THEN pct_notprof ELSE NULL END) AS BLCK_pct_notprof,
	MAX(CASE WHEN subgroup = 'BLCK' THEN pct_l3 ELSE NULL END) AS BLCK_pct_l3,
	MAX(CASE WHEN subgroup = 'BLCK' THEN pct_l4 ELSE NULL END) AS BLCK_pct_l4,
	MAX(CASE WHEN subgroup = 'BLCK' THEN pct_l5 ELSE NULL END) AS BLCK_pct_l5,
	MAX(CASE WHEN subgroup = 'BLCK' THEN pct_ccr ELSE NULL END) AS BLCK_pct_ccr,
	MAX(CASE WHEN subgroup = 'BLCK' THEN pct_glp ELSE NULL END) AS BLCK_pct_glp,
	MAX(CASE WHEN subgroup = 'EDS' THEN num_tested ELSE NULL END) AS EDS_num_tested,
	MAX(CASE WHEN subgroup = 'EDS' THEN pct_notprof ELSE NULL END) AS EDS_pct_notprof,
	MAX(CASE WHEN subgroup = 'EDS' THEN pct_l3 ELSE NULL END) AS EDS_pct_l3,
	MAX(CASE WHEN subgroup = 'EDS' THEN pct_l4 ELSE NULL END) AS EDS_pct_l4,
	MAX(CASE WHEN subgroup = 'EDS' THEN pct_l5 ELSE NULL END) AS EDS_pct_l5,
	MAX(CASE WHEN subgroup = 'EDS' THEN pct_ccr ELSE NULL END) AS EDS_pct_ccr,
	MAX(CASE WHEN subgroup = 'EDS' THEN pct_glp ELSE NULL END) AS EDS_pct_glp,
	MAX(CASE WHEN subgroup = 'ELS' THEN num_tested ELSE NULL END) AS ELS_num_tested,
	MAX(CASE WHEN subgroup = 'ELS' THEN pct_notprof ELSE NULL END) AS ELS_pct_notprof,
	MAX(CASE WHEN subgroup = 'ELS' THEN pct_l3 ELSE NULL END) AS ELS_pct_l3,
	MAX(CASE WHEN subgroup = 'ELS' THEN pct_l4 ELSE NULL END) AS ELS_pct_l4,
	MAX(CASE WHEN subgroup = 'ELS' THEN pct_l5 ELSE NULL END) AS ELS_pct_l5,
	MAX(CASE WHEN subgroup = 'ELS' THEN pct_ccr ELSE NULL END) AS ELS_pct_ccr,
	MAX(CASE WHEN subgroup = 'ELS' THEN pct_glp ELSE NULL END) AS ELS_pct_glp,
	MAX(CASE WHEN subgroup = 'HISP' THEN num_tested ELSE NULL END) AS HISP_num_tested,
	MAX(CASE WHEN subgroup = 'HISP' THEN pct_notprof ELSE NULL END) AS HISP_pct_notprof,
	MAX(CASE WHEN subgroup = 'HISP' THEN pct_l3 ELSE NULL END) AS HISP_pct_l3,
	MAX(CASE WHEN subgroup = 'HISP' THEN pct_l4 ELSE NULL END) AS HISP_pct_l4,
	MAX(CASE WHEN subgroup = 'HISP' THEN pct_l5 ELSE NULL END) AS HISP_pct_l5,
	MAX(CASE WHEN subgroup = 'HISP' THEN pct_ccr ELSE NULL END) AS HISP_pct_ccr,
	MAX(CASE WHEN subgroup = 'HISP' THEN pct_glp ELSE NULL END) AS HISP_pct_glp,
	MAX(CASE WHEN subgroup = 'SWD' THEN num_tested ELSE NULL END) AS SWD_num_tested,
	MAX(CASE WHEN subgroup = 'SWD' THEN pct_notprof ELSE NULL END) AS SWD_pct_notprof,
	MAX(CASE WHEN subgroup = 'SWD' THEN pct_l3 ELSE NULL END) AS SWD_pct_l3,
	MAX(CASE WHEN subgroup = 'SWD' THEN pct_l4 ELSE NULL END) AS SWD_pct_l4,
	MAX(CASE WHEN subgroup = 'SWD' THEN pct_l5 ELSE NULL END) AS SWD_pct_l5,
	MAX(CASE WHEN subgroup = 'SWD' THEN pct_ccr ELSE NULL END) AS SWD_pct_ccr,
	MAX(CASE WHEN subgroup = 'SWD' THEN pct_glp ELSE NULL END) AS SWD_pct_glp,
	MAX(CASE WHEN subgroup = 'WHTE' THEN num_tested ELSE NULL END) AS WHTE_num_tested,
	MAX(CASE WHEN subgroup = 'WHTE' THEN pct_notprof ELSE NULL END) AS WHTE_pct_notprof,
	MAX(CASE WHEN subgroup = 'WHTE' THEN pct_l3 ELSE NULL END) AS WHTE_pct_l3,
	MAX(CASE WHEN subgroup = 'WHTE' THEN pct_l4 ELSE NULL END) AS WHTE_pct_l4,
	MAX(CASE WHEN subgroup = 'WHTE' THEN pct_l5 ELSE NULL END) AS WHTE_pct_l5,
	MAX(CASE WHEN subgroup = 'WHTE' THEN pct_ccr ELSE NULL END) AS WHTE_pct_ccr,
	MAX(CASE WHEN subgroup = 'WHTE' THEN pct_glp ELSE NULL END) AS WHTE_pct_glp

	FROM {{ ref('stg__performance_disagg') }}

	GROUP BY
	reporting_year,
	school_code,
	school_name,
	grade_span,
	subject,
	grade,
	type

)

select * from math where subject = 'RD'
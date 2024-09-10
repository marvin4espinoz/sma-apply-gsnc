-- intermediate dbt data model / table that combines:
-- active_schools + spg_subgroup + performance_disagg + enrollment tables
-- into an intermediate table without changing value types yet but creating the keys

-- intermediate dbt data model / table that combines:
-- active_schools + spg_subgroup + performance_disagg + enrollment tables
-- into an intermediate table without changing value types yet but creating the keys

-- intermediate dbt data model / table that combines:
-- active_schools + spg_subgroup + performance_disagg + enrollment tables
-- into an intermediate table without changing value types yet but creating the keys

with schools as (
    select
        CASE
            when length(school_number) < 6 THEN concat('0', school_number)
            else school_number
        END as school_code,
        *
    from {{ ref('stg__active_schools') }}
),

spg as (
    select
        reporting_year_use as reporting_year,
        lea_code,
        school_code,
        grade_span,
        title_1,
        spg_grade_all,
        spg_score_all,
        ach_score_all,
        eg_score_all,
        eg_status_all,
        eg_index_all,
        spg_grade_amin,
        spg_score_amin,
        ach_score_amin,
        eg_score_amin,
        eg_status_amin,
        eg_index_amin,
        spg_grade_asia,
        spg_score_asia,
        ach_score_asia,
        eg_score_asia,
        eg_status_asia,
        eg_index_asia,
        spg_grade_blck,
        spg_score_blck,
        ach_score_blck,
        eg_score_blck,
        eg_status_blck,
        eg_index_blck,
        spg_grade_hisp,
        spg_score_hisp,
        ach_score_hisp,
        eg_score_hisp,
        eg_status_hisp,
        eg_index_hisp,
        spg_grade_mult,
        spg_score_mult,
        ach_score_mult,
        eg_score_mult,
        eg_status_mult,
        eg_index_mult,
        spg_grade_whte,
        spg_score_whte,
        ach_score_whte,
        eg_score_whte,
        eg_status_whte,
        eg_index_whte,
        spg_grade_eds,
        spg_score_eds,
        ach_score_eds,
        eg_score_eds,
        eg_status_eds,
        eg_index_eds,
        spg_grade_els,
        spg_score_els,
        ach_score_els,
        eg_score_els,
        eg_status_els,
        eg_index_els,
        spg_grade_swd,
        spg_score_swd,
        ach_score_swd,
        eg_score_swd,
        eg_status_swd,
        eg_index_swd
    from {{ ref('int__spg_subgroup_wide') }}
),

enrollment as (
    select
        concat(lea, school) as school_code,
        reporting_year,
        school_type as enrollment_school_type,  -- Renamed to avoid conflict
        indian_male,
        indian_female,
        asian_male,
        asian_female,
        hispanic_male,
        hispanic_female,
        black_male,
        black_female,
        white_male,
        white_female,
        pacific_island_female,
        pacific_island_male,
        two_or__more_male,
        two_or__more_female,
        total
    from {{ ref('stg__enrollment_schools_race_gender') }}
),

performance as (
    select
        reporting_year,
        school_code,
        subject,
        grade,
        type,
        all_num_tested,
        all_pct_notprof,
        all_pct_l3,
        all_pct_l4,
        all_pct_l5,
        all_pct_ccr,
        all_pct_glp,
        blck_num_tested,
        blck_pct_notprof,
        blck_pct_l3,
        blck_pct_l4,
        blck_pct_l5,
        blck_pct_ccr,
        blck_pct_glp,
        eds_num_tested,
        eds_pct_notprof,
        eds_pct_l3,
        eds_pct_l4,
        eds_pct_l5,
        eds_pct_ccr,
        eds_pct_glp,
        els_num_tested,
        els_pct_notprof,
        els_pct_l3,
        els_pct_l4,
        els_pct_l5,
        els_pct_ccr,
        els_pct_glp,
        hisp_num_tested,
        hisp_pct_notprof,
        hisp_pct_l3,
        hisp_pct_l4,
        hisp_pct_l5,
        hisp_pct_ccr,
        hisp_pct_glp,
        swd_num_tested,
        swd_pct_notprof,
        swd_pct_l3,
        swd_pct_l4,
        swd_pct_l5,
        swd_pct_ccr,
        swd_pct_glp,
        whte_num_tested,
        whte_pct_notprof,
        whte_pct_l3,
        whte_pct_l4,
        whte_pct_l5,
        whte_pct_ccr,
        whte_pct_glp
    from {{ ref('int__perf_disagg_wide') }}
)

select
    schools.*,
    panel_data.reporting_year as panel_reporting_year,
    panel_data.* except(school_code, reporting_year)
from
    schools
    left join (
        select
            coalesce(enrollment.school_code, spg.school_code, performance.school_code) as school_code,
            coalesce(enrollment.reporting_year, spg.reporting_year, performance.reporting_year) as reporting_year,
            enrollment.* except(school_code, reporting_year),
            spg.* except(school_code, reporting_year),
            performance.* except(school_code, reporting_year)
        from
            enrollment
            full outer join spg using (school_code, reporting_year)
            full outer join performance using (school_code, reporting_year)
    ) as panel_data on schools.school_code = panel_data.school_code
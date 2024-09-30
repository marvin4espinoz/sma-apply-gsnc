-- intermediate dbt data model / table that combines:
-- active_schools + spg_subgroup + performance_disagg + enrollment tables
-- into an intermediate table without changing value types yet but creating the keys

-- run this model by itself: dbt run --models int__school_profile_inprogress

with active_schools as (
    select
    	school_year,
		school_name,
		official_school_name,
		lea_name,
		name_prefix_description,
		principal_first_name,
		principal_middle_name,
		principal_last_name,
		principal_name_suffix,
		principal_email,
		address_line1,
		address_line2,
		city,
		state,
		zip_code_5,
		zip_code_4,
		phone_office_area,
		phone_office_exch,
		phone_office_line,
		phone_fax_area,
		phone_fax_exch,
		phone_fax_line,
		mailing_address_line1,
		mailing_address_line2,
		mailing_city,
		mailing_state,
		mailing_zip_code_5,
		mailing_zip_code_4,
		url_school_address,
		sch_operational_status,
		sch_operational_status_desc,
		opening_effective_date,
		closing_date,
		school_type,
		school_type_desc,
		school_program_type,
		school_program_type_desc,
		grade_level_current,
		grade_level_approved,
		grade_level_first_year,
		title_i,
		school_calendar_type,
		school_calendar_description,
		extended_hours,
		school_schedule_type,
		school_schedule_type_desc,
		school_designation_type,
		school_designation_desc,
		virtual_status,
		sbe_region,
		sbe_region_names,
		accreditation_status,
		accreditation_status_desc,
		federal_school_number,
		college_board_number,
		school_membership,
		school_year_for_sch_membership,
		school_teacher_count,
		school_year_for_teachers,
		cooperative_innovative_hs,
		cooperative_mmyyyy_sbe_approve,
		first_yr_est_adm,
		first_yr_est_paid_teacher_fte,
		newly_constructed_facility,
		occupancy_date,
		what_facility_will_sch_occupy,
		share_space,
		share_space_describe,
		new_sch_population_from,
		list_of_closing_schools,
		county_code,
		county_description,
		opening_in_school_year,
		locale_type,
		locale_type_desc,
		school_closings,
		school_systartstatus,
		record_created_timestamp,
		record_created_by,
		last_changed_timestamp,
		last_changed_by,
		reporting_year as reporting_year_active_schools,
		school_code_use as school_code_active_schools
    from {{ ref('stg__active_schools') }}
),

spg as (
    select
        reporting_year_use as reporting_year_spg,
        lea as lea_code,
        school_code as school_code_spg,
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
        eg_index_swd,
        rd_spg_grade_all,
        rd_spg_score_all,
        rdgs_ach_score_all,
        rd_eg_score_all,
        rd_eg_status_all,
        rd_eg_index_all,
        rd_spg_grade_amin,
        rd_spg_score_amin,
        rdgs_ach_score_amin,
        rd_eg_score_amin,
        rd_eg_status_amin,
        rd_eg_index_amin,
        rd_spg_grade_asia,
        rd_spg_score_asia,
        rdgs_ach_score_asia,
        rd_eg_score_asia,
        rd_eg_status_asia,
        rd_eg_index_asia,
        rd_spg_grade_blck,
        rd_spg_score_blck,
        rdgs_ach_score_blck,
        rd_eg_score_blck,
        rd_eg_status_blck,
        rd_eg_index_blck,
        rd_spg_grade_hisp,
        rd_spg_score_hisp,
        rdgs_ach_score_hisp,
        rd_eg_score_hisp,
        rd_eg_status_hisp,
        rd_eg_index_hisp,
        rd_spg_grade_mult,
        rd_spg_score_mult,
        rdgs_ach_score_mult,
        rd_eg_score_mult,
        rd_eg_status_mult,
        rd_eg_index_mult,
        rd_spg_grade_whte,
        rd_spg_score_whte,
        rdgs_ach_score_whte,
        rd_eg_score_whte,
        rd_eg_status_whte,
        rd_eg_index_whte,
        ma_spg_grade_all,
        ma_spg_score_all,
        mags_ach_score_all,
        ma_eg_score_all,
        ma_eg_status_all,
        ma_eg_index_all,
        ma_spg_grade_amin,
        ma_spg_score_amin,
        mags_ach_score_amin,
        ma_eg_score_amin,
        ma_eg_status_amin,
        ma_eg_index_amin,
        ma_spg_grade_asia,
        ma_spg_score_asia,
        mags_ach_score_asia,
        ma_eg_score_asia,
        ma_eg_status_asia,
        ma_eg_index_asia,
        ma_spg_grade_blck,
        ma_spg_score_blck,
        mags_ach_score_blck,
        ma_eg_score_blck,
        ma_eg_status_blck,
        ma_eg_index_blck,
        ma_spg_grade_hisp,
        ma_spg_score_hisp,
        mags_ach_score_hisp,
        ma_eg_score_hisp,
        ma_eg_status_hisp,
        ma_eg_index_hisp,
        ma_spg_grade_mult,
        ma_spg_score_mult,
        mags_ach_score_mult,
        ma_eg_score_mult,
        ma_eg_status_mult,
        ma_eg_index_mult,
        ma_spg_grade_whte,
        ma_spg_score_whte,
        mags_ach_score_whte,
        ma_eg_score_whte,
        ma_eg_status_whte,
        ma_eg_index_whte,
        ma_spg_grade_eds,
        ma_spg_score_eds,
        mags_ach_score_eds,
        ma_eg_score_eds,
        ma_eg_status_eds,
        ma_eg_index_eds,
        ma_spg_grade_els,
        ma_spg_score_els,
        mags_ach_score_els,
        ma_eg_score_els,
        ma_eg_status_els,
        ma_eg_index_els,
        ma_spg_grade_swd,
        ma_spg_score_swd,
        mags_ach_score_swd,
        ma_eg_score_swd,
        ma_eg_status_swd,
        ma_eg_index_swd
    from {{ ref('int__spg_subgroup_wide') }}
	where
		school_code not like '%LEA%'
),

enrollment as (
    select
        concat(LEA_use, School_use) as school_code_enrollment, -- double check that i'm using the right columns here
        reporting_year as reporting_year_enrollment,
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
	where
		concat(LEA_use, School_use) not like '%LEA%'
),

--performance as (
--    select
--        reporting_year,
--        school_code,
--        subject,
--        grade,
--        type,
--        all_num_tested,
--        all_pct_notprof,
--        all_pct_l3,
--        all_pct_l4,
--        all_pct_l5,
--        all_pct_ccr,
--        all_pct_glp,
--        blck_num_tested,
--        blck_pct_notprof,
--        blck_pct_l3,
--        blck_pct_l4,
--        blck_pct_l5,
--        blck_pct_ccr,
--        blck_pct_glp,
--        eds_num_tested,
--        eds_pct_notprof,
--        eds_pct_l3,
--        eds_pct_l4,
--        eds_pct_l5,
--        eds_pct_ccr,
--        eds_pct_glp,
--        els_num_tested,
--        els_pct_notprof,
--        els_pct_l3,
--        els_pct_l4,
--        els_pct_l5,
--        els_pct_ccr,
--        els_pct_glp,
--        hisp_num_tested,
--        hisp_pct_notprof,
--        hisp_pct_l3,
--        hisp_pct_l4,
--        hisp_pct_l5,
--        hisp_pct_ccr,
--        hisp_pct_glp,
--        swd_num_tested,
--        swd_pct_notprof,
--        swd_pct_l3,
--        swd_pct_l4,
--        swd_pct_l5,
--        swd_pct_ccr,
--        swd_pct_glp,
--        whte_num_tested,
--        whte_pct_notprof,
--        whte_pct_l3,
--        whte_pct_l4,
--        whte_pct_l5,
--        whte_pct_ccr,
--        whte_pct_glp
--    from {{ ref('int__perf_disagg_wide') }}
--)

eds as (

	select
		school_code as school_code_eds,
		reporting_year as reporting_year_eds,
		school_name as school_name_eds,
		den as student_school_enrollment_eds,
		pct_eds,
		pct_nslp,
		pct_nslp_adj
	from {{ ref('stg__eds') }}
	where
		school_code not like '%LEA%'
		and school_code not like '%SEA%'
),

-- CTE #1
act_sch_and_eds as (

	select * from active_schools full outer join eds ON active_schools.school_code_active_schools = eds.school_code_eds

),

-- CTE #2
act_sch_eds_and_spg as (

	select * from act_sch_and_eds left join spg ON act_sch_and_eds.school_code_eds = spg.school_code_spg

),

-- CTE #3
act_sch_eds_spg_and_enroll as (

	select * from act_sch_eds_and_spg left join enrollment ON act_sch_eds_and_spg.school_code_spg = enrollment.school_code_enrollment
																		AND act_sch_eds_and_spg.reporting_year_spg = enrollment.reporting_year_enrollment

)

select
 	*,
	concat("C-",school_code_active_schools) as school_code_tag
from act_sch_eds_spg_and_enroll
where
	school_code_active_schools is not null





--select
--    schools.*,
--    panel_data.reporting_year as panel_reporting_year,
--    panel_data.* except(school_code, reporting_year)
--from
--    schools
--    left join (
--        select
--            coalesce(enrollment.school_code, spg.school_code, performance.school_code) as school_code,
--            coalesce(enrollment.reporting_year, spg.reporting_year, performance.reporting_year) as reporting_year,
--            enrollment.* except(school_code, reporting_year),
--            spg.* except(school_code, reporting_year),
--            performance.* except(school_code, reporting_year)
--        from
--            enrollment
--            full outer join spg using (school_code, reporting_year)
--            full outer join performance using (school_code, reporting_year)
--    ) as panel_data on schools.school_code = panel_data.school_code
-- staging model that stacks, with specific columns, traditional and charter school enrollment by race and gender

-- staging model that stacks, with specific columns, traditional and charter school enrollment by race and gender

with traditional as (

    select
        CASE
			when length(LEA) = 1 then concat("00",LEA)
			when length(LEA) = 2 then concat("0",LEA)
			else LEA
		END as LEA_use,
		LEA,
		CASE
			when length(School) = 1 then concat("00",School)
			when length(School) = 2 then concat("0",School)
			else School
		END as School_use,
        School,
        ____LEA_Name____ as district_name,
        ___School_Name___ as school_name,
        Indian_Male,
        Indian_Female,
        Asian_Male,
        Asian_Female,
        Hispanic_Male,
        Hispanic_Female,
        Black_Male,
        Black_Female,
        White_Male,
        White_Female,
        Pacific_Island_Female,
        Pacific_Island_Male,
        Two_or__More_Male,
        Two_or__More_Female,
        Total,
        reporting_year,
        school_type
    from {{ ref('stg__enrollment_traditional_race_gender') }}

),

charter as (

    select
        CS as LEA_use,
		CS as LEA,
		"000" as School_use,
        '000' as School,
        'Charter LEA' as district_name,
        Charter_School_Name as school_name,
        INDIANMale as Indian_Male,
        INDIANFemale as Indian_Female,
        ASIANMale as Asian_Male,
        ASIANFemale as Asian_Female,
        HISPANICMale as Hispanic_Male,
        HISPANICFemale as Hispanic_Female,
        BLACKMale as Black_Male,
        BLACKFemale as Black_Female,
        WHITEMale as White_Male,
        WHITEFemale as White_Female,
        PACIFICISLANDMale as Pacific_Island_Male,
        PACIFICISLANDFemale as Pacific_Island_Female,
        TWO_OR_MORE_RACESMale as Two_or__More_Male,
        TWO_OR_MORE_RACESFemale as Two_or__More_Female,
        Total,
        reporting_year,
        school_type
    from {{ ref('stg__enrollment_charter_race_gender') }}

)

select * from traditional
UNION ALL
select * from charter
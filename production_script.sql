/* This script is for creating development sample for ALN buildings.*/
with building_profile as (
    select --155.5k
         a."ApartmentId" buildingid
        --,a."Id"
        ,max(c."NumUnits") units
        ,max(a."GPSLatitude") latitude
        ,max(a."GPSLongitude") longitude
        ,max(round(case when try_to_number(b.addresszip::text) is not null then b.addresszip::text else
            try_to_number(substr(b.addresszip::text, 0, position('-' in b.addresszip::text) - 1)) end)) zip
        ,max(b.addresszip) zip_original
        ,max(b.addressstate) state
        ,max(b.addresscity) city
        ,max(b.addressline1) street
        ,max(c."YearBuilt") year_built
        ,max(c."AptName") name
        ,max(c."CorporateManagementCompanyId") CorporateManagementCompanyId
        ,max(c."Occupancy") Occupancy
        ,max(c."NumberOfStories") NumberOfStories 
        ,max(c."AverageRent") AverageRent
        ,max(c."AverageSqFt") AverageSqFt
    from ext_data.aln_data."ApartmentGeoLocationExtension" a
    join "EXT_DATA"."ALN_DATA".vw_propertyaddress b on a."ApartmentId" = b.apartmentid
    join "EXT_DATA"."ALN_DATA"."ApartmentPropertyExtension" c on a."ApartmentId" = c."ApartmentId"
    join "EXT_DATA"."ALN_DATA"."ApartmentStatus" d on a."ApartmentId" = d."ApartmentId"
    where 1=1
        and b.addresstype = 'Physical Address'
        and b.isprimary = 'Y'
        and d."Status" not in (15, 22, 86, 99) --filtering out unnecessary buildings: 15 - duplicate/discarded, 22 - for sale, 86 and 99 - closed (inactive)
        and a."ApartmentId" not in (select "InactiveEntityId" from ext_data.aln_data."InactiveEntity") 
        --and a."Id" between 13000 and 16000 --for subsetting purposes. used with dynamic string interpolation in databricks.
        and b.addresszip is not null
    group by buildingid
)
/* From this point on, every WITH statement is for gathering features. Development sample is already defined in
   previous WITH statements. First set for features is college distance data. 
   Source for this table comes from snowflake marketplace. Definitions for the columns are not available in snowflake.
   Column definitions were gathered from external source and are loaded into the google drive:
   https://drive.google.com/file/d/1zyEfFPXRjirW8HW7BPb_Kx6JTcf9uxG6/view?usp=sharing 
   In this sql snippet, every building is joined to every college to calculate each possible distance.
   Distance is calculated using HAVERSINE inbuilt function.
   Note: College data is assumed to be static and might be updated once a year (or less frequently)
*/
, colleges_distances as (
    select 
        a.buildingid building_id 
        ,a.latitude
        ,a.longitude
        ,b.instnm
        ,b.latitude
        ,b.longitude
        ,b.highdeg -- 0 - nondegree, 1-certificate, 2-associates degree, 3-bachelors degree, 4-graduate
        -- CCSIZSET comes from carnegie classification: https://carnegieclassifications.iu.edu/classification_descriptions/size_setting.php
        ,case when b.CCSIZSET in (1,2,6,7,8,9,10,11) then 'small'
            when b.CCSIZSET in (3,12,13,14) then 'medium'
            when b.CCSIZSET in (4,5,15,16,17) then 'large' end college_size 
        ,case when b.CCSIZSET in (6,9,12,15) then 'nonresidential'
            when b.CCSIZSET in (7,10,13,16) then 'residential'
            when b.CCSIZSET in (8,11,14,17) then 'highly residential' end college_setting
        ,ifnull(b.COSTT4_A,b.COSTT4_P) avg_cost_1year
        ,b.C150_4 completion_rate-- Completion rate for first-time, full-time students at four-year institutions
        ,round(haversine(a.latitude, a.longitude, b.latitude, b.longitude),2) distance_km
    from building_profile  a
    left join ext_college_scorecard.college_scorecard.merged2018_19_pp b 
    where 1=1
        and b.latitude is not null 
        and b.longitude is not null
        and b.CURROPER = 1 -- operational
)
/* Contains college features per building.
   Features include both counts of coleges in specific set of radii as well as 
   drill down of what kind of colleges these are, what are the costs, complition rates and sizes.
   distance ranges were chosen by discussing with the team and subjective opinion. 
*/
, building_college as (
select 
    a.building_id
    ,min(distance_km) closest_college_km
    ,sum(case when distance_km <= 1 then 1 else 0 end) college_cnt_1km
    ,sum(case when distance_km <= 2 then 1 else 0 end) college_cnt_2km
    ,sum(case when distance_km <= 5 then 1 else 0 end) college_cnt_5km
    ,sum(case when distance_km <= 10 then 1 else 0 end) college_cnt_10km
    ,sum(case when distance_km <= 25 then 1 else 0 end) college_cnt_25km
    ,sum(case when distance_km <= 50 then 1 else 0 end) college_cnt_50km
    ,sum(case when distance_km <= 75 then 1 else 0 end) college_cnt_75km
    
    ,sum(case when distance_km <= 1 and highdeg = 4 then 1 else 0 end) graduate_deg_uni_cnt_1km
    ,sum(case when distance_km <= 2 and highdeg = 4 then 1 else 0 end) graduate_deg_uni_cnt_2km
    ,sum(case when distance_km <= 5 and highdeg = 4 then 1 else 0 end) graduate_deg_uni_cnt_5km
    ,sum(case when distance_km <= 10 and highdeg = 4 then 1 else 0 end) graduate_deg_uni_cnt_10km
    ,sum(case when distance_km <= 25 and highdeg = 4 then 1 else 0 end) graduate_deg_uni_cnt_25km
    ,sum(case when distance_km <= 50 and highdeg = 4 then 1 else 0 end) graduate_deg_uni_cnt_50km
    
    ,sum(case when distance_km <= 1 and highdeg = 3 then 1 else 0 end) bachelors_deg_uni_cnt_1km
    ,sum(case when distance_km <= 2 and highdeg = 3 then 1 else 0 end) bachelors_deg_uni_cnt_2km
    ,sum(case when distance_km <= 5 and highdeg = 3 then 1 else 0 end) bachelors_deg_uni_cnt_5km
    ,sum(case when distance_km <= 10 and highdeg = 3 then 1 else 0 end) bachelors_deg_uni_cnt_10km
    ,sum(case when distance_km <= 25 and highdeg = 3 then 1 else 0 end) bachelors_deg_uni_cnt_25km
    ,sum(case when distance_km <= 50 and highdeg = 3 then 1 else 0 end) bachelors_deg_uni_cnt_50km
    
    ,sum(case when distance_km <= 1 and highdeg <= 2 then 1 else 0 end) other_deg_uni_cnt_1km
    ,sum(case when distance_km <= 2 and highdeg <= 2 then 1 else 0 end) other_deg_uni_cnt_2km
    ,sum(case when distance_km <= 5 and highdeg <= 2 then 1 else 0 end) other_deg_uni_cnt_5km
    ,sum(case when distance_km <= 10 and highdeg <= 2 then 1 else 0 end) other_deg_uni_cnt_10km
    ,sum(case when distance_km <= 25 and highdeg <= 2 then 1 else 0 end) other_deg_uni_cnt_25km
    ,sum(case when distance_km <= 50 and highdeg <= 2 then 1 else 0 end) other_deg_uni_cnt_50km
    
    ,sum(case when distance_km <= 1 and college_size = 'small' then 1 else 0 end) small_college_cnt_1km
    ,sum(case when distance_km <= 2 and college_size = 'small' then 1 else 0 end) small_college_cnt_2km
    ,sum(case when distance_km <= 5 and college_size = 'small' then 1 else 0 end) small_college_cnt_5km
    ,sum(case when distance_km <= 10 and college_size = 'small' then 1 else 0 end) small_college_cnt_10km
    
    ,sum(case when distance_km <= 1 and college_size = 'medium' then 1 else 0 end) medium_college_cnt_1km
    ,sum(case when distance_km <= 2 and college_size = 'medium' then 1 else 0 end) medium_college_cnt_2km
    ,sum(case when distance_km <= 5 and college_size = 'medium' then 1 else 0 end) medium_college_cnt_5km
    ,sum(case when distance_km <= 10 and college_size = 'medium' then 1 else 0 end) medium_college_cnt_10km
    
    ,sum(case when distance_km <= 1 and college_size = 'large' then 1 else 0 end) large_college_cnt_1km
    ,sum(case when distance_km <= 2 and college_size = 'large' then 1 else 0 end) large_college_cnt_2km
    ,sum(case when distance_km <= 5 and college_size = 'large' then 1 else 0 end) large_college_cnt_5km
    ,sum(case when distance_km <= 10 and college_size = 'large' then 1 else 0 end) large_college_cnt_10km
    
    ,sum(case when distance_km <= 1 and college_setting = 'residential' then 1 else 0 end) residental_college_cnt_1km
    ,sum(case when distance_km <= 2 and college_setting = 'residential' then 1 else 0 end) residental_college_cnt_2km
    ,sum(case when distance_km <= 5 and college_setting = 'residential' then 1 else 0 end) residental_college_cnt_5km
    ,sum(case when distance_km <= 10 and college_setting = 'residential' then 1 else 0 end) residental_college_cnt_10km
    
    ,round(avg(case when distance_km <= 1 then avg_cost_1year end), 2) avg_college_cost_1km
    ,round(avg(case when distance_km <= 2 then avg_cost_1year end), 2) avg_college_cost_2km
    ,round(avg(case when distance_km <= 5 then avg_cost_1year end), 2) avg_college_cost_5km
    ,round(avg(case when distance_km <= 10 then avg_cost_1year end), 2) avg_college_cost_10km
    ,round(avg(case when distance_km <= 50 then avg_cost_1year end), 2) avg_college_cost_50km
    
    ,round(avg(case when distance_km <= 1 then completion_rate end), 2) avg_college_completion_rate_1km
    ,round(avg(case when distance_km <= 2 then completion_rate end), 2) avg_college_completion_rate_2km
    ,round(avg(case when distance_km <= 5 then completion_rate end), 2) avg_college_completion_rate_5km
    ,round(avg(case when distance_km <= 10 then completion_rate end), 2) avg_college_completion_rate_10km
    ,round(avg(case when distance_km <= 50 then completion_rate end), 2) avg_college_completion_rate_50km
    
    ,graduate_deg_uni_cnt_50km - graduate_deg_uni_cnt_25km graduate_deg_uni_cnt_25_to_50km
    ,graduate_deg_uni_cnt_25km - graduate_deg_uni_cnt_10km graduate_deg_uni_cnt_10_to_25km
from colleges_distances a
group by a.building_id
)
/* Unemployment data gathered by risk team. 
   Data is filtered to include rows up to and including May 2020. Reason for this is
   we want to have as little intersection with the taget period as possibe, to minimize
   risk of using future variables (meaning using future to predict future, when in reality we should use past to predict future).
   Remember, application approval statuses are collected from June 2020.
   More information about what msa codes are can be found here: https://www.investopedia.com/terms/m/msa.asp
*/
, unemployment as (
    select 
         f.msa_code
        ,round(avg(unemployment_rate), 2) avg_unemployment_rate_1y
    from ML.RISK_MODEL_TRAINING.unemployment f 
    where 1=1 
    and (f.year = 2020 and f.month <= 5) or (f.year = 2019 and f.month > 5)
    group by f.msa_code
)
/* Hospital data is also assumed as static (like college data). 
   Data was found online: https://hifld-geoplatform.opendata.arcgis.com/datasets/geoplatform::hospitals-1/about
   it was loaded as .csv from dbt.
*/
, hospital_distances as (
    select
         a.buildingid building_id 
        ,a.latitude
        ,a.longitude
        ,b.latitude
        ,b.longitude
        ,case when b.type = 'GENERAL ACUTE CARE' then 1 else 0 end general_acute_care_hospital
        ,case when b.owner like '%GOVERNMENT%' then 1 else 0 end government_hospital
        ,b.beds -- gives idea about size of hospital
        ,round(haversine(a.latitude, a.longitude, b.latitude, b.longitude),2) distance_km
    from building_profile a
    left join reports.dbt_bchikvinidze.hospitals b 
    where 1=1
        and b.status = 'OPEN'      
)
/* Feature creation logic is similar to college data feature creation logic.
*/
, building_hospital as (
    select
         a.building_id
        ,min(distance_km) closest_hospital_km
        ,sum(case when distance_km <= 2 then 1 else 0 end) hospital_cnt_2km
        ,sum(case when distance_km <= 5 then 1 else 0 end) hospital_cnt_5km
        ,sum(case when distance_km <= 10 then 1 else 0 end) hospital_cnt_10km
        ,sum(case when distance_km <= 50 then 1 else 0 end) hospital_cnt_50km
        
        ,sum(case when distance_km <= 2 and general_acute_care_hospital = 1 then 1 else 0 end) general_acute_care_hospital_cnt_2km
        ,sum(case when distance_km <= 5 and general_acute_care_hospital = 1 then 1 else 0 end) general_acute_care_hospital_cnt_5km
        ,sum(case when distance_km <= 10 and general_acute_care_hospital = 1 then 1 else 0 end) general_acute_care_hospital_cnt_10km
        
        ,sum(case when distance_km <= 2 and government_hospital = 1 then 1 else 0 end) government_hospital_cnt_2km
        ,sum(case when distance_km <= 5 and government_hospital = 1 then 1 else 0 end) government_hospital_cnt_5km
        ,sum(case when distance_km <= 10 and government_hospital = 1 then 1 else 0 end) government_hospital_cnt_10km
        
        ,round(avg(case when distance_km <= 2 and a.beds > 0 then a.beds end)) hospital_avg_beds_2km
        ,round(avg(case when distance_km <= 5 and a.beds > 0 then a.beds end)) hospital_avg_beds_5km
        ,round(avg(case when distance_km <= 10 and a.beds > 0 then a.beds end)) hospital_avg_beds_10km
    from hospital_distances a
    group by a.building_id
)
/* Source of this WITH is csv that I got from michael (originally from tyler), then used dbt for importing.
   Note that this file containes dublicates, meaning one zip could correspond to more than one cbsa.
   Aggregate function was used to avoid dupes (3.6k dupe out of total of 32.3k). 
   For now there is no way to decide exactly which cbsa code should be used in case of dupes so I'm sticking with
   max function for simplicity. 
*/
, cbsa_zip_mapping as (
    select 
         zip
        ,max(cbsa) cbsa_code
    from reports.dbt_bchikvinidze.cbsa_zip_mapping -- 
    group by zip -- I have some dupes, reason unknown. That's why I'm taking maximum of cbsa codes
)
/* Per capita income is gathered by risk team. source: – BEA, CAINC1 Personal Income Summary: https://www.bea.gov/data/income-saving/personal-income-county-metro-and-other-areas
   reason for grouping is again to avoid dupes (this one had just one dupe)
*/
, per_capita_income as (
    select 
         a.cbsa_code
        ,max(a.per_capita_income) per_capita_income
    from ml.risk_model_training.per_capita_income a
    where a.year = 2020
    group by a.cbsa_code
    having count(1)=1 -- I moving this table to 'with' statement because there is one dupe
)
/* Law enforcement data is handled as static data, like college and hospital data.
   original source is here: https://hifld-geoplatform.opendata.arcgis.com/datasets/local-law-enforcement-locations/explore?location=36.557550%2C-76.088187%2C3.86&showTable=true
*/
, law_enforcement_distances as (
    select 
         a.buildingid building_id 
        ,round(haversine(a.latitude, a.longitude, b.latitude, b.longitude),2) distance_km
    from building_profile  a
    left join reports.dbt_bchikvinidze.law_enforcement_locations b
    where 1=1
    and b.status = 'OPEN'
)
/* Structure for law enforcement features is same as for college and hospital features.*/
, building_law_enforcement as (
    select
         a.building_id
        ,min(distance_km) closest_law_enforcement_km
        ,sum(case when distance_km <= 2 then 1 else 0 end) law_enforcement_cnt_2km
        ,sum(case when distance_km <= 5 then 1 else 0 end) law_enforcement_cnt_5km
        ,sum(case when distance_km <= 10 then 1 else 0 end) law_enforcement_cnt_10km
    from law_enforcement_distances a
    group by a.building_id
)
/* Public and private school data. original source is from:
   https://hifld-geoplatform.opendata.arcgis.com/datasets/private-schools-1/about
   https://hifld-geoplatform.opendata.arcgis.com/datasets/geoplatform::public-schools/about
   These tables did not have identical columns, so inference was needed to identify private elementary schools.
   "level_" = 1 in private schools table was handled as elementary school by comparing level distribution with public school level_ distribution.
*/
, schools_union as (
    select
         a.objectid
        ,a.latitude
        ,a.longitude
        ,a.enrollment
        ,case when a.ft_teacher <> 0 then a.enrollment/a.ft_teacher end student_per_teacher
        ,'PUBLIC' type
        ,case when level_ in ('ELEMENTARY', 'PREKINDERGARTEN') then 'ELEMENTARY' end level
    from reports.dbt_bchikvinidze.public_schools a
    
    UNION ALL
    
    select 
         b.fid as objectid
        ,b.latitude
        ,b.longitude
        ,b.enrollment
        ,case when b.ft_teacher <> 0 then b.enrollment/b.ft_teacher end student_per_teacher
        ,'PRIVATE' type
        ,case when level_ = 1 then 'ELEMENTARY' end level
    from reports.dbt_bchikvinidze.private_schools b
)
/* Intermediary table to calculate distance between all pairs of buildings and schools*/
, schools_distances as (
    select
         a.buildingid building_id 
        ,b.type
        ,b.level
        ,b.objectid
        ,round(haversine(a.latitude, a.longitude, b.latitude, b.longitude), 2) distance_km 
    from building_profile  a
    join schools_union b
)
/* Features here are calculated by the same style as other POI features (college,hospital,law enforcement)
   This time shorter distances are considered (not more than 10KM) because, as opposed to far-away colleges,
   kids usually go to nearby school.
*/
, building_schools as ( -- takes 4.5 minutes
    select
         a.building_id
        ,min(case when a.type = 'PRIVATE' then distance_km end) closest_private_school_km
        ,min(case when a.type = 'PUBLIC' then distance_km end) closest_public_school_km
    
        ,min(case when a.level = 'ELEMENTARY' then distance_km end) closest_private_elem_school_km
        ,min(case when a.level = 'ELEMENTARY' then distance_km end) closest_public_elem_school_km
    
        ,sum(case when distance_km <= 2 and a.type = 'PRIVATE' then 1 else 0 end) private_school_cnt_2km
        ,sum(case when distance_km <= 5 and a.type = 'PRIVATE' then 1 else 0 end) private_school_cnt_5km
        ,sum(case when distance_km <= 10 and a.type = 'PRIVATE' then 1 else 0 end) private_school_cnt_10km
    
        ,sum(case when distance_km <= 2 and a.type = 'PUBLIC' then 1 else 0 end) public_school_cnt_2km
        ,sum(case when distance_km <= 5 and a.type = 'PUBLIC' then 1 else 0 end) public_school_cnt_5km
        ,sum(case when distance_km <= 10 and a.type = 'PUBLIC' then 1 else 0 end) public_school_cnt_10km
    from schools_distances a
    group by a.building_id
)
/* collected by risk team. source from census:  ACS B25031 and ACS B25063
   Aggregate function is used because sometimes more than one area corresponds to one msa code.
*/
, gross_rent as (
    select
     a.msa_code
    ,a.year
    ,avg(a.median_gross_rent_all_bedrooms) median_gross_rent_all_bedrooms
    ,avg(a.median_gross_rent_no_bedrooms) median_gross_rent_no_bedrooms
    ,avg(a.median_gross_rent_one_bedroom) median_gross_rent_one_bedroom
    from ML.RISK_MODEL_TRAINING.gross_rent a
    --where year = 2021
    group by a.msa_code, a.year
)
/* intermediary table for more feature engineering with POI data.
   Result of this WITH is counts by zip code and POI.
*/
, points_of_interest_per_zip as (
    select try_to_number(zip) zip, count(1) cnt, 'law_enforcement' poi from reports.dbt_bchikvinidze.law_enforcement_locations group by zip
    union all
    select try_to_number(zip) zip, count(1) cnt, 'private_schools' poi from reports.dbt_bchikvinidze.private_schools  group by zip
    union all
    select try_to_number(zip) zip, count(1) cnt, 'public_schools' poi from reports.dbt_bchikvinidze.public_schools  group by zip
    union all
    select try_to_number(zip) zip, count(1) cnt, 'hospitals' poi from  reports.dbt_bchikvinidze.hospitals  group by zip
    union all
    select try_to_number(zip) zip, count(1) cnt, 'colleges' poi from ext_college_scorecard.college_scorecard.merged2018_19_pp   group by zip
)
/* count of each POI category in zip */
, poi_per_zip as (
    select 
        a.zip
        ,sum(case when poi = 'law_enforcement' then cnt end) law_enforcement_cnt
        ,sum(case when poi = 'private_schools' then cnt end) private_school_cnt
        ,sum(case when poi = 'public_schools' then cnt end) public_school_cnt
        ,sum(case when poi = 'hospitals' then cnt end) hospital_cnt
        ,sum(case when poi = 'colleges' then cnt end) college_cnt
    from points_of_interest_per_zip a
    group by a.zip
)
/* ALN data: intermediary data for Amenity. contains amenity counts per apartment building. 
   There are 62 different amenities which are divided into 13 amenity groups in the database, but for
   our purposes I have combined these groups that have similar content, to avoid having to add too many new variables
   to my already small dataset.
   The view VW_APTAMENITY is created in such a way that every apartment has every type of amenity joined to it, but further filters
   are required to actually determine which amenity is active for a building. This is why "value" column is used in filters, as well as "qty"
   column. Testing if these filters were correct was done by opening ALN webpage and checking a few cases. 
*/
, aln_amenity as (
    select 
         a.apartmentid
        ,sum(case when c."GroupName" in ('Activity/Lifestyle') then 1 else 0 end) lifestyle_amenity_cnt
        ,sum(case when c."GroupName" in ('Bath', 'Kitchen', 'Floorplan') then 1 else 0 end) household_amenity_cnt
        ,sum(case when c."GroupName" in ('Community', 'Internet/Broadband', 'Services') then 1 else 0 end) service_amenity_cnt
        ,sum(case when c."GroupName" in ('Parking', 'Location') then 1 else 0 end) transport_related_amenity_cnt
        ,sum(case when c."GroupName" in ('Precautionary Measures') then 1 else 0 end) safety_amenity_cnt
        ,sum(case when c."GroupName" in ('Utilities', 'Property Allocated Expenses', 'Waste Disposal') then 1 else 0 end) utility_amenity_cnt
    from  EXT_DATA.ALN_DATA.VW_APTAMENITY a
    join "EXT_DATA"."ALN_DATA"."Amenity" b on a.amenityid = b."AmenityId"
    join "EXT_DATA"."ALN_DATA"."AmenityGroup" c on c."AmenityGroupId" = b."AmenityGroupId"
    where (a.qty > 0 or a.value in ('Y', '*', 'Included'))
    group by a.apartmentid
)
/* WITH statement to collect some basic variables from ALN buildings as well as filter dupes (600 dupes at the time of writing)
   Filter of incative apartmentIDs was not used as it did not result in decrease of rows.
   some apartment statuses were removed for obvious reasons, like in case of duplicate entry, being closed or for sale, rather than rent.
*/
, aln_unique_property as ( -- this WITH is just for removing dublicates from ApartmentPropertyExtension (600 dupes)
    select 
         a."ApartmentId" ApartmentId
        ,max(a."CorporateManagementCompanyId") CorporateManagementCompanyId
        ,max(a."NumUnits") NumUnits
        ,max(a."YearBuilt") YearBuilt
        ,max(a."Occupancy") Occupancy
        ,max(a."NumberOfStories") NumberOfStories
        ,max(a."AverageRent") AverageRent
        ,max(a."AverageSqFt") AverageSqFt
    from "EXT_DATA"."ALN_DATA"."ApartmentPropertyExtension" a
    join "EXT_DATA"."ALN_DATA"."ApartmentStatus" b on a."ApartmentId" = b."ApartmentId"
    where b."Status" not in (15, 22, 86, 99) --filtering out unnecessary buildings: 15 - duplicate/discarded, 22 - for sale, 86 and 99 - closed (inactive)
     --and a."ApartmentId" not in (select "InactiveEntityId" from ext_data.aln_data."InactiveEntity") -- this filter did not result in count decrease of rows
    group by a."ApartmentId"
)
/* self explanatory. is used to identify state and zip of management company.
   Group by used to avoid a few dupes (at this time, 4 were found)
*/
, mgmt_co_address as (
    select 
         a."ManagementCompanyEntityId" ManagementCompanyEntityId
        ,max(a."AddressState") AddressState
        ,max(a."AddressZIP") AddressZIP
    from "EXT_DATA"."ALN_DATA"."MgmtCoAddress" a
    where 1=1
        and "IsPrimary" = 'Y' 
        and "AddressType" = 'Physical'
    group by a."ManagementCompanyEntityId"
)
/*  Zip code where ALN management company has most of its units located.
    "qualify" function makes it possible to filter by window function, without needing another WITH statement.
    zip code needs some altering as it sometimes includes more numbers after zip code in "addresszip" column.
*/
, units_zip_management_company as (
    select 
         a.CorporateManagementCompanyId
        ,c."MgmtOfficeIntegerId"
        ,sum(try_to_number(a.NumUnits)) total_units
        ,round(case when try_to_number(b.addresszip) is not null then b.addresszip else
            try_to_number(substr(b.addresszip, 0, position('-' in b.addresszip) - 1)) end) zip
    from aln_unique_property a
    join "EXT_DATA"."ALN_DATA"."ManagementCompany" c on c."ManagementCompanyEntityId" = a.CorporateManagementCompanyId
    join "EXT_DATA"."ALN_DATA".vw_propertyaddress b on b.apartmentid = a.ApartmentId
    where b.addresstype = 'Physical Address'
    group by a.CorporateManagementCompanyId, c."MgmtOfficeIntegerId", zip, addresszip
    qualify row_number() over (partition by a.CorporateManagementCompanyId order by total_units desc) = 1
)
/* Handles most of the feature engineering using ALN management company data.
*/
, management_company_by_corporate as (
    select --11.5k companies
         a.CorporateManagementCompanyId
        ,c."ManagementCompanyEntityId"
        ,count(distinct a.apartmentid) ll_apartment_cnt
        ,round(avg(try_to_number(a.Numunits)),1) ll_avg_units
        ,median(try_to_number(a.Numunits)) ll_median_units
        ,PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY try_to_number(a.Numunits)) ll_1st_quartile_units
        ,PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY try_to_number(a.Numunits)) ll_3rd_quartile_units
        ,2022 - min(try_to_number(a.YearBuilt)) ll_newest_years_ago
        ,2022 - max(try_to_number(a.YearBuilt)) ll_oldest_years_ago
        ,round(avg(2022 - try_to_number(a.YearBuilt)),1) ll_avg_years_ago
        ,round(avg(try_to_number(a.Occupancy)),1) ll_avg_occupancy
        ,round(avg(try_to_number(a.NumberOfStories)),1) ll_avg_stories
        ,round(avg(case when try_to_number(a.AverageRent) > 0 then try_to_number(a.AverageRent) end),1) ll_avg_rent
        ,round(avg(case when try_to_number(a.AverageSqFt) > 0 then try_to_number(a.AverageSqFt) end),1) ll_avg_sqft
        ,round(sum(case when try_to_number(a.AverageRent) > 0 then try_to_number(a.AverageRent) end) / 
            sum(case when try_to_number(a.AverageSqFt) > 0 then try_to_number(a.AverageSqFt) end),1) ll_avg_rent_over_unit_area
        ,round(count(case when b."IncomeRestricted" = 'Y' then 1 end)/ll_apartment_cnt,3) ll_income_restricted_apartment_pct
        ,round(count(case when b."Section8" = 'Y' then 1 end)/ll_apartment_cnt,3) ll_section8_apartment_pct
        ,round(avg(try_to_number(b."ApplicationFee")),1) ll_avg_application_fee
        ,round(count(case when b."SeniorLiving" = 'Y' then 1 end)/ll_apartment_cnt, 2) ll_senior_housing_pct
        ,round(count(case when b."AssistedLiving" = 'Y' then 1 end)/ll_apartment_cnt, 2) ll_assisted_living_pct
        ,round(count(case when b."StudentHousing" = 'Y' then 1 end)/ll_apartment_cnt, 2) ll_student_housing_pct
        ,round(count(case when b."ShortTerm" = 'Y' then 1 end)/ll_apartment_cnt, 2) ll_short_term_housing_pct
        ,round(count(case when d."MaxNumPets" > 0 then 1 end)/ll_apartment_cnt, 2) ll_pet_allowed_housing_pct
        ,max(e.AddressState) ll_AddressState
        ,max(e.AddressZIP) ll_AddressZIP
        ,max(g.zip) ll_most_units_in_zip
        --,try_to_number(substr(f."NumberOfUnits", 0, position(' ' in f."NumberOfUnits")-1)) new_units_construction_cnt
        ,round(sum(h.lifestyle_amenity_cnt)/ll_apartment_cnt,1) avg_ll_lifestyle_amenity_cnt_per_building 
        ,round(sum(h.household_amenity_cnt)/ll_apartment_cnt,1) avg_ll_household_amenity_cnt_per_building 
        ,round(sum(h.service_amenity_cnt)/ll_apartment_cnt,1) avg_ll_service_amenity_cnt_per_building
        ,round(sum(h.transport_related_amenity_cnt)/ll_apartment_cnt,1) avg_ll_transport_related_amenity_cnt_per_building
        ,round(sum(h.safety_amenity_cnt)/ll_apartment_cnt,1) avg_ll_safety_amenity_cnt_per_building
        ,round(sum(h.utility_amenity_cnt)/ll_apartment_cnt,1) avg_ll_utility_amenity_cnt_per_building
    from aln_unique_property a
    left join "EXT_DATA"."ALN_DATA"."ManagementCompany" c on c."ManagementCompanyEntityId" = a.CorporateManagementCompanyId
    left join "EXT_DATA"."ALN_DATA"."ApartmentLeasingExtension" b on b."ApartmentId" = a.apartmentid
    left join "EXT_DATA"."ALN_DATA"."ApartmentPetExtension" d on d."ApartmentId" = a.apartmentid
    left join mgmt_co_address e on e.ManagementCompanyEntityId = c."ManagementCompanyEntityId"
    left join "EXT_DATA"."ALN_DATA"."NewConstruction" f on f."ApartmentId" = a.apartmentid
    left join units_zip_management_company g on g.CorporateManagementCompanyId = a.CorporateManagementCompanyId
    left join aln_amenity h on h.apartmentid = b."ApartmentId"
    where 1=1
        and a.apartmentid is not null
        and a.CorporateManagementCompanyId is not null
    group by a.CorporateManagementCompanyId, c."ManagementCompanyEntityId"
)
/* joining of dw building with ALN management company features.
   There should not be need to use aggregate function but is used just in case to avoid dupes.
*/
, building_company_aln as (
   select --5.2k buildings matched... not great not terribe (c) chernobyl
         a.buildingid
        ,max(b."ManagementCompanyEntityId") ManagementCompanyEntityId
        ,max(b.ll_apartment_cnt) ll_apartment_cnt
        ,max(b.ll_avg_units) ll_avg_units
        ,max(b.ll_median_units) ll_median_units
        ,max(b.ll_1st_quartile_units) ll_1st_quartile_units
        ,max(b.ll_3rd_quartile_units) ll_3rd_quartile_units
        ,max(b.ll_newest_years_ago) ll_newest_years_ago
        ,max(b.ll_oldest_years_ago) ll_oldest_years_ago
        ,max(b.ll_avg_years_ago) ll_avg_years_ago
        ,max(b.ll_avg_occupancy) ll_avg_occupancy
        ,max(b.ll_avg_stories) ll_avg_stories
        ,max(b.ll_avg_rent) ll_avg_rent
        ,max(b.ll_avg_sqft) ll_avg_sqft
        ,max(b.ll_avg_rent_over_unit_area) ll_avg_rent_over_unit_area
        ,max(b.ll_income_restricted_apartment_pct) ll_income_restricted_apartment_pct
        ,max(b.ll_section8_apartment_pct) ll_section8_apartment_pct
        ,max(b.ll_avg_application_fee) ll_avg_application_fee
        ,max(b.ll_senior_housing_pct) ll_senior_housing_pct
        ,max(b.ll_assisted_living_pct) ll_assisted_living_pct
        ,max(b.ll_student_housing_pct) ll_student_housing_pct
        ,max(b.ll_short_term_housing_pct) ll_short_term_housing_pct
        ,max(b.ll_pet_allowed_housing_pct) ll_pet_allowed_housing_pct
        ,max(b.ll_AddressState) ll_AddressState
        ,max(b.ll_AddressZIP) ll_AddressZIP
        ,max(b.ll_most_units_in_zip) ll_most_units_in_zip
        ,max(b.avg_ll_lifestyle_amenity_cnt_per_building) avg_ll_lifestyle_amenity_cnt_per_building
        ,max(b.avg_ll_household_amenity_cnt_per_building) avg_ll_household_amenity_cnt_per_building
        ,max(b.avg_ll_service_amenity_cnt_per_building) avg_ll_service_amenity_cnt_per_building
        ,max(b.avg_ll_transport_related_amenity_cnt_per_building) avg_ll_transport_related_amenity_cnt_per_building
        ,max(b.avg_ll_safety_amenity_cnt_per_building) avg_ll_safety_amenity_cnt_per_building
        ,max(b.avg_ll_utility_amenity_cnt_per_building) avg_ll_utility_amenity_cnt_per_building
    from building_profile a
    join management_company_by_corporate b on b."ManagementCompanyEntityId" = a.CorporateManagementCompanyId
    group by buildingid
)
/* ALN building features. These were chosen by going over what tables/columns were available in ALN and picking each one that 
   could potentially be of predictive power.
*/
, aln_building as (
    select 
         a.buildingid
        ,a.units aln_NumUnits
        ,2022 - try_to_number(a.Year_Built) aln_built_years_ago
        ,try_to_number(a.Occupancy) aln_Occupancy
        ,try_to_number(a.NumberOfStories) aln_NumberOfStories
        ,a.AverageRent aln_AverageRent
        ,a.AverageSqFt aln_AverageSqFt
        ,round(case when a.AverageSqFt <> 0 then a.AverageRent/a.AverageSqFt end, 2) aln_rent_over_unit_area
        ,case when c."IncomeRestricted" = 'N' then 0 when c."IncomeRestricted" = 'Y' then 1 end aln_income_restricted
        ,case when c."Section8" = 'N' then 0 when c."Section8" = 'Y' then 1 end aln_section8
        ,case when c."ShortTerm" = 'N' then 0 when c."ShortTerm" = 'Y' then 1 end aln_short_term
        ,try_to_number(c."ApplicationFee") aln_ApplicationFee
        ,d."MaxNumPets" aln_MaxNumPets
        ,e.lifestyle_amenity_cnt
        ,e.household_amenity_cnt
        ,e.service_amenity_cnt
        ,e.transport_related_amenity_cnt
        ,e.safety_amenity_cnt
        ,e.utility_amenity_cnt
    from building_profile a
    left join "EXT_DATA"."ALN_DATA"."ApartmentLeasingExtension" c on a.buildingid = c."ApartmentId"
    left join ext_data.aln_data."ApartmentPetExtension" d on a.buildingid = d."ApartmentId"
    left join aln_amenity e on e.apartmentid = a.buildingid
)
/* table supplied by Michael, to match MSA code to MSA name.*/
, msa_names as (
    select 
         distinct lower(msa_name) msa_name
        ,msa_number
    from TG_APP_DB.TG_MANUAL.ZIP_METRO_MAPPING
)
/* Crime data by msa, from fbi: https://ucr.fbi.gov/crime-in-the-u.s/2019/crime-in-the-u.s.-2019/tables/table-6
   imported as csv file for year 2019. in case this variable is left in the model, will need to be updated once a year at most.
*/
, fbi_crime_by_msa as (
    select 
         max(a.VIOLENT_CRIME_PER_100K) VIOLENT_CRIME_PER_100K
        ,max(a.PROPERTY_CRIME_PER_100K) PROPERTY_CRIME_PER_100K
        ,b.msa_number
    from reports.dbt_bchikvinidze.crime_by_msa a
    join msa_names b on lower(b.msa_name) = lower(concat(msa1, ', ', msa2, ' msa'))
    group by b.msa_number
)
/* Features for ALN LL zip code with most units in it.
   Aggregate function is just for using group by without error.
*/
, ll_zip_features as (
    select 
         try_to_number(a.ll_most_units_in_zip::text) zip_ll
        ,max(f.avg_unemployment_rate_1y) ll_zip_avg_unemployment_rate_1y
        ,max(g.median_gross_rent_all_bedrooms) ll_zip_median_gross_rent_all_bedrooms
        ,max(g.median_gross_rent_no_bedrooms) ll_zip_median_gross_rent_no_bedrooms
        ,max(g.median_gross_rent_one_bedroom) ll_zip_median_gross_rent_one_bedroom
        ,max(k.per_capita_income) ll_zip_per_capita_income
        ,max(try_to_number(k2.median_household_income)) ll_zip_median_household_income
        ,max(try_to_number(k2.median_family_income)) ll_zip_median_family_income
        ,max(o.median_age) ll_zip_median_age
        ,max(p.population_density) ll_zip_population_density
        ,max(round(p.population/q.public_school_cnt, 1)) ll_zip_population_per_public_school
        ,max(round(p.population/q.private_school_cnt, 1)) ll_zip_population_per_private_school
        ,max(round(p.population/q.hospital_cnt, 1)) ll_zip_population_per_hospital
        ,max(round(p.population/q.college_cnt, 1)) ll_zip_population_per_college
    from building_company_aln a
    join TG_APP_DB.TG_MANUAL.ZIP_METRO_MAPPING e on e.zip_code = zip_ll
    left join unemployment f on f.msa_code = e.msa_number
    left join gross_rent g on g.msa_code = e.msa_number and g.year = 2019
    left join cbsa_zip_mapping j on j.zip = zip_ll
    left join per_capita_income k on k.cbsa_code = j.cbsa_code
    left join "REPORTS".DBT_BCHIKVINIDZE.MEDIAN_INCOME_BY_ZIP k2 on try_to_number(k2.zip) = zip_ll
    left join reports.dbt_bchikvinidze.median_age_zip o on o.zip_code = zip_ll
    left join reports.dbt_bchikvinidze.population_density_zip p on try_to_number(p.zip) = zip_ll
    left join poi_per_zip q on q.zip = zip_ll
    group by zip_ll
)
/* This is final join of all the features that were gathered for each building */
select -- around 2 minutes to run (limit 100)
a.buildingid
--,count(1) cnt
,a.name
--,case when s.buildingid is not null then 1 else 0 end aln_building_join_available
--,case when r.buildingid is not null then 1 else 0 end aln_company_join_available
--,a.denied_over_available target
--,r."MgmtOfficeIntegerId" MgmtOfficeIntegerId
--,a.landlord_source_id
--,a.denied_over_available_explicit target_explicit
--,a.denied_over_available_explicit_and_clear target_explicit_and_clear
--,a.months_from_first_app
--,a.cnt_status_available_apps
--,a.units 
--,a.avg_monthly_rent
--,2022 - a.year_built building_age 
,try_to_number(a.zip::text) zipcode
,a.state
,a.city
,a.latitude
,a.longitude
,d.closest_college_km
,d.college_cnt_1km
,d.college_cnt_2km
,d.college_cnt_5km
,d.college_cnt_10km
,d.college_cnt_25km
,d.college_cnt_50km
,d.graduate_deg_uni_cnt_1km
,d.graduate_deg_uni_cnt_2km
,d.graduate_deg_uni_cnt_5km
,d.graduate_deg_uni_cnt_10km
,d.graduate_deg_uni_cnt_25km
,d.graduate_deg_uni_cnt_50km
,d.bachelors_deg_uni_cnt_1km
,d.bachelors_deg_uni_cnt_2km
,d.bachelors_deg_uni_cnt_5km
,d.bachelors_deg_uni_cnt_10km
,d.bachelors_deg_uni_cnt_25km
,d.bachelors_deg_uni_cnt_50km
,d.other_deg_uni_cnt_1km
,d.other_deg_uni_cnt_2km
,d.other_deg_uni_cnt_5km
,d.other_deg_uni_cnt_10km
,d.other_deg_uni_cnt_25km
,d.other_deg_uni_cnt_50km
,d.small_college_cnt_1km
,d.small_college_cnt_2km
,d.small_college_cnt_5km
,d.small_college_cnt_10km
,d.medium_college_cnt_1km
,d.medium_college_cnt_2km
,d.medium_college_cnt_5km
,d.medium_college_cnt_10km
,d.large_college_cnt_1km
,d.graduate_deg_uni_cnt_25_to_50km
,d.graduate_deg_uni_cnt_10_to_25km
,d.large_college_cnt_2km
,d.large_college_cnt_5km
,d.large_college_cnt_10km
,d.residental_college_cnt_1km
,d.residental_college_cnt_2km
,d.residental_college_cnt_5km
,d.residental_college_cnt_10km
,d.avg_college_cost_1km
,d.avg_college_cost_2km
,d.avg_college_cost_5km
,d.avg_college_cost_10km
,d.avg_college_cost_50km
,d.avg_college_completion_rate_1km
,d.avg_college_completion_rate_2km
,d.avg_college_completion_rate_5km
,d.avg_college_completion_rate_10km
,d.avg_college_completion_rate_50km
,f.avg_unemployment_rate_1y
,g.median_gross_rent_all_bedrooms
,g.median_gross_rent_no_bedrooms
,g.median_gross_rent_one_bedroom
/*,i.star1_pct -- will uncomment once I gather google reviews
,i.star2_pct
,i.star3_pct
,i.star4_pct
,i.star5_pct
,(i.star1_pct*1 + i.star2_pct*2 + i.star3_pct*3 + i.star4_pct*4 + i.star5_pct*5) AVG_REVIEW_SCORE
,i.with_text_pct
,i.total_cnt total_review_cnt
,round(case when a.units <> 0 then i.total_cnt/a.units end, 2) review_per_unit*/
,k.per_capita_income
,try_to_number(k2.median_household_income) median_household_income
,try_to_number(k2.median_family_income) median_family_income
,h.closest_hospital_km
,h.hospital_cnt_2km
,h.hospital_cnt_5km
,h.hospital_cnt_10km
,h.hospital_cnt_50km
,h.general_acute_care_hospital_cnt_2km
,h.general_acute_care_hospital_cnt_5km
,h.general_acute_care_hospital_cnt_10km
,h.government_hospital_cnt_2km
,h.government_hospital_cnt_5km
,h.government_hospital_cnt_10km
,h.hospital_avg_beds_2km
,h.hospital_avg_beds_5km
,h.hospital_avg_beds_10km
,m.closest_law_enforcement_km
,m.law_enforcement_cnt_2km
,m.law_enforcement_cnt_5km
,m.law_enforcement_cnt_10km
,n.closest_private_school_km
,n.closest_public_school_km
,n.closest_private_elem_school_km
,n.closest_public_elem_school_km
,n.private_school_cnt_2km
,n.private_school_cnt_5km
,n.private_school_cnt_10km
,n.public_school_cnt_2km
,n.public_school_cnt_5km
,n.public_school_cnt_10km
,o.median_age
,p.population_density
,round(p.population/q.public_school_cnt, 1) population_per_public_school
,round(p.population/q.private_school_cnt, 1) population_per_private_school
,round(p.population/q.hospital_cnt, 1) population_per_hospital
,round(p.population/q.college_cnt, 1) population_per_college
,round(p.population/q.law_enforcement_cnt, 1) population_per_law_enforcement
,r.ll_apartment_cnt
,r.ll_avg_units
,r.ll_median_units
,r.ll_1st_quartile_units
,r.ll_3rd_quartile_units
,r.ll_newest_years_ago
,r.ll_oldest_years_ago
,r.ll_avg_years_ago
,r.ll_avg_occupancy
,r.ll_avg_stories
,r.ll_avg_rent
,r.ll_avg_sqft
,r.ll_avg_rent_over_unit_area
,r.ll_income_restricted_apartment_pct
,r.ll_section8_apartment_pct
,r.ll_avg_application_fee
,r.ll_senior_housing_pct
,r.ll_assisted_living_pct
,r.ll_student_housing_pct
,r.ll_short_term_housing_pct
,r.ll_pet_allowed_housing_pct
,r.ll_AddressState
,r.ll_AddressZIP
,r.ll_most_units_in_zip
,r.ll_apartment_cnt*r.ll_avg_units ll_total_units
,r.avg_ll_lifestyle_amenity_cnt_per_building
,r.avg_ll_household_amenity_cnt_per_building
,r.avg_ll_service_amenity_cnt_per_building
,r.avg_ll_transport_related_amenity_cnt_per_building
,r.avg_ll_safety_amenity_cnt_per_building
,r.avg_ll_utility_amenity_cnt_per_building
,s.aln_NumUnits
,s.aln_built_years_ago
,s.aln_Occupancy
,s.aln_NumberOfStories
,s.aln_AverageRent
,s.aln_AverageSqFt
,s.aln_rent_over_unit_area
,s.aln_income_restricted
,s.aln_section8
,s.aln_short_term
,s.aln_ApplicationFee
,s.aln_MaxNumPets
,s.lifestyle_amenity_cnt
,s.household_amenity_cnt
,s.service_amenity_cnt
,s.transport_related_amenity_cnt
,s.safety_amenity_cnt
,s.utility_amenity_cnt
,t.VIOLENT_CRIME_PER_100K
,t.PROPERTY_CRIME_PER_100K
,u.ll_zip_avg_unemployment_rate_1y
,u.ll_zip_median_gross_rent_all_bedrooms
,u.ll_zip_median_gross_rent_no_bedrooms
,u.ll_zip_median_gross_rent_one_bedroom
,u.ll_zip_per_capita_income
,u.ll_zip_median_household_income
,u.ll_zip_median_family_income
,u.ll_zip_median_age
,u.ll_zip_population_density
,u.ll_zip_population_per_public_school
,u.ll_zip_population_per_private_school
,u.ll_zip_population_per_hospital
,u.ll_zip_population_per_college
from building_profile a
left join building_college d on a.buildingid = d.building_id
left join TG_APP_DB.TG_MANUAL.ZIP_METRO_MAPPING e on e.zip_code = try_to_number(a.zip::text)
left join unemployment f on f.msa_code = e.msa_number
left join gross_rent g on g.msa_code = e.msa_number and g.year = 2019 -- this is last year available.
left join building_hospital h on h.building_id = a.buildingid
left join cbsa_zip_mapping j on j.zip = try_to_number(a.zip::text)
left join per_capita_income k on k.cbsa_code = j.cbsa_code
left join "REPORTS".DBT_BCHIKVINIDZE.MEDIAN_INCOME_BY_ZIP k2 on try_to_number(k2.zip) = try_to_number(a.zip::text)
left join building_law_enforcement m on m.building_id = a.buildingid
left join building_schools n on n.building_id = a.buildingid
left join reports.dbt_bchikvinidze.median_age_zip o on o.zip_code = try_to_number(a.zip::text)
left join reports.dbt_bchikvinidze.population_density_zip p on try_to_number(p.zip) = try_to_number(a.zip::text)
left join poi_per_zip q on q.zip = try_to_number(a.zip::text)
left join building_company_aln r on r.buildingid = a.buildingid
left join aln_building s on s.buildingid = a.buildingid
left join fbi_crime_by_msa t on t.msa_number = e.msa_number
left join ll_zip_features u on u.zip_ll = try_to_number(r.ll_most_units_in_zip::text)
where 1=1
and a.zip is not null;

/* This is the development sample that generates all the variables and target for the building tenant profile project.
   Every WITH statement contains its own commentary to explain what is happening in each code section.
   
   First WITH statement defines set of buildings where approval statuses of correcponding applications was
   explicitly available through landlord referral. Purpose of this WITH is to define application statuses
   as well as gather some basic features about a building. Most of the variables are aggregated with max()
   function because of the group by function.
*/
with approval_statuses_explicit as (
    select 
         max(e.source_id) buildingid
        ,max(b.deal_id) dealid
        ,b.source_id id
        ,max(a.months_covered) bondmonthscovered
        ,max(round(months_between(a.lease_end_date, a.lease_start_date))) lease_length    
        ,max(b.product_name) product_name
        ,max(e.units) units
        ,max(e.latitude) latitude
        ,max(e.longitude) longitude
        ,max(e.zip) zip
        ,max(e.landlord_source_id) landlord_source_id
        ,max(e.state) state
        ,max(e.city) city
        ,max(e.street) street
        ,max(e.year_built) year_built
        ,max(b.application_create_date) createdat
        ,max(case when d.value in ('Approved with conditions', 'Approved with Conditions', 'Conditional') then 'Approved with conditions'
             when d.value in ('Denied', 'Approved') then d.value else null end) APPROVAL_STATUS
        ,max(a.monthly_rent) monthly_rent
        ,max(e.name) name
    from TG_DW_DB.public.deal  a 
    join TG_DW_DB.PUBLIC.APPLICATION b on b.deal_id = a.id
    join TG_DW_DB.public.building e on e.id = a.building_id
    left join "TG_APP_DB"."TG_STEADY_PROD"."PUBLIC_ANSWERS" d on d.applicationid = b.source_id
    where 1 = 1
        --and d.questionid = 92 
        --and d.value is not null
        and a.building_id is not null
        and b.application_create_date >= '2020-06-05' --this is minimal createdat for apps where value to question 92 is available
        and b.application_create_date  < '2022-02-01' -- fixing final date of apps for dev sample
        and b.product_name <> 'TG Pay'
        and a.months_covered is not null
    group by b.source_id     
) 
/* 
    This intermediary code is used to identify dublicates from the building rules table.
    At the time of writing this code, there was one such building available.
*/
, duplicated_rules as ( -- there is one such building for now
    select 
        z.id
    from "REPORTS".DBT_BCHIKVINIDZE.BUILDING_RULES z 
    group by z.id
    having count(1)>1
)
/* 
    Where approval status of an application is not available explicityly, inference by building rules is used.
    This WITH creates inference logic to create approval status, as well as gathers basic variables about the building. 
    Note that gathering of applications starts at the same time as landlord referral program. 
*/
, approval_statuses_inferred as (
    select 
     z.id buildingid
    ,b.id dealid
    ,d.source_id id
    ,b.months_covered bondmonthscovered
    ,round(months_between(b.lease_end_date, b.lease_start_date)) lease_length
    --,case when z.coverfreemonths = 'TRUE' then zeroifnull(b.free_months) else 0 end free_months
    --,case when z.coverpre_paidrent = 'TRUE' then zeroifnull(months_between(b.prepaid_month_end_date, b.prepaid_month_start_date)) else 0 end prepaid_months
    --,case when z.coverpro_rateddays = 'TRUE' then zeroifnull(months_between(b.lease_end_date, b.lease_start_date)-lease_length) else 0 end prorated_months
    ,d.product_name
    ,a.units
    ,a.latitude
    ,a.longitude
    ,a.zip
    ,a.landlord_source_id
    ,a.state
    ,a.city
    ,a.street
    ,a.year_built
    ,a.name
    ,d.application_create_date createdat
    ,b.monthly_rent
    ,case when z.firstcoveragetierlgmonthscovered in ('Full Coverage', 'Full coverage') then lease_length
          when z.firstcoveragetierlgmonthscovered = 'Not applicable' then null
          else TRY_TO_NUMERIC(z.firstcoveragetierlgmonthscovered) end approved_coverage
    
    ,case when z.secondcoveragetierlgmonthscovered in ('Full Coverage', 'Full coverage') then lease_length
          when z.secondcoveragetierlgmonthscovered = 'Not applicable' then null
          else TRY_TO_NUMERIC(z.secondcoveragetierlgmonthscovered) end cond_approved_coverage
    
    ,case when z.thirdcoveragetierlgmonthscovered in ('Full Coverage', 'Full coverage') then lease_length
          when z.thirdcoveragetierlgmonthscovered = 'Not applicable' then null
          else TRY_TO_NUMERIC(z.thirdcoveragetierlgmonthscovered) end denied_coverage
    
         --if all coverages are numbers that are given, its straightforward:
    ,case when approved_coverage < cond_approved_coverage and cond_approved_coverage < denied_coverage then
              case when b.months_covered <= approved_coverage then 'Approved'
              when b.months_covered <= cond_approved_coverage then 'Approved with conditions'
              else 'Denied' end 
          --if everything is unknown, nothing we can infer:
         when approved_coverage is null and cond_approved_coverage is null and denied_coverage is null then null
          -- if approved coverage does not exist:
         when approved_coverage is null then
              case when cond_approved_coverage is not null and b.months_covered > cond_approved_coverage then 'Denied'
                   when cond_approved_coverage is null and b.months_covered >= denied_coverage then 'Denied'  
                   else 'Approved with conditions' end -- here we're left with cond. approval or approval, going with worse case for tenant
          -- if conditionally approved coverage does not exist:
          when cond_approved_coverage is null then
              case when b.months_covered > approved_coverage then 'Denied'
                   else 'Approved' end    
          --if denied coverage does not exist
          when denied_coverage is null then
              case when b.months_covered > cond_approved_coverage then 'Denied'
                   when b.months_covered > approved_coverage then 'Approved with conditions'
                   else 'Approved' end
         -- if every coverage is available but they don't differ, assume worse approval category when possible:
         when approved_coverage = cond_approved_coverage and cond_approved_coverage = denied_coverage then
              case when b.months_covered >= denied_coverage then 'Denied' 
                   else 'Approved with conditions' end
          -- if approved and conditional coverages do not differ:
         when approved_coverage = cond_approved_coverage then
              case when b.months_covered >= denied_coverage then 'Denied'
                   else 'Approved with conditions' end
        -- if conditional and denied do not differ:
        when cond_approved_coverage = denied_coverage then
            case when b.months_covered >= denied_coverage then 'Denied'
                 else 'Approved' end
    end APPROVAL_STATUS
    
    --checking if inference was done with confusing building rule:
    ,case when ((approved_coverage < cond_approved_coverage and cond_approved_coverage < denied_coverage) OR
          --if everything is unknown, nothing we can infer:
         (approved_coverage is null and cond_approved_coverage is null and denied_coverage is null)      OR
          -- if approved coverage does not exist:
         (approved_coverage is null and 
              (cond_approved_coverage is not null and
               b.months_covered > cond_approved_coverage) OR
              (cond_approved_coverage is null and 
               b.months_covered >= denied_coverage))                                                   OR
          -- if conditionally approved coverage does not exist:
         (cond_approved_coverage is null and b.months_covered > approved_coverage)                    OR
          --if denied coverage does not exist
         (denied_coverage is null and (b.months_covered > cond_approved_coverage
                   or b.months_covered > approved_coverage))) then 'clear' else 'confusing' end INFERENCE_TYPE
from "REPORTS".DBT_BCHIKVINIDZE.BUILDING_RULES z 
join TG_DW_DB.public.building a on z.id = a.source_id 
join TG_DW_DB.public.deal b on a.id = b.building_id
join "TG_DW_DB".PUBLIC.APPLICATION d on b.id = d.deal_id
where 1=1 
    and b.months_covered is not null 
    and d.source_id not in (select id from approval_statuses_explicit where approval_status is not null) -- no need to infer apps that we already know explicitly
    and d.application_create_date >= '2020-06-05' -- this is minimum createdat for exlicit approval status apps (when landlord referrals began)
    and d.application_create_date < '2022-02-01'
    and d.product_name <> 'TG Pay' -- to avoid dupes
    and z.id not in (select id from duplicated_rules) -- building id can have dupe rule. exclude such. 
)
/*, explicit_vs_inferred as (
    select 
         a.buildingid 
        ,a.dealid
        ,a.id
        ,a.bondmonthscovered
        ,a.lease_length
        ,a.product_name
        ,a.units
        ,a.createdat
        ,a.approval_status
        ,case when a.approval_status is not null then 'available' else 'unavailable' end status_logic_type
        ,'explicit' status_logic_source_type
        ,null inference_type  
        
        ,case when z.firstcoveragetierlgmonthscovered in ('Full Coverage', 'Full coverage') then lease_length
          when z.firstcoveragetierlgmonthscovered = 'Not applicable' then null
          else TRY_TO_NUMERIC(z.firstcoveragetierlgmonthscovered) end approved_coverage
    
        ,case when z.secondcoveragetierlgmonthscovered in ('Full Coverage', 'Full coverage') then lease_length
              when z.secondcoveragetierlgmonthscovered = 'Not applicable' then null
              else TRY_TO_NUMERIC(z.secondcoveragetierlgmonthscovered) end cond_approved_coverage

        ,case when z.thirdcoveragetierlgmonthscovered in ('Full Coverage', 'Full coverage') then lease_length
              when z.thirdcoveragetierlgmonthscovered = 'Not applicable' then null
              else TRY_TO_NUMERIC(z.thirdcoveragetierlgmonthscovered) end denied_coverage
    
        ,case when approved_coverage < cond_approved_coverage and cond_approved_coverage < denied_coverage then
              case when bondmonthscovered <= approved_coverage then 'Approved'
              when bondmonthscovered <= cond_approved_coverage then 'Approved with conditions'
              else 'Denied' end 
          --if everything is unknown, nothing we can infer:
         when approved_coverage is null and cond_approved_coverage is null and denied_coverage is null then null
          -- if approved coverage does not exist:
         when approved_coverage is null then
              case when cond_approved_coverage is not null and bondmonthscovered > cond_approved_coverage then 'Denied'
                   when cond_approved_coverage is null and bondmonthscovered >= denied_coverage then 'Denied'  
                   else 'Approved with conditions' end -- here we're left with cond. approval or approval, going with worse case for tenant
          -- if conditionally approved coverage does not exist:
          when cond_approved_coverage is null then
              case when bondmonthscovered > approved_coverage then 'Denied'
                   else 'Approved' end    
          --if denied coverage does not exist
          when denied_coverage is null then
              case when bondmonthscovered > cond_approved_coverage then 'Denied'
                   when bondmonthscovered > approved_coverage then 'Approved with conditions'
                   else 'Approved' end
         -- if every coverage is available but they don't differ, assume worse approval category when possible:
         when approved_coverage = cond_approved_coverage and cond_approved_coverage = denied_coverage then
              case when bondmonthscovered >= denied_coverage then 'Denied' 
                   else 'Approved with conditions' end
          -- if approved and conditional coverages do not differ:
         when approved_coverage = cond_approved_coverage then
              case when bondmonthscovered >= denied_coverage then 'Denied'
                   else 'Approved with conditions' end
        -- if conditional and denied do not differ:
        when cond_approved_coverage = denied_coverage then
            case when bondmonthscovered >= denied_coverage then 'Denied'
                 else 'Approved' end
    end if_inferred_APPROVAL_STATUS
    
    --checking if inference was done with confusing building rule:
    ,case when ((approved_coverage < cond_approved_coverage and cond_approved_coverage < denied_coverage) OR
          --if everything is unknown, nothing we can infer:
         (approved_coverage is null and cond_approved_coverage is null and denied_coverage is null)      OR
          -- if approved coverage does not exist:
         (approved_coverage is null and 
              (cond_approved_coverage is not null and
               bondmonthscovered > cond_approved_coverage) OR
              (cond_approved_coverage is null and 
               bondmonthscovered >= denied_coverage))                                                   OR
          -- if conditionally approved coverage does not exist:
         (cond_approved_coverage is null and bondmonthscovered > approved_coverage)                    OR
          --if denied coverage does not exist
         (denied_coverage is null and (bondmonthscovered > cond_approved_coverage
                   or bondmonthscovered > approved_coverage))) then 'clear' else 'confusing' end if_inferred_inference_type
    
    from approval_statuses_explicit a
    left join "REPORTS".DBT_BCHIKVINIDZE.BUILDING_RULES z on z.id = a.buildingid
    where a.id not in (select id from approval_statuses_inferred)
)*/
/* merging explicit and inferred statuses as well as some basic data about buildings*/
, explicit_and_inferred_statuses as (
    select
         buildingid 
        ,dealid
        ,id
        ,bondmonthscovered
        ,lease_length
        ,product_name
        ,units
        ,monthly_rent
        ,latitude
        ,longitude
        ,zip
        ,landlord_source_id
        ,state
        ,city
        ,street
        ,year_built
        ,createdat
        ,name
        ,round(months_between(sysdate(), createdat),1) months_from_app
        ,approval_status
        ,case when approval_status is not null then 'available' else 'unavailable' end status_logic_type
        ,'explicit' status_logic_source_type
        ,null inference_type
    from approval_statuses_explicit a
    where a.approval_status is not null
    
    union 
    select 
         buildingid 
        ,dealid
        ,id
        ,bondmonthscovered
        ,lease_length
        ,product_name
        ,units
        ,monthly_rent
        ,latitude
        ,longitude
        ,zip
        ,landlord_source_id
        ,state
        ,city
        ,street
        ,year_built
        ,createdat
        ,name
        ,round(months_between(sysdate(), createdat),1) months_from_app
        ,approval_status
        ,case when approval_status is not null then 'inferred' else 'cant be inferred' end status_logic_type
        ,'inferred' status_logic_source_type
        ,inference_type
    from approval_statuses_inferred
)
/*, building_period_intermediaty as (
    select a.buildingid
        ,a.units
        ,date_trunc('MONTH', a.createdat) year_month
        ,count(1) apps_cnt
        ,sum(case when approval_status = 'Denied' then 1 else 0 end) denied_apps_cnt
        ,sum(case when approval_status is not null then 1 else 0 end) cnt_status_available_apps
    from explicit_and_inferred_statuses a
    where 1=1
    group by a.buildingid, a.units, year_month
)
,building_period as (
select 
     a.buildingid
    ,a.units
    ,a.year_month
    ,sum(a.denied_apps_cnt) over (PARTITION BY a.buildingid order by a.year_month rows between 0 following and 5 following) denied_apps_cnt_6M
    ,sum(a.denied_apps_cnt) over (PARTITION BY a.buildingid order by a.year_month rows between 0 following and 8 following) denied_apps_cnt_9M
    ,sum(a.denied_apps_cnt) over (PARTITION BY a.buildingid order by a.year_month rows between 0 following and 11 following) denied_apps_cnt_12M
    ,sum(a.cnt_status_available_apps) over (PARTITION BY a.buildingid order by a.year_month rows between 0 following and 5 following) cnt_status_available_apps_6M
    ,sum(a.cnt_status_available_apps) over (PARTITION BY a.buildingid order by a.year_month rows between 0 following and 8 following) cnt_status_available_apps_9M
    ,sum(a.cnt_status_available_apps) over (PARTITION BY a.buildingid order by a.year_month rows between 0 following and 11 following) cnt_status_available_apps_12M
    ,sum(a.apps_cnt) over (PARTITION BY a.buildingid order by a.year_month rows between 0 following and 5 following) apps_cnt_6M
    ,sum(a.apps_cnt) over (PARTITION BY a.buildingid order by a.year_month rows between 0 following and 8 following) apps_cnt_9M
    ,sum(a.apps_cnt) over (PARTITION BY a.buildingid order by a.year_month rows between 0 following and 11 following) apps_cnt_12M
    ,round(case when cnt_status_available_apps_6M <> 0 then denied_apps_cnt_6M/cnt_status_available_apps_6M end, 3) denied_over_available_6m
    ,round(case when cnt_status_available_apps_9M <> 0 then denied_apps_cnt_9M/cnt_status_available_apps_9M end, 3) denied_over_available_9m
    ,round(case when cnt_status_available_apps_12M <> 0 then denied_apps_cnt_12M/cnt_status_available_apps_12M end, 3) denied_over_available_12m
    ,round(case when a.units <> 0 then (apps_cnt_6M/a.units)/6 end, 3) tg_utilization_6m
    ,round(case when a.units <> 0 then (apps_cnt_9M/a.units)/9 end, 3) tg_utilization_9m
    ,round(case when a.units <> 0 then (apps_cnt_12M/a.units)/12 end, 3) tg_utilization_12m
from building_period_intermediaty a
)*/
/* This WITH statement contains final logic for target creation.
   There are some other versions of target which were not pursued in this project, are kept in script for history purposes.
   Inference type was used for analytic purposes.
*/
, building_profile as (
    select --3.3k buildings with at least 1 apps, 
         a.buildingid
        ,a.units
        ,round(avg(monthly_rent), 2) avg_monthly_rent
        ,a.latitude
        ,a.longitude
        ,a.zip
        ,a.landlord_source_id
        ,a.state
        ,a.city
        ,a.street
        ,a.year_built
        ,a.name
        ,round(months_between(sysdate(), min(a.createdat)),1) months_from_first_app
        ,count(distinct dealid) deals_cnt
        ,count(1) apps_cnt
        ,sum(case when approval_status is not null then 1 else 0 end) cnt_status_available_apps
        ,sum(case when approval_status = 'Denied' then 1 else 0 end) denied_apps_cnt
    
        ,sum(case when status_logic_type = 'available' then 1 else 0 end) cnt_explicit_apps
        ,sum(case when (INFERENCE_TYPE = 'clear' or status_logic_type = 'available') then 1 else 0 end) cnt_clear_and_explicit_apps
        ,sum(case when status_logic_type = 'available' and approval_status = 'Denied' then 1 else 0 end) cnt_denied_explicit_apps
        ,sum(case when (INFERENCE_TYPE = 'clear' or status_logic_type = 'available') and approval_status = 'Denied' then 1 else 0 end) cnt_denied_clear_and_explicit_apps
    
        ,case when cnt_status_available_apps <> 0 then
            round(denied_apps_cnt/cnt_status_available_apps, 2) else null end denied_over_available
        ,case when cnt_explicit_apps <> 0 then
            round(cnt_denied_explicit_apps/cnt_explicit_apps, 2) else null end denied_over_available_explicit
        ,case when cnt_clear_and_explicit_apps <> 0 then
            round(cnt_denied_clear_and_explicit_apps/cnt_clear_and_explicit_apps, 2) else null end denied_over_available_explicit_and_clear

    from explicit_and_inferred_statuses a
    where 1=1
    --and a.approval_status is not null
    group by a.buildingid, a.units, a.latitude, a.longitude, a.landlord_source_id, a.zip, a.state, a.city, a.street, a.year_built, a.name
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
/* Hospita data is also assumed as static (like college data). 
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
    where a.year = 2019
    group by a.cbsa_code
    having count(1)=1 -- I moving this table to 'with' statement because there is one dupe
)
/*, landlord as (
    select
        a.source_id
        ,count(distinct b.source_id) ll_building_cnt
        ,round(avg(b.units)) ll_avg_building_units       
    from "TG_DW_DB"."PUBLIC"."LANDLORD" a -- 7857
    join TG_DW_DB.PUBLIC.building b on b.landlord_source_id = a.source_id -- 3986
    group by a.source_id
)*/
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
/* Here is google review data, loaded as .csv file after being scraped by selenium on google maps.
   Reason for risk team's data not being used, that has already been loaded into snowflake, is that
   it did not have any filtering by time. The data in below WITH is at least 1 year old to decrease overlap
   with target period.
*/
, google_reviews as (
    select 
         b.source_id buildingid
        ,max(round(case when a.total_cnt  <> 0 then a.star1_cnt/a.total_cnt end, 2)) star1_pct
        ,max(round(case when a.total_cnt  <> 0 then a.star2_cnt/a.total_cnt end, 2)) star2_pct
        ,max(round(case when a.total_cnt  <> 0 then a.star3_ct/a.total_cnt end, 2)) star3_pct
        ,max(round(case when a.total_cnt  <> 0 then a.star4_cnt/a.total_cnt end, 2)) star4_pct
        ,max(round(case when a.total_cnt  <> 0 then a.star5_cnt/a.total_cnt end, 2)) star5_pct
        ,max(round(case when a.total_cnt  <> 0 then a.with_text_cnt/a.total_cnt end, 2)) with_text_pct
        ,max(a.total_cnt) total_cnt
    from reports.dbt_bchikvinidze.google_reviews_1_year_old a
    join TG_DW_DB.public.building b on (lower(a.address) = lower(concat(b.name, ', ', b.city, ', ', b.state)) or 
                                        (lower(a.address) = lower(concat(b.name, ',', b.city, ',', b.state))))
    group by buildingid
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
/* matching by Atahan to connect DW buildings to ALN buildings.
   There are multiple matches in some cases, for those last one to be updated in ALN is chosen.
*/
, dw_to_aln_building_matching as (
    select  --1,994, 41 dupes
         a.id_mon
        ,b."ApartmentId" id_aln
        ,b."LastDateUpdated"
    from "EXT_DATA"."ALN_JOIN"."BUILDING_MATCHED" a
    join ext_data.aln_data."Apartment" b on a.id_aln = b."ApartmentId"
    qualify row_number() over (partition by a.id_mon order by b."LastDateUpdated" desc) = 1
)
/* ALN building features. These were chosen by going over what tables/columns were available in ALN and picking each one that 
   count potentially be of predictive power.
*/
, aln_building as (
    select 
         a.id_mon buildingid
        ,b.NumUnits aln_NumUnits
        ,2022 - try_to_number(b.YearBuilt) aln_built_years_ago
        ,try_to_number(b.Occupancy) aln_Occupancy
        ,try_to_number(b.NumberOfStories) aln_NumberOfStories
        ,b.AverageRent aln_AverageRent
        ,b.AverageSqFt aln_AverageSqFt
        ,round(case when b.AverageSqFt <> 0 then b.AverageRent/b.AverageSqFt end, 2) aln_rent_over_unit_area
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
    from dw_to_aln_building_matching a
    join aln_unique_property b on a.id_aln = b.ApartmentId
    left join "EXT_DATA"."ALN_DATA"."ApartmentLeasingExtension" c on a.id_aln = c."ApartmentId"
    left join ext_data.aln_data."ApartmentPetExtension" d on a.id_aln = d."ApartmentId"
    left join aln_amenity e on e.apartmentid = a.id_aln
)
/* Following 3 WITH statements are joining DW building to its corresponding ALN management company through hubspot as intermediary.
   the main table that contains pre-determined maping between hubspot and ALN management company is in reports.prod.aln_to_hs_company_mapping table.
   That table was filled in by the sales team. Actual technical work for this table was done by Bill (William) Matrinez. matchig logic for creating
   this table includes name matching after filtering of state, done in google sheets. Bill assumes that around 90% of all matches should be 
   correct.
   Hubspot contains both buildings and their respective management companies as rows, and they have parent-child relationship (child - building,
   parent - management company). This joining logic was supplied by Michael. 
   Note: these three WITH statements don't cover many of the buildings in our development sample. For those, another matching logic
   was developed without HUBSPOT intermediary. Will be discussed later.
*/
, building_to_aln_company_match1 as (
    SELECT -- 5.2k buildings matched
     distinct b.source_id
    ,child.companyid hubspot_buildingId
    ,parent.companyid hubspot_parentId
    ,max(m.aln_id) aln_id
    ,max(b.landlord_source_id) landlord_source_id
    --,max(m.company_name) hs_company_name
    FROM "TG_APP_DB"."HUBSPOT"."COMPANIES" CHILD 
    join "TG_APP_DB"."HUBSPOT"."COMPANIES" PARENT ON CHILD.PROPERTY_HS_PARENT_COMPANY_ID:value = PARENT.COMPANYID
    join TG_DW_DB.public.building b on b.hubspotcompanyid = child.companyid -- 22.8k joined out of 33k
    join reports.prod.aln_to_hs_company_mapping m on m.hubspot_id = hubspot_parentId -- 5.2k buildings
    join ext_data.aln_data."ManagementCompany" a on a."MgmtOfficeIntegerId" = m.aln_id -- 5.2k
    group by b.source_id, hubspot_buildingId, hubspot_parentId
)
/* Sometimes child to parent relationship is not enough to trace management company to its ALN counterpart.
   So another layer is joined to create child to grandparent relationship, that identifies extra observations.
   This WITH runs on buildings for which match was not found in previous WITH statement
*/
, building_to_aln_company_match2 as (
    SELECT -- 3 joins - 24, 4 joins - 28
         b.source_id
        ,child.companyid hubspot_buildingId
        ,PARENTPARENT.companyid hubspot_parentId
        ,max(m.aln_id) aln_id
        ,max(b.landlord_source_id) landlord_source_id
        --,max(m.company_name) hs_company_name
    FROM "TG_APP_DB"."HUBSPOT"."COMPANIES" CHILD 
    join "TG_APP_DB"."HUBSPOT"."COMPANIES" PARENT ON CHILD.PROPERTY_HS_PARENT_COMPANY_ID:value = PARENT.COMPANYID
    join "TG_APP_DB"."HUBSPOT"."COMPANIES" PARENTPARENT ON PARENT.PROPERTY_HS_PARENT_COMPANY_ID:value = PARENTPARENT.COMPANYID
    join TG_DW_DB.public.building b on b.hubspotcompanyid = child.companyid
    join reports.prod.aln_to_hs_company_mapping m on m.hubspot_id = hubspot_parentId 
    join "TG_DW_DB"."PUBLIC"."LANDLORD" l on l.source_id = b.landlord_source_id
    join ext_data.aln_data."ManagementCompany" a on a."MgmtOfficeIntegerId" = m.aln_id
    where b.source_id not in (select source_id from building_to_aln_company_match1)
    group by b.source_id, hubspot_buildingId, hubspot_parentId 
)
/* in very few cases we need deeper connection (child to great-grandparent). No results were observed after even deeper recursive join.
   This WITH runs on buildings for which previous two WITH did not return results.
*/
, building_to_aln_company_match3 as (
    SELECT -- 3 joins - 24, 4 joins - 28
         b.source_id
        ,child.companyid hubspot_buildingId
        ,PARENTPARENTPARENT.companyid hubspot_parentId
        ,max(m.aln_id) aln_id
        ,max(b.landlord_source_id) landlord_source_id
        --,max(m.company_name) hs_company_name
    FROM "TG_APP_DB"."HUBSPOT"."COMPANIES" CHILD 
    join "TG_APP_DB"."HUBSPOT"."COMPANIES" PARENT ON CHILD.PROPERTY_HS_PARENT_COMPANY_ID:value = PARENT.COMPANYID
    join "TG_APP_DB"."HUBSPOT"."COMPANIES" PARENTPARENT ON PARENT.PROPERTY_HS_PARENT_COMPANY_ID:value = PARENTPARENT.COMPANYID
    join "TG_APP_DB"."HUBSPOT"."COMPANIES" PARENTPARENTPARENT ON PARENTPARENT.PROPERTY_HS_PARENT_COMPANY_ID:value = PARENTPARENTPARENT.COMPANYID
    join TG_DW_DB.public.building b on b.hubspotcompanyid = child.companyid
    join reports.prod.aln_to_hs_company_mapping m on m.hubspot_id = hubspot_parentId 
    join "TG_DW_DB"."PUBLIC"."LANDLORD" l on l.source_id = b.landlord_source_id
    join ext_data.aln_data."ManagementCompany" a on a."MgmtOfficeIntegerId" = m.aln_id
    where b.source_id not in (select source_id from building_to_aln_company_match1)
         and b.source_id not in (select source_id from building_to_aln_company_match2)
    group by b.source_id, hubspot_buildingId, hubspot_parentId 
)
/* This next two WITH statements contain matching of DW buildings to ALN Management companies,
   Developed by using fuzzymatch (by Atahan) plus extra rules of matching either phone number or email domain.
   Original code for matching can be found in databricks: https://dbc-bff98e28-6039.cloud.databricks.com/?o=1167741459609899#notebook/2359178861470069/command/2359178861470070
   table written to snowflake by importing csv though dbt.
   The matching did produce some dupes so to identify those, this next WITH statement is used:
*/
, dup_landlord_dw_to_aln_match_by_datateam as (
    select 
         id_tg
        ,count(1) cnt
    from reports.dbt_bchikvinidze.landlord_mapping_dw_to_aln -- source logic in databricks. loaded as csv from dbt
    group by id_tg 
    having count(1)>1
)
/* Matching of DW building to ALN Management company. 
   Cases where dupes happened are removed as we have no way of determining which one of those management
   companies was correct. 
*/
, landlord_dw_to_aln_match_by_datateam as (
    select 
        distinct e.source_id
        ,null hubspot_buildingId --adding these columns so that it aligns with other UNIONs
        ,null hubspot_parentId
        ,c."MgmtOfficeIntegerId" aln_id
        ,e.landlord_source_id
    from reports.dbt_bchikvinidze.landlord_mapping_dw_to_aln a
    join tg_dw_db.public.landlord b on a.id_tg = b.id
    join "EXT_DATA"."ALN_DATA"."ManagementCompany" c on c."ManagementCompanyEntityId" = a.id_aln
    join TG_DW_DB.public.building e on e.landlord_source_id = b.source_id
    where a.id_tg not in (select id_tg from dup_landlord_dw_to_aln_match_by_datateam)
    group by e.source_id, aln_id, landlord_source_id
)
/* merging all matches between DW building and ALN management company, that were identified by 
   fuzzy match+phone/domain rule, or were matched through hubspot. 
*/
, building_to_aln_company_match as (
    select * from building_to_aln_company_match1 a
        union all 
    select * from building_to_aln_company_match2 b
        union all 
    select * from building_to_aln_company_match3 c
        union
    select * from landlord_dw_to_aln_match_by_datateam d
    /*where 1=1
        and d.source_id not in (select source_id from building_to_aln_company_match1) -- adding these just in case original mapping changes, to avoid future dupes.
        and d.source_id not in (select source_id from building_to_aln_company_match2)
        and d.source_id not in (select source_id from building_to_aln_company_match3) */
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
    join "EXT_DATA"."ALN_DATA".vw_propertyaddress b on b.apartmentid = a.apartmentid
    where b.addresstype = 'Physical Address'
    group by a.CorporateManagementCompanyId, c."MgmtOfficeIntegerId", zip
    qualify row_number() over (partition by a.CorporateManagementCompanyId order by total_units desc) = 1
)
/* Handles most of the feature engineering using ALN management company data.
*/
, management_company_by_corporate as (
    select --11.5k companies
         a.CorporateManagementCompanyId
        ,c."MgmtOfficeIntegerId"
        ,count(distinct a.ApartmentId) ll_apartment_cnt
        ,round(avg(try_to_number(a.NumUnits)),1) ll_avg_units
        ,median(try_to_number(a.NumUnits)) ll_median_units
        ,PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY try_to_number(a.NumUnits)) ll_1st_quartile_units
        ,PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY try_to_number(a.NumUnits)) ll_3rd_quartile_units
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
    left join "EXT_DATA"."ALN_DATA"."ApartmentLeasingExtension" b on b."ApartmentId" = a.ApartmentId
    left join "EXT_DATA"."ALN_DATA"."ApartmentPetExtension" d on d."ApartmentId" = a.ApartmentId
    left join mgmt_co_address e on e.ManagementCompanyEntityId = c."ManagementCompanyEntityId"
    left join "EXT_DATA"."ALN_DATA"."NewConstruction" f on f."ApartmentId" = a.ApartmentId
    left join units_zip_management_company g on g.CorporateManagementCompanyId = a.CorporateManagementCompanyId
    left join aln_amenity h on h.apartmentid = b."ApartmentId"
    where 1=1
        and a.ApartmentId is not null
        and a.CorporateManagementCompanyId is not null
    group by a.CorporateManagementCompanyId, c."MgmtOfficeIntegerId"
)
/* joining of dw building with ALN management company features.
   There should not be need to use aggregate function but is used just in case to avoid dupes.
*/
, building_company_aln as (
   select --5.2k buildings matched... not great not terribe (c) chernobyl
         a.source_id buildingid
        ,max(b."MgmtOfficeIntegerId") "MgmtOfficeIntegerId"
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
    from building_to_aln_company_match a
    join management_company_by_corporate b on b."MgmtOfficeIntegerId" = a.aln_id
    group by buildingid
)
/* table supplied by Michae, to match MSA code to MSA name.*/
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
--, tmp as (
select -- around 2 minutes to run (limit 50)
a.buildingid
--,count(1) cnt
,a.name
,case when s.buildingid is not null then 1 else 0 end aln_building_join_available
,case when r.buildingid is not null then 1 else 0 end aln_company_join_available
,a.denied_over_available target
--,r."MgmtOfficeIntegerId" MgmtOfficeIntegerId
--,a.landlord_source_id
,a.denied_over_available_explicit target_explicit
,a.denied_over_available_explicit_and_clear target_explicit_and_clear
,a.months_from_first_app
,a.cnt_status_available_apps
,a.units 
,a.avg_monthly_rent
,2022 - a.year_built building_age 
,try_to_number(a.zip) zipcode
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
,i.star1_pct
,i.star2_pct
,i.star3_pct
,i.star4_pct
,i.star5_pct
,(i.star1_pct*1 + i.star2_pct*2 + i.star3_pct*3 + i.star4_pct*4 + i.star5_pct*5) AVG_REVIEW_SCORE
,i.with_text_pct
,i.total_cnt total_review_cnt
,round(case when a.units <> 0 then i.total_cnt/a.units end, 2) review_per_unit
,k.per_capita_income
,try_to_number(k2.median_household_income) median_household_income
,try_to_number(k2.median_family_income) median_family_income
--,l.ll_building_cnt
--,l.ll_avg_building_units
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
left join TG_APP_DB.TG_MANUAL.ZIP_METRO_MAPPING e on e.zip_code = try_to_number(a.zip)
left join unemployment f on f.msa_code = e.msa_number
left join gross_rent g on g.msa_code = e.msa_number and g.year = 2019
left join building_hospital h on h.building_id = a.buildingid
--left join ml.risk_model_training.building_review i on i.source_id = a.buildingid -- I won't use these reviews as inputs unless I'm certain we also get them for ALN data. we'll also need to add timestamp
left join google_reviews i on i.buildingid = a.buildingid
left join cbsa_zip_mapping j on j.zip = try_to_number(a.zip)
left join per_capita_income k on k.cbsa_code = j.cbsa_code
left join "REPORTS".DBT_BCHIKVINIDZE.MEDIAN_INCOME_BY_ZIP k2 on try_to_number(k2.zip) = try_to_number(a.zip)
--left join landlord l on l.source_id = a.landlord_source_id
left join building_law_enforcement m on m.building_id = a.buildingid
left join building_schools n on n.building_id = a.buildingid
left join reports.dbt_bchikvinidze.median_age_zip o on o.zip_code = try_to_number(a.zip)
left join reports.dbt_bchikvinidze.population_density_zip p on try_to_number(p.zip) = try_to_number(a.zip)
left join poi_per_zip q on q.zip = try_to_number(a.zip)
left join building_company_aln r on r.buildingid = a.buildingid
left join aln_building s on s.buildingid = a.buildingid
left join fbi_crime_by_msa t on t.msa_number = e.msa_number
left join ll_zip_features u on u.zip_ll = try_to_number(r.ll_most_units_in_zip::text)
where 1=1

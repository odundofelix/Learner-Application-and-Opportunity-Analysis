##Explore the Structure of Each Dataset
#Check the structure of each Dataset
#cognito_raw_table
select*
from cognito_raw2
limit 30;

ALTER TABLE cognito_raw2
RENAME COLUMN user_modified_last_date TO user_last_modified_date;
#checking for duplicates
SELECT *,
ROW_NUMBER () OVER (PARTITION BY user_id, email, gender, user_create_date, user_last_modified_date, birth_date, city, zipcode, state) as row_num
FROM cognito_raw2;

WITH
    duplicate_cognito AS (
        SELECT *, ROW_NUMBER() OVER (
                PARTITION BY
                    user_id, email, gender, user_create_date, user_last_modified_date, birth_date, city, zipcode, state
            ) as row_num
        FROM cognito_raw2
    )
SELECT *
FROM duplicate_cognito
WHERE
    row_num > 1;
#zero record, no duplicates

select DISTINCT COUNT (*),user_id
from cognito_raw2
GROUP BY user_id
HAVING COUNT(*)>1;
#user_id is unique possible the primary key (uuuid_generate_v4())
SELECT COUNT (*)
FROM (SELECT ROW_NUMBER () OVER (
    PARTITION BY user_id, email, gender, user_create_date, user_last_modified_date, 
    birth_date, city, zipcode, state) as row_num
    FROM cognito_raw2)
sub
WHERE row_num>1;
#There are no duplicate records in cognito_raw dataset

#Inconsistent formats (Standardizing the Dataset)
##Leading/trailing whitespace
SELECT *
FROM cognito_raw2
WHERE user_id <> TRIM (user_id)
        OR email <> TRIM(email)
        OR gender <> TRIM(gender)
        OR city <>  TRIM(city)
        OR state <> TRIM (state)
        OR zipcode <> TRIM (zipcode);
#to detect entries with unwanted spaces that break grouping or filtering.
#There was no data with white spaces

##For inconsistent capitalisation
SELECT DISTINCT COUNT (*) as total,gender
FROM cognito_raw2
GROUP BY gender;

SELECT COUNT(*) FROM cognito_raw2 WHERE gender like 'Don%';
#No inconsistent capitalisation. However, 361 records were "Don%27t want to specify"
#instead of "Don't want to specify"
SELECT DISTINCT city
FROM cognito_raw2;
#There is inconsistent capitalisation in city

SELECT DISTINCT state
FROM cognito_raw2;
# There is inconsistent capitalisation in state

##Non-date birthdate entries
SELECT birth_date
FROM cognito_raw2
WHERE birth_date !~'^\d{2}/\d{2}/\d{4}$';

SELECT birth_date
FROM cognito_raw2
WHERE
    birth_date ~ '^\d{2}/\d{2}/\d{4}$';

SELECT birth_date
FROM cognito_raw2
WHERE
    birth_date !~ '^\d{2}-\d{2}-\d{4}$'
    AND birth_date !~ '^\d{4}-\d{2}-\d{2}$';
#All the records in birthdate are not in the ISO format (YYYY-MM-DD OR MM-DD-YYYY)
#they are in US '^\d{2}/\d{2}/\d{4}$ format.

##completely invalid or partial dates
SELECT birth_date
FROM cognito_raw2
WHERE birth_date ~ '[A-Za-z]';

SELECT birth_date from cognito_raw2 where birth_date ~ 's';
# no partial or invalid dates

##inconsistent email
SELECT*
FROM cognito_raw2
WHERE email NOT LIKE '%@%.%';
## all emails are in the correct format

#null/mising values
SELECT 
    COUNT (*) FILTER (WHERE user_id IS NULL) AS null_user,
    COUNT(*) FILTER (WHERE email IS NULL) AS null_email,
    COUNT(*) FILTER (WHERE gender = 'NULL') AS null_gender,
    COUNT(*) FILTER (WHERE birth_date = 'NULL') AS null_birthdate,
    COUNT(*) FILTER (WHERE city = 'NULL') AS null_city,
    COUNT(*) FILTER (WHERE zipcode = 'NULL') AS null_zip,
    COUNT(*) FILTER (WHERE state = 'NULL') AS null_state
FROM cognito_raw2;
#There are 42862 null values for gender and birthdate, 42863 for city, 42867 for zip, and 42864 for state
#however there are fake nulls which should be set in proper SQL nulls#no null values for user_id and emails
SELECT DISTINCT (state)
FROM cognito_raw2
where state like 'NUL%'

SELECT
    COUNT(*) FILTER (
        WHERE
            user_id = ''
    ) AS empty_user,
    COUNT(*) FILTER (
        WHERE
            email = ''
    ) AS empty_email,
    COUNT(*) FILTER (
        WHERE
            gender = ''
    ) AS empty_gender,
    COUNT(*) FILTER (
        WHERE
            birth_date = ''
    ) AS empty_birthdate,
    COUNT(*) FILTER (
        WHERE
            city = ''
    ) AS empty_city,
    COUNT(*) FILTER (
        WHERE
            zipcode = ''
    ) AS empty_zip,
    COUNT(*) FILTER (
        WHERE
            state = ''
    ) AS empty_state
FROM cognito_raw2;
##there were no empty fields from the cognito_raw table.

#2.cohort_raw table
select *
from cohort_raw
limit 30;
##duplicates
SELECT cohort_id, cohort_code, start_date, end_date, size, COUNT(*)
FROM cohort_raw
GROUP BY cohort_id, cohort_code, start_date, end_date, size
HAVING COUNT(*)>1;
##No duplicated record in the cohortraw table.
SELECT cohort_code, COUNT(*)
FROM cohort_raw
GROUP BY cohort_code
HAVING COUNT(*)>1;
# no duplicate cohort_code can be used as primary key

##Inconsistent naming (spaces, symbols)
SELECT DISTINCT cohort_id
FROM cohort_raw
WHERE cohort_id NOT ILIKE 'cohort#';
#zero record. However, the cohort_id was duplicated in all records

SELECT *
FROM cohort_raw
WHERE cohort_id <> TRIM(cohort_id)
        OR cohort_code <> TRIM(cohort_code);
#zero record
SELECT DISTINCT cohort_code, COUNT(*)
FROM cohort_raw
GROUP BY cohort_code
HAVING COUNT(*)>1;
#no duplicate cohort_code record
SELECT DISTINCT COUNT (*)
FROM cohort_raw;

SELECT DISTINCT
    cohort_code
FROM cohort_raw
WHERE
    cohort_code ~ '[^A-Za-z0-9_-]';
#no special characters in the cohort_code table

-- Check for valid Unix timestamps in start_date
SELECT start_date FROM cohort_raw WHERE start_date::TEXT ~ '^\d+$';
#all records are in correct unix timestamp
-- Check for invalid Unix timestamps in end_date
SELECT end_date FROM cohort_raw WHERE end_date::TEXT !~ '^\d+$';
# zero record on invalid unix timestamp

#Great! To convert Unix timestamps into human-readable ISO dates (e.g., YYYY-MM-DD) and then check for weird or invalid dates like:
--Future dates
--Very old dates (before, say, 1970)
--Clearly wrong timestamps
SELECT
    cohort_id,
    start_date,
    to_timestamp(start_date::BIGINT / 1000)::DATE AS start_date_converted
FROM cohort_raw
LIMIT 100;

--start and end dates in the future
SELECT
    cohort_id,
    start_date,
    to_timestamp(start_date::BIGINT / 1000)::DATE AS start_date_converted
FROM cohort_raw
WHERE
    to_timestamp(start_date::BIGINT / 1000)::DATE > CURRENT_DATE;

SELECT
    cohort_id,
    end_date,
    to_timestamp(end_date::BIGINT / 1000)::DATE AS end_date_converted
FROM cohort_raw
WHERE
    to_timestamp(end_date::BIGINT / 1000)::DATE > CURRENT_DATE;

SELECT
    cohort_id,
    end_date,
    to_timestamp(end_date::BIGINT / 1000)::DATE AS end_date_converted
FROM cohort_raw
WHERE
    to_timestamp(start_date::BIGINT / 1000)::DATE < DATE '1970-01-01';

SELECT
    cohort_id,
    end_date,
    to_timestamp(end_date::BIGINT / 1000)::DATE AS end_date_converted
FROM cohort_raw
WHERE
    to_timestamp(end_date::BIGINT / 1000)::DATE > CURRENT_DATE;
# zero wrong timestamp

#Null/missing values
SELECT COUNT(*) FILTER (WHERE cohort_id IS NULL)as null_id,
    COUNT(*) FILTER (WHERE cohort_code IS NULL) as null_code,
   COUNT(*) FILTER (WHERE start_date IS NULL)as null_startdate,
    COUNT(*)FILTER (WHERE end_date IS NULL)as null_enddate,
    COUNT(*)FILTER (WHERE size IS NULL) as null_size
FROM cohort_raw;
# no null values
SELECT COUNT(*) FILTER (WHERE cohort_id = '')as empty_id,
    COUNT(*) FILTER (WHERE cohort_code = '') as empty_code,
    COUNT(*) FILTER (WHERE start_date::TEXT = '')as empty_startdate,
    COUNT(*)FILTER (WHERE end_date::TEXT = '')as empty_enddate,
    COUNT(*)FILTER (WHERE size::TEXT ='') as empty_size
FROM cohort_raw;
#Zero records
SELECT *
FROM cohort_raw
where cohort_code like 'NULL%';

##Numerical precision
SELECT*
FROM cohort_raw WHERE size <0;

SELECT * FROM cohort_raw ORDER BY size DESC limit 10;
# No numerical outliers or anomalies

##3. learner_opportunity_raw
SELECT*
FROM learner_opportunity_raw
limit 100;

select *
from learner_opportunity_raw
where
    assigned_cohort = 'B456514';
# checking for Duplicates
-- Entire row duplicates
SELECT
    enrollment_id,
    learner_id,
    assigned_cohort,
    apply_date,
    status,
    COUNT(*)
FROM learner_opportunity_raw
GROUP BY
    enrollment_id,
    learner_id,
    assigned_cohort,
    apply_date,
    status
HAVING
    COUNT(*) > 1;
#no duplicates

--
SELECT enrollment_id, COUNT(*)
FROM learner_opportunity_raw
GROUP BY
    enrollment_id
HAVING
    COUNT(*) > 1;
#enrollment_id are not unique. They are foreign key reference cognito_raw2 (user_id).
SELECT learner_id, COUNT(*)
FROM learner_opportunity_raw
GROUP BY learner_id
HAVING COUNT(*) > 1;
#learner_id not unique . They are foreign key reference opportunity_raw (opportunity_id).
SELECT assigned_cohort, COUNT(*)
FROM learner_opportunity_raw
GROUP BY assigned_cohort
HAVING COUNT(*) > 1;
# assigned_cohort is not unique. They are foreign key reference cohort_raw (cohort_code)

-- NULL or empty values across all columns
SELECT
    COUNT(*) FILTER (
        WHERE
            enrollment_id IS NULL
    ) AS null_enrollment_id,
    COUNT(*) FILTER (
        WHERE
            learner_id IS NULL
    ) AS null_learner_id,
    COUNT(*) FILTER (
        WHERE
            assigned_cohort = 'NULL'
    ) AS null_assigned_cohort,
    COUNT(*) FILTER (
        WHERE
            apply_date = 'NULL'
    ) AS null_apply_date,
    COUNT(*) FILTER (
        WHERE
            status::TEXT = 'NULL'
    ) AS null_status
FROM learner_opportunity_raw;
#13318 null_assigned_cohort but fake nulls need to be converted to the correct sql null
# 188 null_apply_date but fake nulls need to be converted to the correct sql null
SELECT
  COUNT(*) FILTER (WHERE enrollment_id = '') AS missing_enrollment_id,
  COUNT(*) FILTER (WHERE learner_id = '') AS missing_learner_id,
  COUNT(*) FILTER (WHERE assigned_cohort = '') AS missing_assigned_cohort,
  COUNT(*) FILTER (WHERE apply_date = '') AS missing_apply_date,
  COUNT(*) FILTER (WHERE status::TEXT = '') AS missing_status
FROM learner_opportunity_raw;

##checking for inconsistent formats
SELECT enrollment_id
FROM learner_opportunity_raw
WHERE enrollment_id NOT LIKE 'Learner#%';
# all enrollment_id follows the 'Learner#%' format
SELECT learner_id
FROM learner_opportunity_raw
WHERE learner_id NOT LIKE 'Opportunity#%';
# all learner_id follows the 'Opportunity#%' format

SELECT assigned_cohort
FROM learner_opportunity_raw
WHERE LENGTH(assigned_cohort) != 7 AND assigned_cohort != 'NULL';
#zero records. all assigned_cohort records that are not null does not exceed 7 characters
-- Check for non-ISO format
SELECT apply_date
FROM learner_opportunity_raw
WHERE
    apply_date NOT LIKE '%Z'
    AND apply_date <> 'NULL';
#zero records

#This will return records where the value has spaces at the beginning or end.
SELECT enrollment_id
FROM learner_opportunity_raw
WHERE enrollment_id != TRIM(enrollment_id);
#zero records

## 4. opportunity_raw table
select*
from opportunity_raw;

#checking for duplicate values
select count (*), opportunity_id, opportunity_name, category, opportunity_code, tracking_questions
from opportunity_raw
group by opportunity_id, opportunity_name, category, opportunity_code,tracking_questions
HAVING count(*)>1;

select distinct
    count(*),
    opportunity_id
from opportunity_raw
GROUP BY
    opportunity_id
HAVING
    count(*) > 1;

select distinct
    count(*),
    opportunity_code
from opportunity_raw
GROUP BY
    opportunity_code
HAVING
    count(*) > 1;
##No duplicate records after running the two queries. Possibly the opportunity_id is the pk since they are uniquely identified for each record
## the opportunity_code is also uniquely identified

select distinct count(*),category
from opportunity_raw
GROUP BY category
HAVING count(*)>=1;
#there are six categories: internship 43 records, Event and Competition 41 records each, career 23 records, course 18 records, and masterclass 11 records.
--Missing Values
SELECT
    COUNT(*) FILTER (
        WHERE
            opportunity_id IS NULL
            OR opportunity_id = ''
    ) AS missing_id,
    COUNT(*) FILTER (
        WHERE
            opportunity_code IS NULL
            OR opportunity_code = ''
    ) AS missing_code,
    COUNT(*) FILTER (
        WHERE
            opportunity_name IS NULL
            OR opportunity_name = ''
    ) AS missing_name,
    COUNT(*) FILTER (
        WHERE
            category IS NULL
            OR category = ''
    ) AS missing_category,
    COUNT(*) FILTER (
        WHERE
            tracking_questions = 'NULL'
            OR tracking_questions = ''
    ) AS missing_questions
FROM opportunity_raw;
--there are 69 missing values as null in the tracking_questions field.
##checking for Inconsistent format
SELECT *
FROM opportunity_raw
WHERE opportunity_name <> TRIM(opportunity_name);
--five opportunity names have trailing or leading spaces

SELECT * FROM opportunity_raw WHERE category <> TRIM(category);
--no record in category have leading or trailing spaces
SELECT *
FROM opportunity_raw
WHERE
    opportunity_code <> TRIM(opportunity_code);
--no record in opportunity_code have trailing or leading spaces
SELECT * FROM opportunity_raw WHERE LENGTH(opportunity_code) <> 7;
--no opportunity_code exceeding 7 characters
SELECT *
FROM opportunity_raw
WHERE
    opportunity_id NOT LIKE 'Opportunity#%';
--all opportunity_id have the same starting format
SELECT *
FROM opportunity_raw
WHERE
    tracking_questions != 'NULL'
    AND (
        tracking_questions NOT LIKE '{%'
        OR tracking_questions NOT LIKE '%}'
    );
--no record. meaning all tracking questions do not have malformed array values i.e not starting with { or ending with}

##5. learner_raw table
select*
from learner_raw;
#checking for full row duplicate values
select count (*), learner_id, country, degree, institution, major
from learner_raw
group by learner_id, country, degree, institution,major
HAVING count(*)>1;

select distinct
    count(*),
    learner_id
from learner_raw
GROUP BY
    learner_id
HAVING
    count(*) > 1;
--No duplicate records after running the two queries. Possibly the leaner_id is the pk since they are uniquely identified for each record
--However, when we remove the prefix "Learner#" in each of the learner_id, they can be mapped on the user_id in cognito_raw table
--implies that leaner_id can be fk reference to cognito_raw (user_id)

select distinct
    count(*),
    degree
from learner_raw
GROUP BY
    degree
HAVING
    count(*) >= 1;
--there are seven distinct degree types: Graduate student 31806 records, Undergraduate Student 30709 records
-- and Not in Education 6319 records, High school studnets 4109 records, other professional 2997 records, and the rest null values.

--Checking for Missing Values
SELECT
    COUNT(*) FILTER (
        WHERE
            learner_id IS NULL
            OR learner_id = ''
    ) AS missing_id,
    COUNT(*) FILTER (
        WHERE
            country = 'NULL'
            OR country = ''
    ) AS missing_country,
    COUNT(*) FILTER (
        WHERE
            degree = 'NULL'
            OR degree = ''
    ) AS missing_degree,
    COUNT(*) FILTER (
        WHERE
            institution = 'NULL'
            OR institution = ''
    ) AS missing_institution,
    COUNT(*) FILTER (
        WHERE
            major = 'NULL'
            OR major = ''
    ) AS missing_major
FROM learner_raw;
--there are 52693 missing values as null in the degree and institution fields, 52694 in the major field, and 2275 in the country field.
--checking for Inconsistent format
SELECT * FROM learner_raw WHERE country <> TRIM(country);
--no country record has a trailing or leading spaces

SELECT * FROM learner_raw WHERE degree <> TRIM(degree);
--no record in degree has leading or trailing spaces
SELECT * FROM learner_raw WHERE institution <> TRIM(institution);
--no record in institution has trailing or leading spaces
SELECT * FROM learner_raw WHERE major <> TRIM(major);
--no record in major field has trailing or leading space
SELECT * FROM learner_raw WHERE learner_id NOT LIKE 'Learner#%';
--all learner_id have the same starting format
SELECT DISTINCT
    COUNT(*),
    country
FROM learner_raw
GROUP BY
    country
HAVING
    COUNT(*) >= 1;
--no incorrect capitalization in country. however one record cote d%27ivoire need to be changed to cote d'ivoire to encode the special character
SELECT DISTINCT
    COUNT(*),
    degree
FROM learner_raw
GROUP BY
    degree
HAVING
    COUNT(*) >= 1;
--no incorrect capitalization
SELECT DISTINCT
    COUNT(*),
    institution
from learner_raw
GROUP BY
    institution
HAVING
    COUNT(*) >= 1;
--there is incorrect capitalisation
select *
from opportunity_raw
where
    opportunity_id = 'Opportunity#0000000010WCBS50CYGDX97ES4';

select * from opportunity_raw;

--opharn checks
--learner_opportunity_raw//enrollment_id fk reference cognito_raw//user_id
SELECT enrollment_id
FROM learner_opportunity_raw
WHERE
    REPLACE(enrollment_id, 'Learner#', '') NOT IN (
        SELECT user_id
        FROM cognito_raw2
    );
--there are 275 opharned records (enrollment_id). They could not be mapped on cognito_raw2(user_id)

--learner_opportunity_raw//assigneed_cohort fk reference cohort_raw//cohort_code
select assigned_cohort
from learner_opportunity_raw
where
    assigned_cohort not in (
        SELECT cohort_code
        from cohort_raw
    );
--13318 records (assigned_cohort) are opharn-- They are all null vallues which were flagged in null values check

--laerner_opportunity_raw//learner_id fk reference opportunity_raw//opportunity_id
SELECT learner_id
FROM learner_opportunity_raw
WHERE
    learner_id NOT IN (
        SELECT opportunity_id
        FROM opportunity_raw
    );
--no oprhan record. All records mapped

--learner_raw//learner_id fk reference cognito_raw2//user_id
SELECT learner_id
FROM learner_raw
WHERE
    REPLACE(learner_id, 'Learner#', '') NOT IN (
        SELECT user_id
        FROM cognito_raw2
    );
--85 records (learner_id) were not mapped on the user_id

#6. marketing_campaign_data_all_accounts_2023_2024
select*
from marketing_campaign_data_all_accounts_2023_2024;
-- checking for missing values
SELECT
    COUNT(*) AS total_records,
    COUNT(*) FILTER (
        WHERE
            "Ad_account_name" IS NULL
            OR "Ad_account_name" = ''
    ) AS missing_account_name,
    COUNT(*) FILTER (
        WHERE
            campaign_name IS NULL
            OR campaign_name = ''
    ) AS missing_campaign_name,
    COUNT(*) FILTER (
        WHERE
            delivery_status IS NULL
            OR delivery_status = ''
    ) AS missing_delivery_status,
    COUNT(*) FILTER (
        WHERE
            delivery_level IS NULL
            OR delivery_level = ''
    ) AS missing_delivery_level,
    COUNT(*) FILTER (
        WHERE
            reach IS NULL
            OR reach::TEXT = ''
    ) AS missing_reach,
    COUNT(*) FILTER (
        WHERE
            outbound IS NULL
            OR outbound::TEXT = ''
    ) AS missing_outbound,
    COUNT(*) FILTER (
        WHERE
            empty IS NULL
            OR empty::TEXT = ''
    ) AS missing_empty,
    COUNT(*) FILTER (
        WHERE
            result_type IS NULL
            OR result_type = ''
    ) AS missing_result_type,
    COUNT(*) FILTER (
        WHERE
            result IS NULL
            OR result::TEXT = ''
    ) AS missing_result,
    COUNT(*) FILTER (
        WHERE
            cost_per_result IS NULL
    ) AS missing_cost_per_result,
    COUNT(*) FILTER (
        WHERE
            "amount_spent_AED" IS NULL
    ) AS missing_amount_spend,
    COUNT(*) FILTER (
        WHERE
            cost_per_link_click IS NULL
    ) AS missing_cost_per_click,
    COUNT(*) FILTER (
        WHERE
            reporting_starts IS NULL
    ) AS missing_reporting_date
FROM
    marketing_campaign_data_all_accounts_2023_2024;
##total recors 142, missing Ad account_name, delivery_status, delivery_level, reach, result_type, cost_per_results, and reporting_starts had one 1 missing value each,
## missing_campaign_name, outbound, empty(unnamed field), and cost_per_click had 3 missing values each, amonut_spend and results had no missing value in their records.
--- checking for duplicates
## full row duplicate check
SELECT , COUNT() 
FROM marketing_campaign_data_all_accounts_2023_2024
GROUP BY 
  "Ad_account_name", campaign_name, delivery_status, delivery_level,
  reach, outbound, empty, result_type, result,
  cost_per_result, "amount_spent_AED", cost_per_link_click, reporting_starts
HAVING COUNT(*) > 1;
## no duplicates in full row check

SELECT campaign_name, COUNT(*) 
FROM marketing_campaign_data_all_accounts_2023_2024
GROUP BY campaign_name
HAVING COUNT(*) > 1;
## 2 duplicate records for "EVENT: Social Impact Initiative" and "Dec | Materclasses | Block Chain Essentials Masterclass" each with three null values
SELECT "Ad_account_name", COUNT(*) 
FROM marketing_campaign_data_all_accounts_2023_2024
GROUP BY "Ad_account_name"
HAVING COUNT(*) > 1;
##1 null value, 26 records as "Brand Awareness", 91 "SLU", and 24"RIT"

--Incosnsistent checks
SELECT *
FROM
    marketing_campaign_data_all_accounts_2023_2024
WHERE
    LENGTH("Ad_account_name") != LENGTH(TRIM("Ad_account_name"))
    OR LENGTH(campaign_name) != LENGTH(TRIM(campaign_name))
    OR LENGTH(delivery_status) != LENGTH(TRIM(delivery_status))
    OR LENGTH(delivery_level) != LENGTH(TRIM(delivery_level))
    OR LENGTH(result_type) != LENGTH(TRIM(result_type))
    OR LENGTH(reporting_starts) != LENGTH(TRIM(reporting_starts));
-- no trailing or leading values in the ad account name, campaign name, delivery status, deleivery level, result type and reportimg starts
## Data Format Checks
SELECT reporting_starts,
       TO_DATE(reporting_starts, 'DD/MM/YYYY') AS parsed_date
FROM marketing_campaign_data_all_accounts_2023_2024
WHERE reporting_starts IS NOT NULL;

SELECT DISTINCT (reporting_starts)
FROM
    marketing_campaign_data_all_accounts_2023_2024;
## all date records are in the correct US format with only one null value

#Negative values Check
SELECT *
FROM marketing_campaign_data_all_accounts_2023_2024
WHERE 
  reach < 0 OR outbound < 0 OR result < 0 OR
  cost_per_result < 0 OR "amount_spent_AED" < 0 OR cost_per_link_click < 0 OR empty<0;
## no negative values
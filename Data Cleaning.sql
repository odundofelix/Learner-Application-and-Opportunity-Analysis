--Table One: Cognito_raw2
SELECT * FROM cognito_raw2_staging;

CREATE TABLE cognito_raw2_staging (
    LIKE cognito_raw2 INCLUDING ALL
);

INSERT INTO cognito_raw2_staging SELECT * FROM cognito_raw2;

#Handling missing values
UPDATE cognito_raw2_staging
SET
    gender = NULLIF(gender, 'NULL'),
    birth_date = NULLIF(birth_date, 'NULL'),
    city = NULLIF(city, 'NULL'),
    zipcode = NULLIF(zipcode, 'NULL'),
    state = NULLIF(state, 'NULL');
-- Default gender to 'Not Specified' if NULL
UPDATE cognito_raw2_staging
SET
    gender = 'Not Specified'
WHERE
    gender IS NULL;

-- default birth_date to 'Not Given'
UPDATE cognito_raw2_staging
SET
    birth_date = 'Not Given'
WHERE
    birth_date IS NULL;

--fix gender spelling (URL-encode issue)
UPDATE cognito_raw2_staging
SET
    gender = 'Don''t want to specify'
WHERE
    gender = 'Don%27t want to specify';

-- standardize city
UPDATE cognito_raw2_staging
SET
    city = INITCAP(city)
WHERE
    city IS NOT NULL;
--standardize state
UPDATE cognito_raw2_staging
SET
    state = UPPER(state)
WHERE
    state IS NOT NULL;

--convert birth_date to ISO format
ALTER TABLE cognito_raw2_staging ADD COLUMN birth_date_iso DATE;

-- Step 1: Add new column to store cleaned date
ALTER TABLE cognito_raw2 ADD COLUMN birth_date_iso DATE;

-- Step 2: Convert only properly formatted values (DD/MM/YYYY) into the new column
UPDATE cognito_raw2_staging
SET
    birth_date_iso = TO_DATE(birth_date, 'DD/MM/YYYY')
WHERE
    birth_date ~ '^\d{2}/\d{2}/\d{4}$';

SELECT DISTINCT
    birth_date
FROM cognito_raw2_staging
WHERE
    birth_date IS NOT NULL
    AND birth_date <> 'NULL'
    AND NOT birth_date ~ '^\d{2}/\d{2}/\d{4}$';

select * from cognito_raw2_staging where gender like 'Do%';

ALTER TABLE cognito_raw2_staging DROP COLUMN birth_date;

ALTER TABLE cognito_raw2_staging
RENAME COLUMN birth_date_iso TO birth_date;

ALTER TABLE cognito_raw2_staging ADD PRIMARY KEY (user_id);

#Table 2: cohort_raw
--standardizing formats
SELECT * FROM cohort_raw_staging;

CREATE TABLE cohort_raw_staging (LIKE cohort_raw INCLUDING ALL);

INSERT INTO cohort_raw_staging SELECT * FROM cohort_raw;

ALTER TABLE cohort_raw_staging
ADD COLUMN start_date_iso DATE,
ADD COLUMN end_date_iso DATE;

UPDATE cohort_raw_staging
SET
    start_date_iso = TO_TIMESTAMP(start_date::BIGINT / 1000)::DATE,
    end_date_iso = TO_TIMESTAMP(end_date::BIGINT / 1000)::DATE;

ALTER TABLE cohort_raw_staging
RENAME COLUMN start_date_iso TO start_date;

#Table Three: learner_opportunity_raw

SELECT*
FROM learner_opportunity_raw_staging;

CREATE TABLE learner_opportunity_raw_staging (
    LIKE learner_opportunity_raw INCLUDING ALL
);

INSERT INTO
    learner_opportunity_raw_staging
SELECT *
FROM learner_opportunity_raw;

-- Step 1: Normalize fake NULLs
UPDATE learner_opportunity_raw_staging
SET
    assigned_cohort = NULL
WHERE
    assigned_cohort IN ('NULL', '');

UPDATE learner_opportunity_raw_staging
SET
    apply_date = NULL
WHERE
    apply_date IN ('NULL', '');

UPDATE learner_opportunity_raw_staging
SET
    status = NULL
WHERE
    status IN ('NULL', '');

SELECT DISTINCT
    COUNT(*),
    status
FROM
    learner_opportunity_raw_staging
GROUP BY
    status;

# Table 4: opportunity_raw
select*
 from opportunity_raw_staging;

CREATE TABLE opportunity_raw_staging (
    LIKE opportunity_raw INCLUDING ALL
);

INSERT INTO opportunity_raw_staging SELECT * FROM opportunity_raw;

UPDATE opportunity_raw_staging
SET
    tracking_questions = NULL
WHERE
    tracking_questions = 'NULL';

--standardizing_format
UPDATE opportunity_raw_staging
SET
    opportunity_name = TRIM(opportunity_name)
WHERE
    opportunity_name <> TRIM(opportunity_name);

ALTER TABLE opportunity_raw_staging ADD PRIMARY KEY (opportunity_id);

#Table 5: learner_raw
CREATE TABLE learner_raw_staging
(LIKE learner_raw INCLUDING ALL);

INSERT INTO learner_raw_staging SELECT * FROM learner_raw;

select * from learner_raw_staging;
--standardizing
UPDATE learner_raw_staging SET country = NULL WHERE country = 'NULL';

UPDATE learner_raw_staging SET degree = NULL WHERE degree = 'NULL';

UPDATE learner_raw_staging
SET
    institution = NULL
WHERE
    institution = 'NULL';

UPDATE learner_raw_staging SET major = NULL WHERE major = 'NULL';

select * from learner_raw_staging where major is null;

-- normalize learner_id
SELECT learner_id, REPLACE(learner_id, 'Learner#', '') AS learner_fk
FROM learner_raw_staging;

UPDATE learner_raw_staging
SET
    country = 'Côte d''Ivoire'
WHERE
    country = 'Cote d%27ivoire';

UPDATE learner_raw_staging
SET
    institution = INITCAP(institution)
WHERE
    institution IS NOT NULL;

select * from learner_raw_staging where country like 'Co%';

#Table 6: marketing campaign
CREATE TABLE marketing_campaign_data_staging
(LIKE marketing_campaign_data_all_accounts_2023_2024);

INSERT INTO
    marketing_campaign_data_staging
SELECT *
FROM
    marketing_campaign_data_all_accounts_2023_2024;

select * from marketing_campaign_data_staging;

UPDATE marketing_campaign_data_staging
SET
    "Ad_account_name" = 'Unknown'
WHERE
    "Ad_account_name" IS NULL;

UPDATE marketing_campaign_data_staging
SET
    delivery_status = 'Unknown'
WHERE
    delivery_status IS NULL;

UPDATE marketing_campaign_data_staging
SET
    campaign_name = 'Unknown'
WHERE
    campaign_name IS NULL;

UPDATE marketing_campaign_data_staging
SET
    unnamed = 0
WHERE
    unnamed IS NULL;

DELETE FROM marketing_campaign_data_staging
WHERE
    "Ad_account_name" = '  unknown';

select *
from
    marketing_campaign_data_staging
where
    campaign_name = 'EVENT: Social Impact Initiative';

ALTER TABLE marketing_campaign_data_staging RENAME empty TO unnamed;

ALTER TABLE marketing_campaign_data_staging
ADD COLUMN reporting_starts_iso DATE;

UPDATE marketing_campaign_data_staging
SET
    reporting_starts_iso = TO_DATE(
        reporting_starts,
        'DD/MM/YYYY'
    )
WHERE
    reporting_starts ~ '^\d{2}/\d{2}/\d{4}$';

ALTER TABLE marketing_campaign_data_staging DROP reporting_starts;

ALTER TABLE marketing_campaign_data_staging
RENAME reporting_starts_iso TO reporting_starts;

select * from marketing_campaign_data_staging;

--opharn checks
--learner_opportunity_raw//enrollment_id fk reference cognito_raw//user_id
SELECT enrollment_id
FROM
    learner_opportunity_raw_staging
WHERE
    REPLACE(enrollment_id, 'Learner#', '') NOT IN (
        SELECT user_id
        FROM cognito_raw2_staging
    );
--there are 275 opharned records (enrollment_id). They could not be mapped on cognito_raw2(user_id)

--learner_opportunity_raw//assigneed_cohort fk reference cohort_raw//cohort_code
select assigned_cohort
from
    learner_opportunity_raw_staging
where
    assigned_cohort not in (
        SELECT cohort_code
        from cohort_raw_staging
    );
--The 13318 records (assigned_cohort) opharn that were before cleaning are gone.
--laerner_opportunity_raw//learner_id fk reference opportunity_raw//opportunity_id
SELECT learner_id
FROM
    learner_opportunity_raw_staging
WHERE
    learner_id NOT IN (
        SELECT opportunity_id
        FROM opportunity_raw_staging
    );
--no oprhan record. All records mapped

--learner_raw//learner_id fk reference cognito_raw2//user_id
SELECT learner_id
FROM learner_raw_staging
WHERE
    REPLACE(learner_id, 'Learner#', '') NOT IN (
        SELECT user_id
        FROM cognito_raw2_staging
    );
--85 records (learner_id) were not mapped on the user_id

select * from opportunity_raw_staging where tracking_questions = '';

CALL run_etl_master_user_opportunity1 ();
-- Executes logic
SELECT * FROM master_user_opportunity1;
-- View data afterwards
select * from cognito_raw2_staging1 where birth_date IS NULL;

select apply_date from master_user_opportunity;
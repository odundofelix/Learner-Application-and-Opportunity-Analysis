CREATE OR REPLACE PROCEDURE public.run_etl_master_user_opportunity()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- STEP 1: Create STAGING TABLES if not exist
    CREATE TABLE IF NOT EXISTS cognito_raw2_staging AS TABLE cognito_raw2 WITH NO DATA;
    CREATE TABLE IF NOT EXISTS learner_raw_staging AS TABLE learner_raw WITH NO DATA;
    CREATE TABLE IF NOT EXISTS learner_opportunity_raw_staging AS TABLE learner_opportunity_raw WITH NO DATA;
    CREATE TABLE IF NOT EXISTS opportunity_raw_staging AS TABLE opportunity_raw WITH NO DATA;
    CREATE TABLE IF NOT EXISTS cohort_raw_staging AS TABLE cohort_raw WITH NO DATA;

    --STEP 2: Data Cleaning
--Table One: Cognito_raw2
INSERT INTO cognito_raw2_staging SELECT * FROM cognito_raw2;
--Handling missing values
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
ALTER TABLE cognito_raw2_staging
ADD COLUMN birth_date_new DATE;
UPDATE cognito_raw2_staging
SET birth_date_new = TO_DATE(birth_date, 'DD/MM/YYYY')
WHERE birth_date ~ '^\d{2}/\d{2}/\d{4}$';
ALTER TABLE cognito_raw2_staging DROP COLUMN birth_date;
ALTER TABLE cognito_raw2_staging RENAME COLUMN birth_date_new TO birth_date;


ALTER TABLE cognito_raw2_staging ADD PRIMARY KEY (user_id);

--Table 2: cohort_raw
--standardizing formats
INSERT INTO cohort_raw_staging SELECT * FROM cohort_raw;

ALTER TABLE cohort_raw_staging
ADD COLUMN start_date_iso DATE,
ADD COLUMN end_date_iso DATE;

UPDATE cohort_raw_staging
SET
    start_date_iso = TO_TIMESTAMP(start_date::BIGINT / 1000)::DATE,
    end_date_iso = TO_TIMESTAMP(end_date::BIGINT / 1000)::DATE;

ALTER TABLE cohort_raw_staging
DROP start_date;
ALTER TABLE cohort_raw_staging
RENAME COLUMN start_date_iso TO start_date;

ALTER TABLE cohort_raw_staging
DROP end_date;
ALTER TABLE cohort_raw_staging
RENAME COLUMN end_date_iso TO end_date;

--Table Three: learner_opportunity_raw
INSERT INTO
    learner_opportunity_raw_staging
SELECT*
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

PERFORM DISTINCT
    COUNT(*),
    status
FROM
    learner_opportunity_raw_staging
GROUP BY
    status;

-- Table 4: opportunity_raw
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

--Table 5: learner_raw
INSERT INTO learner_raw_staging SELECT * FROM learner_raw;

PERFORM * from learner_raw_staging;
--standardizing
UPDATE learner_raw_staging SET country = NULL WHERE country = 'NULL';
UPDATE learner_raw_staging SET degree = NULL WHERE degree = 'NULL';

UPDATE learner_raw_staging
SET
    institution = NULL
WHERE
    institution = 'NULL';

UPDATE learner_raw_staging SET major = NULL WHERE major = 'NULL';

PERFORM * from learner_raw_staging where major is null;

-- normalize learner_id
PERFORM learner_id, REPLACE(learner_id, 'Learner#', '') AS learner_fk
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

PERFORM * from learner_raw_staging where country like 'Co%';


--Step 3 Table Creation Query
-- Create the Master Table
CREATE TABLE IF NOT EXISTS master_user_opportunity (
    user_id TEXT,--temporary as text was converted after loading to uuid
    email TEXT,
    gender TEXT,
    birth_date DATE,
    city TEXT,
    state TEXT,
    country TEXT,
    degree TEXT,
    institution TEXT,
    major TEXT,
    opportunity_code TEXT NOT NULL,
    opportunity_name TEXT,
    category TEXT,
    tracking_questions TEXT,
    apply_date TIMESTAMPTZ,
    status TEXT,
    cohort_code TEXT NOT NULL,
    start_date DATE,
    end_date DATE,
    size INTEGER,
    user_created_date TIMESTAMPTZ,
    user_last_modified_date TIMESTAMPTZ,
    CONSTRAINT pk_user_opportunity_cohort PRIMARY KEY (user_id, opportunity_code, cohort_code)
);
-- Useful for filters/search conditions
CREATE INDEX idx_user_id ON master_user_opportunity (user_id);
CREATE INDEX idx_opportunity_code ON master_user_opportunity (opportunity_code);
CREATE INDEX idx_cohort_code ON master_user_opportunity (cohort_code);

-- Other optional indexes
CREATE INDEX idx_email ON master_user_opportunity (email);
CREATE INDEX idx_gender ON master_user_opportunity (gender);
CREATE INDEX idx_category ON master_user_opportunity (category);
CREATE INDEX idx_degree ON master_user_opportunity (degree);

-- STEP 4: Load FINAL TABLE
    TRUNCATE TABLE master_user_opportunity;
--3 Load the Dataset
INSERT INTO master_user_opportunity (
    user_id, email, gender, birth_date, city, state, country, degree,
    institution, major, opportunity_code, opportunity_name, category,
    tracking_questions, apply_date, status, cohort_code, start_date,
    end_date, size, user_created_date, user_last_modified_date
)
WITH learner_cleaned AS (
    SELECT
        REPLACE(learner_id, 'Learner#', '') AS user_id,
        country,
        degree,
        institution,
        major
    FROM learner_raw_staging
),
opportunity_cleaned AS (
    SELECT
        o.opportunity_id,
        o.opportunity_name,
        o.opportunity_code,
        o.category,
        o.tracking_questions
    FROM opportunity_raw_staging o
),
learner_opp_cleaned AS (
    SELECT
        REPLACE(enrollment_id, 'Learner#', '') AS user_id,
        learner_id AS opportunity_id,
        assigned_cohort AS cohort_code,
        CAST(apply_date AS TIMESTAMPTZ),
        status
    FROM learner_opportunity_raw_staging
),
cohort_cleaned AS (
    SELECT
        cohort_code,
        start_date,
        end_date,
        size
    FROM cohort_raw_staging
)
SELECT
    u.user_id::TEXT AS user_id,
    u.email,
    u.gender,
    u.birth_date::DATE,
    u.city,
    u.state,
    lr.country,
    lr.degree,
    lr.institution,
    lr.major,
    o.opportunity_code,
    o.opportunity_name,
    o.category,
    o.tracking_questions,
    lo.apply_date::TIMESTAMPTZ,
    lo.status,
    lo.cohort_code,
    c.start_date,
    c.end_date,
    c.size,
    u.user_create_date::TIMESTAMPTZ AS user_created_date,
    u.user_last_modified_date::TIMESTAMPTZ
FROM cognito_raw2_staging u
JOIN learner_cleaned lr ON u.user_id::TEXT = lr.user_id
JOIN learner_opp_cleaned lo ON u.user_id::TEXT = lo.user_id
JOIN opportunity_cleaned o ON lo.opportunity_id = o.opportunity_id
JOIN cohort_cleaned c ON lo.cohort_code = c.cohort_code;

    RAISE NOTICE 'ETL process for master_user_opportunity completed successfully.';

END;
$procedure$

CALL run_etl_master_user_opportunity();  -- Executes logic
SELECT * FROM master_user_opportunity; -- View data afterwards


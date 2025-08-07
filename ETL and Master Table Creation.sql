#4.1 Extract the Data
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
        apply_date,
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
    u.user_id,
    u.email,
    u.gender,
    u.birth_date,
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
    lo.apply_date,
    lo.status,
    lo.cohort_code,
    c.start_date,
    c.end_date,
    c.size,
    u.user_create_date AS user_created_date,
    u.user_last_modified_date
FROM cognito_raw2_staging u
JOIN learner_cleaned lr ON u.user_id = lr.user_id
JOIN learner_opp_cleaned lo ON u.user_id = lo.user_id
JOIN opportunity_cleaned o ON lo.opportunity_id = o.opportunity_id
JOIN cohort_cleaned c ON lo.cohort_code = c.cohort_code;
#4.2 Create the Master Table
CREATE TABLE IF NOT EXISTS master_user_opportunity (
    user_id TEXT, --temporary as text,
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
    PRIMARY KEY (user_id, opportunity_code, cohort_code)
);
#4.3 Load the Dataset
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
    u.birth_date,
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
#4.4 Data Quality Checks
SELECT * from master_user_opportunity;

SELECT user_id
FROM master_user_opportunity
WHERE
    user_id IS NULL
    OR user_id !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

ALTER TABLE master_user_opportunity
ALTER COLUMN user_id TYPE UUID USING user_id::UUID;
--primary key check
SELECT
    user_id,
    opportunity_code,
    cohort_code,
    COUNT(*) AS duplicate_count
FROM master_user_opportunity
GROUP BY
    user_id,
    opportunity_code,
    cohort_code
HAVING
    COUNT(*) > 1;
-- zero record as expected, quality check passed
##null checks on critical fields
SELECT *
FROM master_user_opportunity
WHERE user_id IS NULL
   OR opportunity_code IS NULL
   OR cohort_code IS NULL;
--zero record as expected, quality check passed
## valid date range check
SELECT *
FROM master_user_opportunity
WHERE start_date > end_date;
--zero record as expected, quality check passed
##future date value check
SELECT *
FROM master_user_opportunity
WHERE birth_date > CURRENT_DATE
   OR apply_date > CURRENT_TIMESTAMP;
--zero record as expected, quality check passed
SELECT *
FROM master_user_opportunity
WHERE
    gender IS NULL
    OR city IS NULL
    OR country IS NULL;

SELECT
    COUNT(*) FILTER (
        WHERE
            gender IS NULL
    ) AS missing_gender,
    COUNT(*) FILTER (
        WHERE
            city IS NULL
    ) AS missing_city,
    COUNT(*) FILTER (
        WHERE
            country IS NULL
    ) AS missing_country
FROM master_user_opportunity;
-- 325 records that have null city and 37 missing country with zero record on gender
##cohort size check
SELECT DISTINCT cohort_code, size
FROM master_user_opportunity
WHERE size IS NULL OR size <= 0;
--zero record as expected, quality check passed
##status validation
SELECT DISTINCT status
FROM master_user_opportunity;
## row count sanity check
SELECT COUNT(*) FROM master_user_opportunity;
## same 100200 records as during extraction. quality check passed

##Quality Score Summary Report
SELECT 
  (SELECT COUNT(*) FROM master_user_opportunity) AS total_rows,
  (SELECT COUNT(*) FROM master_user_opportunity WHERE user_id IS NULL OR opportunity_code IS NULL OR cohort_code IS NULL) AS nulls_in_critical,
  (SELECT COUNT(*) FROM (
      SELECT user_id, opportunity_code, cohort_code, COUNT(*)
      FROM master_user_opportunity
      GROUP BY user_id, opportunity_code, cohort_code
      HAVING COUNT(*) > 1
  ) dup) AS duplicate_rows,
  (SELECT COUNT(*) FROM master_user_opportunity WHERE start_date > end_date) AS invalid_dates,
  (SELECT COUNT(*) FROM master_user_opportunity WHERE birth_date > CURRENT_DATE OR apply_date > CURRENT_TIMESTAMP) AS future_dates;
--

-- Useful for filters/search conditions
CREATE INDEX idx_user_id ON master_user_opportunity (user_id);

CREATE INDEX idx_opportunity_code ON master_user_opportunity (opportunity_code);

CREATE INDEX idx_cohort_code ON master_user_opportunity (cohort_code);

-- Other optional indexes
CREATE INDEX idx_email ON master_user_opportunity (email);

CREATE INDEX idx_gender ON master_user_opportunity (gender);

CREATE INDEX idx_category ON master_user_opportunity (category);

CREATE INDEX idx_degree ON master_user_opportunity (degree);
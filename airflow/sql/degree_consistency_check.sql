-- ==========================================================================================================
-- DATA QUALITY CHECK: DEGREE PROGRAM CONSISTENCY
-- ==========================================================================================================
--
-- QUERY METADATA:
--   Query ID:       DQ-002 (Airflow Data Quality Query 002)
--   Version:        1.1.0  
--   Author:         Data Engineering Team
--   Created:        2025-08-20
--   Last Modified:  2025-08-20
--   Environment:    Production (Airflow DAG)
--   Dependencies:   ADMISSIONS_RECORDS, REGISTRATION_HISTORY, ACTIVE_APPLICATIONS, STUDENT_NAMES
--
-- PURPOSE:
--   This query identifies inconsistencies between degree programs recorded in the
--   admissions system versus the registration system. This validation ensures
--   data integrity across systems when students transition from admitted to enrolled status.
--
-- BUSINESS CONTEXT:
--   Degree program mismatches can occur when admissions updates a student's program
--   after they've been processed into the registration system, or when programs
--   are consolidated due to low enrollment. Admissions staff typically maintain
--   accurate records, but communication gaps can create data inconsistencies.
--
-- TROUBLESHOOTING GUIDANCE:
--   When mismatches are detected:
--   1. Verify current program status in admissions system
--   2. Check registration system for recent degree changes
--   3. Confirm student intent through admissions counselor
--   4. Update registration records to match current program
--   5. Notify relevant departments of program changes
--
-- VALIDATION RULES:
--   Compares DEGREE_CODE from admissions records with DEGREE_CODE from registration
--   for students in deposited/confirmed status during current academic term.
--   Excludes placeholder programs (e.g., 'UNDECIDED', 'PENDING').
--
-- CHANGE LOG:
--   2025-08-20 Data Team
--     Anonymized table and field names for open source release
--     Maintained core business logic for cross-system validation
--     Added comprehensive documentation and troubleshooting steps
--
-- ==========================================================================================================

-- QUERY IMPLEMENTATION:
-- Joins admissions and registration data for deposited students
-- Identifies cases where degree codes don't match between systems
-- Includes student identification information for remediation

-- EXPECTED OUTPUT:
--   STUDENT_NAME, STUDENT_ID, ADMISSION_DATE, ADMISSIONS_DEGREE, REGISTRATION_DEGREE, MISMATCH_FLAG
--   Each row represents a student with inconsistent degree program codes

SELECT      DISTINCT sn.STUDENT_NAME,
            ar.STUDENT_ID,
            ar.ADMISSION_DATE,
            ar.DEGREE_CODE AS ADMISSIONS_DEGREE,
            rh.DEGREE_CODE AS REGISTRATION_DEGREE,
            CASE 
                WHEN ar.DEGREE_CODE <> rh.DEGREE_CODE THEN 'MISMATCH'
                ELSE NULL 
            END AS MISMATCH_FLAG

FROM        ADMISSIONS_RECORDS ar

-- Join with deposited/confirmed status students
JOIN        DEPOSITED_STATUS ds 
            ON ar.APPLICATION_STATUS = ds.STATUS_CODE

-- Get student name for identification
JOIN        STUDENT_NAMES sn 
            ON ar.STUDENT_ID = sn.STUDENT_ID

-- Link to registration system via application mapping
LEFT JOIN   APPLICATION_MAPPING am 
            ON ar.APPLICATION_ID = am.ADMISSIONS_APPLICATION_ID
LEFT JOIN   REGISTRATION_HISTORY rh 
            ON am.STUDENT_ID = rh.STUDENT_ID 
            AND am.REGISTRATION_SEQUENCE = rh.SEQUENCE_NUMBER

WHERE 
            -- Focus on current academic year and term
            ar.ACADEMIC_YEAR = YEAR(GETDATE()) 
            AND ar.TERM_CODE = 'FALL'
    
            -- Exclude placeholder/undecided programs
            AND ar.DEGREE_CODE NOT IN ('UNDECIDED', 'PENDING', 'PLACEHOLDER')
    
            -- Only include cases with actual degree code mismatches
            AND ar.DEGREE_CODE <> rh.DEGREE_CODE

ORDER BY    sn.STUDENT_NAME;
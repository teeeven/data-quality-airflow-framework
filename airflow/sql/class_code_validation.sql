-- ==========================================================================================================
-- DATA QUALITY CHECK: CLASS CODE VALIDATION
-- ==========================================================================================================
--
-- QUERY METADATA:
--   Query ID:       DQ-001 (Airflow Data Quality Query 001)
--   Version:        1.2.0
--   Author:         Data Engineering Team
--   Created:        2025-08-20
--   Last Modified:  2025-08-20
--   Environment:    Production (Airflow DAG)
--   Dependencies:   STUDENT_TERMS, CLASS_DEFINITION, ACTIVE_STUDENTS
--
-- PURPOSE:
--   This query identifies students enrolled in class codes that are inconsistent
--   with their declared degree program. This validation helps maintain data integrity
--   for downstream reporting systems that depend on accurate program enrollment data.
--
-- BUSINESS CONTEXT:
--   Class code mismatches typically occur during enrollment transitions (e.g., Summer to Fall)
--   when placeholder records are active, or when students change degree programs without
--   proper system updates. This is most common with graduate program transitions.
--
-- TROUBLESHOOTING GUIDANCE:
--   When violations are found:
--   1. Verify student's current degree program in student information system
--   2. Check enrollment history for recent program changes
--   3. Validate that class code assignments align with degree requirements
--   4. Update student records to reflect correct program enrollment
--
-- VALIDATION RULES:
--   BACHELOR_ARCH (Bachelor of Architecture):        Valid codes ['UG_1A','UG_1B','UG_2A','UG_2B','UG_3A','UG_3B','UG_4A','UG_4B','UG_5A','UG_TRANS']
--   MASTERS_1 (Master's Architecture Track 1):       Valid codes ['G1','G2','G3','G4','G5','G6','G_TRANS']  
--   MASTERS_2 (Master's Architecture Track 2):       Valid codes ['X1','X2','X3','X4','X_TRANS']
--   MASTERS_MEDIA (Master's Media Arts):              Valid codes ['N1','N2','N3']
--   MASTERS_TECH (Master's Architecture Technology):  Valid codes ['T1','T2','T3']  
--   MASTERS_URBAN (Master's Urban Design):            Valid codes ['C1','C2','C3']
--   MASTERS_THEORY (Master's Theory & Pedagogy):      Valid codes ['P1','P2','P3']
--   MASTERS_LANDSCAPE (Master's Landscape):           Valid codes ['L1','L2','L3']
--
-- CHANGE LOG:
--   2025-08-20 Data Team
--     Implemented explicit CASE statement logic for validation rules
--     Anonymized for open source release while preserving business logic
--     Added comprehensive documentation and troubleshooting guidance
--
-- ==========================================================================================================

-- QUERY IMPLEMENTATION:
-- Uses Common Table Expression (CTE) to identify students with mismatched class codes
-- Returns records where DATA_QUALITY_ALERT = 1 indicating validation failure
-- Joins student enrollment data with class definitions and active student filters

-- EXPECTED OUTPUT:
--   APPLICATION_ID, STUDENT_ID, ACADEMIC_YEAR, TERM_CODE, DEGREE_CODE, 
--   DIVISION_CODE, CLASS_CODE, CLASS_DESCRIPTION, DATA_QUALITY_ALERT
--   Each row represents a student with class code inconsistent with degree program

WITH validation_check AS (
        SELECT          st.APPLICATION_ID,
                        st.STUDENT_ID,
                        st.ACADEMIC_YEAR,
                        st.TERM_CODE,
                        st.DEGREE_CODE,
                        st.DIVISION_CODE,
                        st.CLASS_CODE,
                        cd.CLASS_DESCRIPTION,
                        CASE 
                            -- Bachelor of Architecture validation
                            WHEN st.DEGREE_CODE = 'BACHELOR_ARCH' 
                                AND st.CLASS_CODE NOT IN ('UG_1A','UG_1B','UG_2A','UG_2B','UG_3A','UG_3B','UG_4A','UG_4B','UG_5A','UG_TRANS') 
                                THEN 1
                            
                            -- Master's Architecture Track 1 validation    
                            WHEN st.DEGREE_CODE = 'MASTERS_1'    
                                AND st.CLASS_CODE NOT IN ('G1','G2','G3','G4','G5','G6','G_TRANS') 
                                THEN 1
                            
                            -- Master's Architecture Track 2 validation
                            WHEN st.DEGREE_CODE = 'MASTERS_2'    
                                AND st.CLASS_CODE NOT IN ('X1','X2','X3','X4','X_TRANS') 
                                THEN 1
                            
                            -- Master's Media Arts validation
                            WHEN st.DEGREE_CODE = 'MASTERS_MEDIA'    
                                AND st.CLASS_CODE NOT IN ('N1','N2','N3') 
                                THEN 1
                            
                            -- Master's Architecture Technology validation
                            WHEN st.DEGREE_CODE = 'MASTERS_TECH'    
                                AND st.CLASS_CODE NOT IN ('T1','T2','T3') 
                                THEN 1
                            
                            -- Master's Urban Design validation
                            WHEN st.DEGREE_CODE = 'MASTERS_URBAN'    
                                AND st.CLASS_CODE NOT IN ('C1','C2','C3') 
                                THEN 1
                            
                            -- Master's Theory & Pedagogy validation
                            WHEN st.DEGREE_CODE = 'MASTERS_THEORY'    
                                AND st.CLASS_CODE NOT IN ('P1','P2','P3') 
                                THEN 1
                            
                            -- Master's Landscape validation
                            WHEN st.DEGREE_CODE = 'MASTERS_LANDSCAPE'    
                                AND st.CLASS_CODE NOT IN ('L1','L2','L3') 
                                THEN 1
        
                                -- All other cases pass validation
                            ELSE 0
                            END AS DATA_QUALITY_ALERT
        
        FROM            STUDENT_TERMS st
        JOIN            CLASS_DEFINITION cd 
                        ON st.CLASS_CODE = cd.CLASS_CODE
        JOIN            ACTIVE_STUDENTS act 
                        ON st.STUDENT_ID = act.STUDENT_ID
)

-- Return only records that failed validation
SELECT          APPLICATION_ID,
                STUDENT_ID,
                ACADEMIC_YEAR,
                TERM_CODE,
                DEGREE_CODE,
                DIVISION_CODE,
                CLASS_CODE,
                CLASS_DESCRIPTION,
                DATA_QUALITY_ALERT
FROM            validation_check 
WHERE           DATA_QUALITY_ALERT = 1
ORDER BY        DEGREE_CODE
                , CLASS_CODE
                , STUDENT_ID;
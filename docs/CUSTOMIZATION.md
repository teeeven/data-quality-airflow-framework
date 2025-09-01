# Customization Guide

This guide explains how to adapt the Higher Education Data Quality Framework for your institution's specific needs, data schema, and business rules.

## Database Schema Customization

### Mapping Your Tables

The framework uses generic table names that you'll need to map to your institutional schema:

#### Student Enrollment Tables
```sql
-- Framework Reference → Your Institution
STUDENT_TERMS         → ENROLLMENT_RECORDS, etc.
CLASS_DEFINITION      → CLASS_CODES, ACADEMIC_LEVELS, etc.  
ACTIVE_STUDENTS       → CURRENT_STUDENTS, ACTIVE_ENROLLMENT, etc.
```

#### Cross-System Validation Tables  
```sql
-- Admissions System
ADMISSIONS_RECORDS    → CANDIDACY, APPLICATIONS, PROSPECTS, etc.
DEPOSITED_STATUS      → CONFIRMED_APPS, ENROLLED_STUDENTS, etc.

-- Registration System  
REGISTRATION_HISTORY  → DEGREE_HISTORY, STUDENT_PROGRAMS, etc.
APPLICATION_MAPPING   → STUDENT_BRIDGE, APP_TO_REG_LINK, etc.
```

### Field Mapping

Update field names to match your schema:

```sql
-- Common field mappings
APPLICATION_ID    → APPID, APP_ID, APPLICATION_NUMBER
STUDENT_ID        → ID_NUM, STUDENT_NUMBER, PERSON_ID
ACADEMIC_YEAR     → YR_CDE, YEAR_CODE, ACADEMIC_YEAR
TERM_CODE         → TRM_CDE, SEMESTER, TERM
DEGREE_CODE       → DEGREE_CDE, PROGRAM_CODE, MAJOR_CODE
CLASS_CODE        → CLASS_CDE, LEVEL_CODE, CLASSIFICATION
```

## Business Rule Customization

### Academic Program Validation

Customize degree program and class code mappings for your institution:

#### Traditional Semester System
```sql
-- Undergraduate programs
WHEN st.DEGREE_CODE = 'BA_LIBERAL_ARTS' 
    AND st.CLASS_CODE NOT IN ('FR','SO','JR','SR') 
    THEN 1

WHEN st.DEGREE_CODE = 'BS_ENGINEERING'
    AND st.CLASS_CODE NOT IN ('FR','SO','JR','SR','5Y') -- 5-year program
    THEN 1
```

#### Quarter System
```sql  
-- Graduate programs in quarter system
WHEN st.DEGREE_CODE = 'MS_COMPUTER_SCIENCE'
    AND st.CLASS_CODE NOT IN ('G1','G2','G3','G4','G5','G6','G7','G8')
    THEN 1
```

#### Professional Programs
```sql
-- Professional degree programs
WHEN st.DEGREE_CODE = 'JD_LAW'
    AND st.CLASS_CODE NOT IN ('1L','2L','3L')
    THEN 1

WHEN st.DEGREE_CODE = 'MD_MEDICINE' 
    AND st.CLASS_CODE NOT IN ('MS1','MS2','MS3','MS4')
    THEN 1
```

### Multi-Institution Support

For multi-campus or consortium institutions:

```sql
-- Add campus/institution filtering
SELECT ... 
FROM STUDENT_TERMS st
WHERE st.CAMPUS_CODE = 'MAIN_CAMPUS'
   OR st.CAMPUS_CODE IN ('BRANCH_A','BRANCH_B')
```

## Adding Custom Validation Checks

### 1. Create New Validation SQL

Create a new file `airflow/sql/your_custom_check.sql`:

```sql
-- ==========================================================================================================
-- DATA QUALITY CHECK: ENROLLMENT STATUS VALIDATION
-- ==========================================================================================================
-- PURPOSE: Validate that enrolled students have valid enrollment status codes

SELECT 
    st.STUDENT_ID,
    st.ENROLLMENT_STATUS,
    st.ACADEMIC_YEAR,
    st.TERM_CODE,
    'Invalid enrollment status' AS VIOLATION_REASON
FROM STUDENT_TERMS st
WHERE st.ENROLLMENT_STATUS NOT IN ('ENROLLED','REGISTERED','ACTIVE')
  AND st.TERM_CODE = 'CURRENT_TERM'
ORDER BY st.STUDENT_ID;
```

### 2. Add Task to DAG

Edit `dags/data_quality_system.py` to include your new check:

```python
@task()
def check_enrollment_status():
    """Execute enrollment status validation check."""
    logger.info("Starting enrollment status validation")
    
    results_df, violation_count = execute_data_quality_check(
        sql_file_path="enrollment_status_validation.sql",
        conn_id="institutional_database", 
        check_name="enrollment_status"
    )
    
    csv_filepath = None
    if violation_count > 0:
        csv_filepath = save_results_to_csv(
            results_df,
            "enrollment_status",
            "/usr/local/airflow/include/output/data_quality_system"
        )
    
    return {
        "check_name": "Enrollment Status Validation",
        "check_description": "Students with invalid enrollment status codes",
        "violation_count": violation_count,
        "csv_filepath": csv_filepath,
        "has_violations": violation_count > 0
    }

# Update the aggregation task
@task()
def aggregate_results(class_codes_result, degree_consistency_result, enrollment_status_result):
    checks = [class_codes_result, degree_consistency_result, enrollment_status_result]
    # ... rest of aggregation logic

# Update task dependencies  
enrollment_status_result = check_enrollment_status()
aggregated_result = aggregate_results(
    class_codes_result, 
    degree_consistency_result,
    enrollment_status_result  # Add new check
)
```

## Email Notification Customization

### Theme Customization

Create institution-specific color themes:

```python
# Institutional branding themes
UNIVERSITY_THEMES = {
    'enrollment': {
        'color': '#1f4788',      # University blue
        'bg': '#e8f0fe',
        'icon': '📊'
    },
    'academic': {
        'color': '#8b0000',      # University red  
        'bg': '#ffeaea',
        'icon': '🎓'
    },
    'financial': {
        'color': '#006400',      # Forest green
        'bg': '#f0fff0', 
        'icon': '💰'
    }
}
```

### Custom Email Templates

Create specialized templates for different stakeholder groups:

```python
def create_registrar_email(summary_data):
    """Email template for registrar's office"""
    return f"""
    <div style="font-family: Arial, sans-serif;">
        <h1 style="color: #1f4788;">📋 Registrar Data Quality Report</h1>
        <div style="background: #e8f0fe; padding: 15px; border-radius: 5px;">
            <h2>Student Enrollment Validation Summary</h2>
            <ul>
                <li><strong>Total Students Reviewed:</strong> {summary_data['total_students']}</li>
                <li><strong>Classification Issues:</strong> {summary_data['class_violations']}</li>
                <li><strong>Program Mismatches:</strong> {summary_data['program_violations']}</li>
            </ul>
        </div>
    </div>
    """

def create_admissions_email(summary_data):
    """Email template for admissions office"""  
    return f"""
    <div style="font-family: Arial, sans-serif;">
        <h1 style="color: #8b0000;">🎓 Admissions Data Quality Report</h1>
        <div style="background: #ffeaea; padding: 15px; border-radius: 5px;">
            <h2>Cross-System Validation Results</h2>
            <ul>
                <li><strong>Applications Reviewed:</strong> {summary_data['total_applications']}</li>
                <li><strong>Degree Code Mismatches:</strong> {summary_data['degree_mismatches']}</li>
                <li><strong>Status Inconsistencies:</strong> {summary_data['status_issues']}</li>
            </ul>
        </div>
    </div>
    """
```

### Role-Based Notifications

Send different emails to different stakeholders:

```python
@task()
def send_stakeholder_notifications(aggregated_result):
    """Send customized emails to different departments"""
    
    # Registrar notification
    if aggregated_result['class_code_violations'] > 0:
        registrar_content = create_registrar_email(aggregated_result)
        send_email(
            to=["registrar@institution.edu"],
            subject="Student Classification Issues Detected",
            html_content=registrar_content
        )
    
    # Admissions notification  
    if aggregated_result['degree_mismatches'] > 0:
        admissions_content = create_admissions_email(aggregated_result)
        send_email(
            to=["admissions@institution.edu"],
            subject="Cross-System Degree Code Issues",
            html_content=admissions_content
        )
    
    # IT Data Team notification (comprehensive report)
    it_content = create_technical_report(aggregated_result)
    send_email(
        to=["data-team@institution.edu"],
        subject="Complete Data Quality Report",
        html_content=it_content,
        attachments=aggregated_result['attachments']
    )
```

## Performance Optimization

### Large Dataset Handling

For institutions with large student populations:

```python
# Batch processing for large datasets
def execute_large_dataset_check(sql_file_path, conn_id, batch_size=10000):
    """Execute data quality check in batches for large datasets"""
    
    # Count total records first
    count_query = f"""
        SELECT COUNT(*) as total_records 
        FROM ({open(sql_file_path).read()}) as subquery
    """
    
    hook = MsSqlHook(mssql_conn_id=conn_id)
    total_records = hook.get_first(count_query)[0]
    
    # Process in batches
    all_results = []
    for offset in range(0, total_records, batch_size):
        batch_query = f"""
            {open(sql_file_path).read()}
            ORDER BY STUDENT_ID
            OFFSET {offset} ROWS
            FETCH NEXT {batch_size} ROWS ONLY
        """
        
        batch_results = pd.read_sql(batch_query, hook.get_sqlalchemy_engine())
        all_results.append(batch_results)
    
    # Combine all batches
    final_results = pd.concat(all_results, ignore_index=True)
    return final_results, len(final_results)
```

### Parallel Processing Optimization

```python
# Configure parallel task execution
default_args = {
    "owner": "Data Engineering Team",
    "pool": "data_quality_pool",        # Use dedicated resource pool
    "priority_weight": 10,              # Higher priority
    "max_active_tis_per_dag": 3,       # Limit concurrent tasks
}

@dag(
    default_args=default_args,
    max_active_runs=1,                  # Prevent overlapping runs
    catchup=False,
)
def data_quality_system():
    # DAG implementation
```

## Integration Patterns

### Webhook Notifications

Add webhook integration for modern alerting systems:

```python
import requests

def send_slack_notification(webhook_url, message):
    """Send notification to Slack via webhook"""
    payload = {
        "text": f"🔍 Data Quality Alert: {message}",
        "username": "Airflow Data Quality Bot"
    }
    requests.post(webhook_url, json=payload)

def send_teams_notification(webhook_url, summary):
    """Send notification to Microsoft Teams"""
    payload = {
        "@type": "MessageCard",
        "summary": "Data Quality Report",
        "themeColor": "FF6B6B" if summary['violations'] > 0 else "51CF66",
        "sections": [{
            "activityTitle": "Data Quality Check Complete",
            "facts": [
                {"name": "Total Violations", "value": str(summary['violations'])},
                {"name": "Checks Run", "value": str(summary['total_checks'])}
            ]
        }]
    }
    requests.post(webhook_url, json=payload)
```

### Data Warehouse Integration

Export results to your data warehouse:

```python
@task()
def export_to_warehouse(aggregated_result):
    """Export data quality results to institutional data warehouse"""
    
    # Create summary record
    summary_record = {
        'execution_date': pendulum.now('UTC'),
        'total_checks': len(aggregated_result['checks']),
        'total_violations': aggregated_result['total_violations'],
        'status': 'PASSED' if aggregated_result['all_passed'] else 'VIOLATIONS'
    }
    
    # Insert to warehouse
    warehouse_hook = MsSqlHook(mssql_conn_id="data_warehouse")
    warehouse_hook.insert_rows(
        table="DATA_QUALITY_EXECUTION_LOG",
        rows=[summary_record]
    )
```

## Testing and Validation

### Unit Testing Custom Checks

Create tests for your custom validation logic:

```python
# tests/test_custom_validations.py
import pytest
import pandas as pd
from airflow.utils.dq_utils import execute_data_quality_check

def test_enrollment_status_validation():
    """Test enrollment status validation logic"""
    
    # Mock data with known violations
    test_data = pd.DataFrame({
        'STUDENT_ID': [1, 2, 3],
        'ENROLLMENT_STATUS': ['ENROLLED', 'INVALID', 'REGISTERED']
    })
    
    # Expected: 1 violation (student 2 with INVALID status)
    results_df, violation_count = execute_data_quality_check(
        sql_file_path="enrollment_status_validation.sql",
        conn_id="test_database"
    )
    
    assert violation_count == 1
    assert results_df.iloc[0]['STUDENT_ID'] == 2
```

### End-to-End Testing

```python
@pytest.fixture
def sample_student_data():
    """Create sample data for testing"""
    return {
        'students': [
            {'id': 1, 'degree': 'BACHELOR_ARCH', 'class_code': 'UG_1A'},  # Valid
            {'id': 2, 'degree': 'BACHELOR_ARCH', 'class_code': 'G1'},     # Invalid 
        ]
    }

def test_dag_execution(sample_student_data):
    """Test complete DAG execution with sample data"""
    # DAG execution test implementation
```

This customization guide provides the foundation for adapting the framework to your institution's specific needs. Continue with the [ARCHITECTURE.md](ARCHITECTURE.md) for deeper technical understanding.
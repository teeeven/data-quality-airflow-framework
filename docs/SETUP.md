# Setup Guide

This guide walks you through setting up the Higher Education Data Quality Framework in your Apache Airflow environment.

## Prerequisites

### System Requirements
- **Apache Airflow**: 2.0 or higher
- **Python**: 3.8 or higher  
- **Database**: SQL Server (or compatible with minor modifications)
- **SMTP Server**: For email notifications

### Required Python Packages
- `apache-airflow[microsoft.mssql]`
- `pandas`
- `sqlalchemy`

## Installation Steps

### 1. Prepare Your Airflow Environment

```bash
# Navigate to your Airflow directory
cd $AIRFLOW_HOME

# Create necessary directories if they don't exist
mkdir -p dags/utils
mkdir -p sql
mkdir -p include/output/data_quality_system
```

### 2. Copy Framework Files

```bash
# Copy DAG file
cp data-quality-airflow-framework/airflow/dags/data_quality_system.py dags/

# Copy utility functions
cp data-quality-airflow-framework/airflow/utils/*.py dags/utils/

# Copy SQL validation queries
cp data-quality-airflow-framework/airflow/sql/*.sql sql/
```

### 3. Configure Database Connection

Create a new Airflow connection for your institutional database:

#### Using Airflow Web UI:
1. Navigate to **Admin → Connections**
2. Click **Add a new record**
3. Configure connection:
   - **Connection Id**: `institutional_database`
   - **Connection Type**: `Microsoft SQL Server`
   - **Host**: Your SQL Server hostname
   - **Schema**: Your database name
   - **Login**: Database username
   - **Password**: Database password
   - **Port**: 1433 (default)

#### Using Airflow CLI:
```bash
airflow connections add institutional_database \
    --conn-type mssql \
    --conn-host your-sql-server.institution.edu \
    --conn-schema your_database_name \
    --conn-login your_username \
    --conn-password your_password \
    --conn-port 1433
```

### 4. Configure Email Settings

Set up SMTP environment variables for email notifications:

```bash
# Add to your .bashrc or environment configuration
export AIRFLOW__SMTP__SMTP_HOST="smtp.your-institution.edu"
export AIRFLOW__SMTP__SMTP_PORT="587"
export AIRFLOW__SMTP__SMTP_USER="your-email@institution.edu"
export AIRFLOW__SMTP__SMTP_PASSWORD="your-app-password"
```

For production environments, use Airflow Variables or Secrets:

```bash
# Using Airflow Variables
airflow variables set SMTP_HOST "smtp.your-institution.edu"
airflow variables set SMTP_USER "your-email@institution.edu"
airflow variables set SMTP_PASSWORD "your-app-password"
```

### 5. Customize for Your Institution

#### Update Database Schema References

Edit the SQL files to match your database schema:

**File: `sql/class_code_validation.sql`**
```sql
-- Replace these table references:
STUDENT_TERMS        → YOUR_STUDENT_ENROLLMENT_TABLE
CLASS_DEFINITION     → YOUR_CLASS_CODES_TABLE  
ACTIVE_STUDENTS      → YOUR_ACTIVE_STUDENT_VIEW
```

**File: `sql/degree_consistency_check.sql`**
```sql
-- Replace these table references:
ADMISSIONS_RECORDS   → YOUR_ADMISSIONS_TABLE
REGISTRATION_HISTORY → YOUR_REGISTRATION_TABLE
STUDENT_NAMES        → YOUR_STUDENT_NAME_VIEW
```

#### Configure Validation Rules

Update degree program codes and class code mappings:

```sql
-- Example customization in class_code_validation.sql
WHEN st.DEGREE_CODE = 'YOUR_BACHELOR_PROGRAM' 
    AND st.CLASS_CODE NOT IN ('FRESHMAN','SOPHOMORE','JUNIOR','SENIOR') 
    THEN 1

WHEN st.DEGREE_CODE = 'YOUR_MASTER_PROGRAM'    
    AND st.CLASS_CODE NOT IN ('GRAD_1','GRAD_2','GRAD_3') 
    THEN 1
```

#### Update Email Recipients

Edit `dags/data_quality_system.py`:

```python
# Replace with your institutional email addresses
send_email(
    to=["data-team@your-institution.edu", "registrar@your-institution.edu"],
    subject=subject,
    html_content=html_content,
    attachments=attachments
)
```

### 6. Test the Installation

#### Validate DAG Syntax
```bash
# Test DAG for syntax errors
python dags/data_quality_system.py
```

#### Test Database Connection
```bash
# Test database connection
airflow connections test institutional_database
```

#### Run Individual Tasks
```bash
# Test class code validation
airflow tasks test institution_data_quality_system check_class_codes 2025-01-01

# Test degree consistency check  
airflow tasks test institution_data_quality_system check_degree_consistency 2025-01-01
```

## Configuration Options

### Output Directory

By default, CSV files are saved to `/usr/local/airflow/include/output/data_quality_system/`. 

To customize:
```python
# In dq_utils.py, modify the default path:
def save_results_to_csv(
    results_df: pd.DataFrame,
    check_name: str,
    output_dir: str = "/your/custom/output/path"  # Update this
)
```

### Email Themes

Customize email styling by modifying theme colors:

```python
# Green theme (success/enrollment)
status_color = "#2e7d32"
status_bg = "#e8f5e8"

# Orange theme (warnings/data quality)
status_color = "#f57c00" 
status_bg = "#fff3e0"

# Blue theme (financial/administrative)
status_color = "#1976d2"
status_bg = "#e3f2fd"
```

### Scheduling

The DAG is configured for manual execution by default:

```python
@dag(
    dag_id="institution_data_quality_system",
    schedule=None,  # Manual trigger only
    # ...
)
```

For automated scheduling:
```python
schedule="0 8 * * MON-FRI",  # Weekdays at 8 AM
# or
schedule="@daily",           # Daily execution
```

## Troubleshooting

### Common Issues

**1. Import Errors**
```
ModuleNotFoundError: No module named 'airflow.utils.dq_utils'
```
**Solution**: Ensure utils files are in the correct path: `$AIRFLOW_HOME/dags/utils/`

**2. Database Connection Failures**
```
Cannot connect to database: Login failed for user
```
**Solution**: Verify database credentials and network connectivity

**3. SQL File Not Found**
```
FileNotFoundError: SQL file not found: /usr/local/airflow/airflow/sql/...
```
**Solution**: Check SQL file paths and permissions

**4. Email Sending Failures**
```
SMTPAuthenticationError: Username and Password not accepted
```
**Solution**: Verify SMTP credentials and use app-specific passwords if required

### Logs and Monitoring

Monitor DAG execution through:
- **Airflow Web UI**: Task logs and execution history
- **Email Notifications**: Success/failure alerts
- **CSV Output Files**: Detailed violation data

### Performance Tuning

For large datasets:
- Increase task timeout: `execution_timeout=timedelta(minutes=30)`
- Add resource limits: Configure worker memory and CPU
- Optimize SQL queries: Add indexes on frequently queried columns

## Next Steps

After successful installation:
1. Review the [CUSTOMIZATION.md](CUSTOMIZATION.md) guide for advanced configuration
2. Read [ARCHITECTURE.md](ARCHITECTURE.md) to understand system design
3. Set up monitoring and alerting for production use
4. Consider adding additional validation checks for your specific needs
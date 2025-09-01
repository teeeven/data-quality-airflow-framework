# Architecture Documentation

This document explains the design principles, technical architecture, and implementation patterns of the Higher Education Data Quality Framework.

## Design Principles

### 1. Parallel Processing First
The framework is built around parallel execution to optimize performance:
- Multiple data quality checks run simultaneously
- Independent tasks can scale horizontally  
- Reduces total execution time for comprehensive validation

### 2. Separation of Concerns
Clear separation between different system responsibilities:
- **SQL files**: Business logic and validation rules
- **Python utilities**: Data processing and infrastructure
- **DAG definition**: Orchestration and workflow
- **Email templates**: Presentation and notifications

### 3. Configuration-Driven
Minimize hard-coded values to support multiple institutions:
- Database connections via Airflow configuration
- Validation rules in SQL files
- Email settings via environment variables
- Output paths configurable per environment

### 4. Production-Ready Error Handling
Comprehensive error handling at every level:
- SQL execution errors don't crash the pipeline
- Email failures don't prevent data processing  
- Retry logic for transient failures
- Detailed logging for troubleshooting

## System Architecture

### High-Level Component View

```
┌─────────────────────────────────────────────────────────────┐
│                    Airflow Scheduler                        │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                 Data Quality DAG                            │
│  ┌─────────────────┐    ┌─────────────────────────────────┐ │
│  │  Validation     │    │  Validation Task 2              │ │
│  │  Task 1         │◄──►│  (Degree Consistency)           │ │
│  │  (Class Codes)  │    │                                 │ │
│  └─────────────────┘    └─────────────────────────────────┘ │
│           │                           │                     │
│           ▼                           ▼                     │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              Aggregation Task                           │ │
│  └─────────────────────┬───────────────────────────────────┘ │
│                        ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              Email Notification                         │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                 External Systems                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ SQL Server  │  │ SMTP Server │  │ File System         │  │
│  │ Database    │  │             │  │ (CSV Outputs)       │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Task Flow Architecture

```
Start
  │
  ├─→ check_class_codes() ──────────┐
  │                                 │
  └─→ check_degree_consistency() ───┤
                                    │
                                    ▼
                            aggregate_results()
                                    │
                                    ▼
                            send_email_alert()
                                    │
                                    ▼
                                  End
```

**Key Design Features:**
- **Parallel Execution**: First two tasks run simultaneously
- **Dependency Management**: Aggregation waits for both validations
- **Sequential Notification**: Email sent after aggregation completes
- **Failure Isolation**: Individual task failures don't cascade

## Component Deep Dive

### 1. Data Quality Utilities (`dq_utils.py`)

**Purpose**: Reusable functions for executing SQL-based validation checks

**Key Functions:**
```python
execute_data_quality_check()  # SQL execution and result processing
save_results_to_csv()        # Violation data export  
validate_check_configuration() # Pre-execution validation
```

**Design Patterns:**
- **Single Responsibility**: Each function has one clear purpose
- **Error Propagation**: Exceptions bubble up with context
- **Resource Management**: Database connections properly closed
- **Logging Integration**: Structured logging at key points

### 2. Email Utilities (`email_utils.py`) 

**Purpose**: Professional email notifications with institutional styling

**Key Functions:**
```python
send_email()                          # Core SMTP functionality
create_institutional_html_template()  # Standardized HTML formatting
```

**Design Patterns:**
- **Template Method**: Consistent email structure across notifications
- **Configuration Injection**: SMTP settings via environment/parameters
- **Graceful Degradation**: Email failures don't crash data processing
- **Attachment Handling**: CSV files attached when violations detected

### 3. SQL Validation Queries

**Purpose**: Business logic implementation in SQL for performance

**Design Patterns:**
- **Self-Documenting**: Comprehensive header documentation
- **Parameterization Ready**: Structured for dynamic parameters
- **Performance Optimized**: CTEs and proper indexing guidance
- **Result Standardization**: Consistent output schema

**Example Structure:**
```sql
-- Metadata and documentation header
-- Business context and troubleshooting

WITH validation_logic AS (
    -- Core validation CTE
    SELECT ..., 
           CASE 
               WHEN business_rule_violation THEN 1
               ELSE 0
           END AS DATA_QUALITY_ALERT
    FROM source_tables
)
SELECT * FROM validation_logic WHERE DATA_QUALITY_ALERT = 1;
```

### 4. DAG Orchestration (`data_quality_system.py`)

**Purpose**: Workflow orchestration using Airflow TaskFlow API

**Key Components:**
- **Task Definition**: Individual validation functions
- **Dependency Management**: Task execution order
- **Error Handling**: Failure callbacks and retry logic
- **Configuration**: DAG-level settings and scheduling

**Design Patterns:**
- **Functional Programming**: Tasks as pure functions where possible
- **Immutable Data Flow**: Results passed between tasks unchanged
- **Explicit Dependencies**: Clear task relationships
- **Resource Isolation**: Each task manages its own connections

## Data Flow Architecture

### Input Data Sources
```
Institutional Database
├── Student Enrollment Tables
│   ├── Current term enrollment records
│   ├── Degree program assignments  
│   └── Academic classification codes
├── Admissions System Tables
│   ├── Application records
│   ├── Admission decisions
│   └── Program preferences
└── Registration System Tables  
    ├── Enrolled student records
    ├── Degree history tracking
    └── Academic status codes
```

### Processing Pipeline
```
Raw Data → SQL Validation → Pandas DataFrame → CSV Export
    ↓              ↓              ↓              ↓
Database      Business       Python         File
Tables        Rules        Processing      System
```

### Output Artifacts
```
Email Notifications
├── HTML formatted reports
├── Success/failure indicators
├── Violation summaries
└── CSV attachments (when violations found)

CSV Files  
├── Timestamped violation records
├── Complete violation details
├── Audit trail for analysis
└── Integration data for downstream systems
```

## Error Handling Strategy

### Error Categories and Responses

**1. Configuration Errors**
- **Source**: Missing connections, invalid file paths
- **Response**: Fail fast with clear error messages
- **Recovery**: Manual configuration fix required

**2. Database Errors**  
- **Source**: SQL syntax, connection timeouts, permission issues
- **Response**: Log detailed error, attempt retry
- **Recovery**: Automatic retry up to configured limit

**3. Processing Errors**
- **Source**: Data type mismatches, memory issues
- **Response**: Log error context, continue with other tasks  
- **Recovery**: Graceful degradation, partial results

**4. Notification Errors**
- **Source**: SMTP failures, attachment issues
- **Response**: Log error, continue pipeline execution
- **Recovery**: Email failure doesn't affect data processing

### Error Propagation Flow

```
Task Execution Error
        │
        ▼
Local Error Handling
        │
        ├─→ Retry if Transient
        │
        └─→ Log and Fail if Critical
                │
                ▼
        DAG Failure Callback
                │
                ▼
        Airflow Alert System
```

## Performance Considerations

### Parallel Processing Benefits

**Sequential Execution Time:**
```
Task 1: 3 minutes
Task 2: 4 minutes  
Aggregation: 30 seconds
Email: 10 seconds
Total: 7 minutes 40 seconds
```

**Parallel Execution Time:**
```
Task 1 & 2: max(3, 4) = 4 minutes (parallel)
Aggregation: 30 seconds
Email: 10 seconds  
Total: 5 minutes 10 seconds (32% faster)
```

### Database Optimization

**Query Performance:**
- Use appropriate indexes on join columns
- Leverage CTEs for complex logic
- Consider query execution plans
- Monitor for table scans

**Connection Management:**
- Connection pooling via Airflow hooks
- Proper connection cleanup
- Timeout configuration
- Retry logic for transient failures

### Memory Management

**Large Dataset Handling:**
```python
# Chunked processing for large result sets
def process_large_dataset(query, chunk_size=10000):
    for chunk in pd.read_sql(query, connection, chunksize=chunk_size):
        process_chunk(chunk)
```

## Security Architecture

### Data Protection
- **Connection Security**: Encrypted database connections
- **Credential Management**: Environment variables/Airflow Variables
- **Access Control**: Database permissions per connection
- **Audit Logging**: All data access logged

### Email Security  
- **SMTP Authentication**: Secure credential handling
- **TLS Encryption**: Encrypted email transmission
- **Attachment Scanning**: Consider security scanning for attachments
- **Recipient Validation**: Authorized email addresses only

### File System Security
- **Path Validation**: Prevent directory traversal
- **Permission Checks**: Appropriate file permissions
- **Cleanup Policies**: Automatic file removal after retention period
- **Access Auditing**: File access logging

## Extensibility Patterns

### Adding New Validation Checks

**1. Create SQL Validation File**
```sql
-- Follow established documentation pattern
-- Implement business logic in SQL
-- Return standardized result format
```

**2. Add Task to DAG**
```python
@task()
def check_new_validation():
    # Follow established task pattern
    # Return standardized result dictionary
```

**3. Update Aggregation Logic**
```python
# Include new check in aggregation
# Update email template if needed
# Add to dependency chain
```

### Integration Points

**Webhook Integration:**
```python
# Add webhook notification to email task
def send_webhook_notification(webhook_url, payload):
    requests.post(webhook_url, json=payload)
```

**Data Warehouse Export:**
```python
# Add warehouse export task
@task()
def export_to_warehouse(results):
    warehouse_hook.insert_rows(table, results)
```

**Monitoring Integration:**
```python  
# Add metrics collection
def collect_metrics(results):
    metrics_client.increment('data_quality.violations', results['total_violations'])
```

## Deployment Architecture

### Environment Patterns

**Development Environment:**
```
- Manual DAG triggers
- Test database connections
- Console logging
- Local file output
```

**Staging Environment:**
```  
- Scheduled execution (limited)
- Production-like database
- Email to test accounts
- Network file storage
```

**Production Environment:**
```
- Full scheduling enabled
- Production database connections  
- Stakeholder email notifications
- Monitored file storage
- Alerting integration
```

### Scalability Considerations

**Horizontal Scaling:**
- Multiple Airflow workers
- Database connection pooling
- Load balancing for email delivery
- Distributed file storage

**Vertical Scaling:**
- Worker memory allocation
- Database query optimization  
- Email batch processing
- File I/O optimization

This architecture supports institutions from small colleges to large universities, with clear patterns for scaling and customization based on specific needs.
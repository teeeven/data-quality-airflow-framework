import pendulum
import logging
from airflow.decorators import dag, task
from airflow.utils.dq_utils import (
    execute_data_quality_check,
    save_results_to_csv
)
from airflow.utils.email_utils import send_email

# Configure module-level logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def on_failure_callback(context):
    """Called on task failure."""
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    exception = context.get('exception')
    logger.error("DAG '%s', task '%s' failed: %s", dag_id, task_id, exception)

# Default arguments
default_args = {
    "owner": "Data Team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": on_failure_callback,
}

@dag(
    dag_id="data_quality_system",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 8, 1),
    schedule=None,
    catchup=False,
    tags=["data-quality", "monitoring", "institutional"],
    doc_md="Data quality monitoring for multiple checks: class codes and degree program validation",
)
def data_quality_system():
    """
    Higher Education Data Quality Monitoring System
    
    This DAG implements a parallel data quality monitoring framework designed for
    institutional student information systems. It performs two key validation checks:
    
    1. Academic Class Code Validation - Ensures students are enrolled in class codes
       that align with their degree program requirements
    2. Degree Program Consistency - Validates degree codes match between admissions
       and registration systems
       
    The system runs checks in parallel, aggregates results, and sends professional
    email notifications with conditional formatting and CSV attachments for violations.
    """
    
    @task()
    def check_class_codes():
        """Execute the class codes data quality check."""
        logger.info("Starting class codes validation check")
        
        # Execute the check
        results_df, violation_count = execute_data_quality_check(
            sql_file_path="class_code_validation.sql",
            conn_id="institutional_database",
            check_name="inconsistent_class_codes"
        )
        
        # Save CSV if violations found
        csv_filepath = None
        if violation_count > 0:
            csv_filepath = save_results_to_csv(
                results_df, 
                "inconsistent_class_codes", 
                "/usr/local/airflow/include/output/data_quality_system"
            )
        
        logger.info(f"Class codes check completed: {violation_count} violations found")
        
        # Return detailed data for aggregation
        return {
            "check_name": "Class Code Validation",
            "check_description": "Students enrolled in class codes inconsistent with degree program",
            "violation_count": violation_count,
            "csv_filepath": csv_filepath,
            "has_violations": violation_count > 0
        }
    
    @task()
    def check_degree_consistency():
        """Execute the degree program consistency data quality check."""
        logger.info("Starting degree program consistency check")
        
        # Execute the check
        results_df, violation_count = execute_data_quality_check(
            sql_file_path="degree_consistency_check.sql",
            conn_id="institutional_database",
            check_name="degree_mismatch"
        )
        
        # Save CSV if violations found
        csv_filepath = None
        if violation_count > 0:
            csv_filepath = save_results_to_csv(
                results_df, 
                "degree_mismatch", 
                "/usr/local/airflow/include/output/data_quality_system"
            )
        
        logger.info(f"Degree consistency check completed: {violation_count} violations found")
        
        # Return detailed data for aggregation
        return {
            "check_name": "Degree Program Consistency", 
            "check_description": "Admissions vs Registration degree program mismatches",
            "violation_count": violation_count,
            "csv_filepath": csv_filepath,
            "has_violations": violation_count > 0
        }
    
    @task()
    def aggregate_results(class_codes_result, degree_consistency_result):
        """Aggregate results from all data quality checks."""
        logger.info("Aggregating results from all data quality checks")
        
        checks = [class_codes_result, degree_consistency_result]
        total_violations = sum(check["violation_count"] for check in checks)
        total_checks = len(checks)
        checks_with_violations = sum(1 for check in checks if check["has_violations"])
        
        # Prepare attachments list
        attachments = []
        for check in checks:
            if check["csv_filepath"]:
                attachments.append(check["csv_filepath"])
        
        aggregated_result = {
            "checks": checks,
            "total_violations": total_violations,
            "total_checks": total_checks,
            "checks_with_violations": checks_with_violations,
            "attachments": attachments,
            "all_passed": total_violations == 0
        }
        
        logger.info(f"Aggregation complete - {total_violations} total violations across {total_checks} checks")
        return aggregated_result

    @task()
    def send_email_alert(aggregated_result):
        """Send email alert with consolidated results from all checks."""
        logger.info(f"Starting email notification with aggregated results")
        
        total_violations = aggregated_result["total_violations"]
        checks = aggregated_result["checks"]
        attachments = aggregated_result["attachments"]
        
        # Create email subject  
        if total_violations == 0:
            subject = "Institutional Data Quality Report: All Checks Passed ✅"
        else:
            subject = f"Institutional Data Quality Alert: {total_violations} total violations found ⚠️"
        
        # Professional HTML email with conditional styling
        if total_violations == 0:
            status_color = "#2e7d32"
            status_bg = "#e8f5e8"
            status_icon = "✅"
            status_text = "All Checks Passed"
        else:
            status_color = "#f57c00"
            status_bg = "#fff3e0"
            status_icon = "⚠️"
            status_text = f"{total_violations} Total Violations Found"
        
        # Build individual check sections
        check_sections = ""
        for check in checks:
            check_color = "#2e7d32" if check["violation_count"] == 0 else "#f57c00"
            check_bg = "#e8f5e8" if check["violation_count"] == 0 else "#fff3e0"
            check_icon = "✅" if check["violation_count"] == 0 else "⚠️"
            
            check_sections += f"""
            <div style="background:{check_bg};padding:12px;border-radius:5px;margin:10px 0;border-left:4px solid {check_color};">
                <h3 style="color:{check_color};margin:0 0 8px 0;">{check_icon} {check["check_name"]}</h3>
                <ul style="font-size:14px; margin:0; padding-left:1.2em;">
                    <li><strong>Description:</strong> {check["check_description"]}</li>
                    <li><strong>Violations:</strong> {check["violation_count"]:,}</li>
                    <li><strong>Results File:</strong> {check["csv_filepath"].split("/")[-1] if check["csv_filepath"] else "No violations to export"}</li>
                </ul>
            </div>
            """
        
        html_content = f"""
        <div style="font-family: Arial, sans-serif; max-width: 800px;">
            <h1 style="color: {status_color}; border-bottom: 3px solid {status_color};">
                🔍 Institutional Data Quality Report
            </h1>
            
            <div style="background:{status_bg};padding:15px;border-radius:5px;margin:15px 0;">
                <h2 style="color:{status_color};margin:0 0 10px 0;">{status_icon} {status_text}</h2>
                <ul style="font-size:16px; margin:0; padding-left:1.2em;">
                    <li><strong>Total Checks:</strong> {len(checks)}</li>
                    <li><strong>Timestamp:</strong> {pendulum.now().format('YYYY-MM-DD HH:mm:ss')} UTC</li>
                    <li><strong>Total Violations:</strong> {total_violations:,}</li>
                    <li><strong>Checks with Issues:</strong> {aggregated_result["checks_with_violations"]} of {len(checks)}</li>
                </ul>
            </div>
            
            <h3 style="color: #333; margin-top: 20px;">📋 Individual Check Results</h3>
            {check_sections}
            
            {'<div style="background:#e8f5e8;padding:15px;border-radius:5px;margin:15px 0;"><p style="margin:0;font-size:16px;color:#2e7d32;"><strong>🎉 Excellent!</strong> All data quality checks passed with no violations detected.</p></div>' if total_violations == 0 else f'<div style="background:#ffebee;padding:15px;border-radius:5px;margin:15px 0;"><p style="margin:0;font-size:16px;color:#d32f2f;"><strong>⚠️ Action Required:</strong> {total_violations} total violations detected across {aggregated_result["checks_with_violations"]} checks. Please review the attached CSV files for detailed information.</p></div>'}
            
            <div style="margin-top:20px;padding:10px;background:#f5f5f5;border-radius:5px;">
                <p style="margin:0;font-size:14px;color:#666;">
                    🤖 <em>Generated by Institutional Data Quality Monitoring System</em><br>
                    <small>Open Source Data Quality Framework for Higher Education</small>
                </p>
            </div>
        </div>
        """
        
        # Send email
        try:
            send_email(
                to=["data-team@institution.edu"],
                subject=subject,
                html_content=html_content,
                attachments=attachments
            )
            logger.info("Email notification sent successfully")
            return "Email sent successfully"
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            raise
    
    # Define parallel task execution with aggregation
    class_codes_result = check_class_codes()
    degree_consistency_result = check_degree_consistency()
    
    # Aggregate results after both checks complete
    aggregated_result = aggregate_results(class_codes_result, degree_consistency_result)
    
    # Send consolidated email notification
    email_result = send_email_alert(aggregated_result)
    
    # Set task dependencies - parallel execution followed by aggregation and notification
    [class_codes_result, degree_consistency_result] >> aggregated_result >> email_result

# Instantiate the DAG
data_quality_system()
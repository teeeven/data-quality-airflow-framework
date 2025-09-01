# custom_validation_example.py
# Example of extending the Data Quality Framework with custom validation checks

"""
This file demonstrates how to extend the Higher Education Data Quality Framework
with custom validation checks specific to your institution's needs.

Examples included:
1. GPA Requirements Validation
2. Credit Hour Validation  
3. Financial Hold Validation
4. Prerequisites Validation
5. Custom Email Templates

Usage:
1. Copy this file to your dags/ directory
2. Modify the validation logic for your schema
3. Update the main DAG to include your custom checks
4. Test with your institutional data
"""

import pendulum
import logging
from airflow.decorators import dag, task
from airflow.utils.dq_utils import execute_data_quality_check, save_results_to_csv
from airflow.utils.email_utils import send_email, create_institutional_html_template

logger = logging.getLogger(__name__)


# =============================================================================
# CUSTOM VALIDATION TASKS
# =============================================================================

@task()
def check_gpa_requirements():
    """
    Validate that students meet GPA requirements for their academic programs.
    
    Business Rules:
    - Undergraduate programs require minimum 2.0 GPA
    - Graduate programs require minimum 3.0 GPA  
    - Students below minimums should be flagged for academic review
    """
    logger.info("Starting GPA requirements validation check")
    
    results_df, violation_count = execute_data_quality_check(
        sql_file_path="gpa_requirements_validation.sql",
        conn_id="institutional_database",
        check_name="gpa_requirements"
    )
    
    # Save detailed results if violations found
    csv_filepath = None
    if violation_count > 0:
        csv_filepath = save_results_to_csv(
            results_df,
            "gpa_requirements",
            "/usr/local/airflow/include/output/data_quality_system"
        )
    
    logger.info(f"GPA requirements check completed: {violation_count} students below minimum GPA")
    
    return {
        "check_name": "GPA Requirements",
        "check_description": "Students below minimum GPA requirements for their programs",
        "violation_count": violation_count,
        "csv_filepath": csv_filepath,
        "has_violations": violation_count > 0,
        "business_impact": "high" if violation_count > 50 else "medium"
    }


@task()
def check_credit_hour_validation():
    """
    Validate that students are enrolled in appropriate credit hours for their status.
    
    Business Rules:
    - Full-time undergraduate: 12+ credit hours
    - Full-time graduate: 9+ credit hours
    - Part-time status should align with credit hour enrollment
    """
    logger.info("Starting credit hour validation check")
    
    results_df, violation_count = execute_data_quality_check(
        sql_file_path="credit_hour_validation.sql",
        conn_id="institutional_database", 
        check_name="credit_hours"
    )
    
    csv_filepath = None
    if violation_count > 0:
        csv_filepath = save_results_to_csv(
            results_df,
            "credit_hour_mismatches",
            "/usr/local/airflow/include/output/data_quality_system"
        )
    
    logger.info(f"Credit hour validation completed: {violation_count} status/credit hour mismatches")
    
    return {
        "check_name": "Credit Hour Validation",
        "check_description": "Students with enrollment status not matching credit hour load",
        "violation_count": violation_count,
        "csv_filepath": csv_filepath,
        "has_violations": violation_count > 0,
        "business_impact": "medium"
    }


@task()
def check_financial_holds():
    """
    Validate that students with financial holds aren't enrolled in classes.
    
    Business Rules:
    - Students with active financial holds should not be enrolled
    - Registration holds should prevent new enrollments
    - Payment plan students may have conditional enrollment
    """
    logger.info("Starting financial holds validation check")
    
    results_df, violation_count = execute_data_quality_check(
        sql_file_path="financial_holds_validation.sql", 
        conn_id="institutional_database",
        check_name="financial_holds"
    )
    
    csv_filepath = None
    if violation_count > 0:
        csv_filepath = save_results_to_csv(
            results_df,
            "financial_hold_violations",
            "/usr/local/airflow/include/output/data_quality_system"
        )
    
    logger.info(f"Financial holds check completed: {violation_count} students enrolled with active holds")
    
    return {
        "check_name": "Financial Holds Validation",
        "check_description": "Students enrolled despite having active financial holds",
        "violation_count": violation_count,
        "csv_filepath": csv_filepath,
        "has_violations": violation_count > 0,
        "business_impact": "high",  # Financial compliance is critical
        "notify_bursar": True  # Flag for special notification
    }


@task()  
def check_prerequisite_validation():
    """
    Validate that students enrolled in courses have completed prerequisites.
    
    Business Rules:
    - Students must complete prerequisite courses before advanced courses
    - Concurrent prerequisites allowed for specified course combinations
    - Override permissions should be documented and tracked
    """
    logger.info("Starting prerequisite validation check")
    
    results_df, violation_count = execute_data_quality_check(
        sql_file_path="prerequisite_validation.sql",
        conn_id="institutional_database",
        check_name="prerequisites"
    )
    
    csv_filepath = None
    if violation_count > 0:
        csv_filepath = save_results_to_csv(
            results_df,
            "prerequisite_violations",
            "/usr/local/airflow/include/output/data_quality_system"
        )
    
    logger.info(f"Prerequisite validation completed: {violation_count} enrollment without prerequisites")
    
    return {
        "check_name": "Prerequisite Validation", 
        "check_description": "Students enrolled in courses without completing prerequisites",
        "violation_count": violation_count,
        "csv_filepath": csv_filepath,
        "has_violations": violation_count > 0,
        "business_impact": "medium",
        "notify_academic_advisors": True
    }


# =============================================================================
# ADVANCED AGGREGATION WITH CUSTOM BUSINESS LOGIC
# =============================================================================

@task()
def aggregate_custom_results(gpa_result, credit_hour_result, financial_holds_result, prerequisite_result):
    """
    Advanced aggregation with business logic and prioritization.
    
    Implements:
    - Violation severity classification
    - Business impact assessment  
    - Stakeholder notification routing
    - Trend analysis preparation
    """
    logger.info("Aggregating custom validation results with business logic")
    
    all_checks = [gpa_result, credit_hour_result, financial_holds_result, prerequisite_result]
    
    # Calculate summary metrics
    total_violations = sum(check["violation_count"] for check in all_checks)
    total_checks = len(all_checks)
    checks_with_violations = sum(1 for check in all_checks if check["has_violations"])
    
    # Business impact classification
    high_impact_violations = sum(
        check["violation_count"] for check in all_checks 
        if check.get("business_impact") == "high"
    )
    
    # Collect all attachments
    attachments = []
    high_priority_attachments = []
    
    for check in all_checks:
        if check["csv_filepath"]:
            attachments.append(check["csv_filepath"])
            if check.get("business_impact") == "high":
                high_priority_attachments.append(check["csv_filepath"])
    
    # Determine notification strategy
    notification_level = "critical" if high_impact_violations > 0 else "standard"
    
    # Create comprehensive result object
    aggregated_result = {
        "checks": all_checks,
        "summary": {
            "total_violations": total_violations,
            "total_checks": total_checks,
            "checks_with_violations": checks_with_violations,
            "high_impact_violations": high_impact_violations,
            "all_passed": total_violations == 0
        },
        "business_metrics": {
            "financial_compliance_issues": financial_holds_result["violation_count"],
            "academic_standing_issues": gpa_result["violation_count"],
            "enrollment_accuracy_issues": credit_hour_result["violation_count"],
            "academic_progression_issues": prerequisite_result["violation_count"]
        },
        "attachments": {
            "all": attachments,
            "high_priority": high_priority_attachments
        },
        "notification_routing": {
            "level": notification_level,
            "notify_bursar": any(check.get("notify_bursar", False) for check in all_checks),
            "notify_academic_advisors": any(check.get("notify_academic_advisors", False) for check in all_checks),
            "executive_summary_required": high_impact_violations > 25
        },
        "execution_metadata": {
            "execution_timestamp": pendulum.now('UTC').isoformat(),
            "framework_version": "1.0.0"
        }
    }
    
    logger.info(f"Advanced aggregation complete - {total_violations} total violations, notification level: {notification_level}")
    
    return aggregated_result


# =============================================================================
# CUSTOM EMAIL NOTIFICATION STRATEGIES  
# =============================================================================

@task()
def send_stakeholder_notifications(aggregated_result):
    """
    Send customized notifications to different stakeholder groups based on violation types and severity.
    """
    logger.info("Sending targeted stakeholder notifications")
    
    summary = aggregated_result["summary"]
    business_metrics = aggregated_result["business_metrics"] 
    routing = aggregated_result["notification_routing"]
    
    notifications_sent = []
    
    # 1. Data Team - Complete Technical Report
    data_team_content = create_technical_report(aggregated_result)
    send_email(
        to=["data-team@institution.edu"],
        subject=f"Data Quality Technical Report - {summary['total_violations']} Total Issues",
        html_content=data_team_content,
        attachments=aggregated_result["attachments"]["all"]
    )
    notifications_sent.append("data_team")
    
    # 2. Bursar Office - Financial Compliance Issues
    if routing["notify_bursar"] and business_metrics["financial_compliance_issues"] > 0:
        bursar_content = create_financial_compliance_report(aggregated_result)
        send_email(
            to=["bursar@institution.edu", "student-accounts@institution.edu"],
            subject=f"Financial Holds Compliance Alert - {business_metrics['financial_compliance_issues']} Cases",
            html_content=bursar_content,
            attachments=aggregated_result["attachments"]["high_priority"]
        )
        notifications_sent.append("bursar")
    
    # 3. Academic Advisors - Academic Standing Issues  
    if routing["notify_academic_advisors"]:
        advisor_content = create_academic_advisory_report(aggregated_result)
        send_email(
            to=["academic-advisors@institution.edu", "student-success@institution.edu"],
            subject=f"Student Academic Standing Review Required",
            html_content=advisor_content
        )
        notifications_sent.append("academic_advisors")
    
    # 4. Registrar - Enrollment and Classification Issues
    if business_metrics["enrollment_accuracy_issues"] > 0:
        registrar_content = create_registrar_report(aggregated_result)
        send_email(
            to=["registrar@institution.edu", "enrollment-services@institution.edu"],
            subject=f"Enrollment Status Validation Results",
            html_content=registrar_content
        )
        notifications_sent.append("registrar")
    
    # 5. Executive Summary - High Impact Issues
    if routing["executive_summary_required"]:
        executive_content = create_executive_summary(aggregated_result)
        send_email(
            to=["provost@institution.edu", "vp-student-affairs@institution.edu"],
            subject=f"EXECUTIVE ALERT: {summary['high_impact_violations']} High-Impact Student Data Issues",
            html_content=executive_content
        )
        notifications_sent.append("executive")
    
    logger.info(f"Stakeholder notifications sent to: {', '.join(notifications_sent)}")
    
    return {
        "notifications_sent": notifications_sent,
        "total_notifications": len(notifications_sent),
        "notification_level": routing["level"],
        "timestamp": pendulum.now('UTC').isoformat()
    }


# =============================================================================
# CUSTOM EMAIL TEMPLATE FUNCTIONS
# =============================================================================

def create_technical_report(aggregated_result):
    """Create detailed technical report for data team."""
    summary = aggregated_result["summary"]
    checks = aggregated_result["checks"]
    
    # Build detailed check sections
    check_details = ""
    for check in checks:
        status_color = "#2e7d32" if check["violation_count"] == 0 else "#f57c00"
        status_bg = "#e8f5e8" if check["violation_count"] == 0 else "#fff3e0"
        status_icon = "✅" if check["violation_count"] == 0 else "⚠️"
        
        check_details += f"""
        <div style="background:{status_bg};padding:12px;border-radius:5px;margin:10px 0;border-left:4px solid {status_color};">
            <h3 style="color:{status_color};margin:0 0 8px 0;">{status_icon} {check["check_name"]}</h3>
            <ul style="font-size:14px; margin:0; padding-left:1.2em;">
                <li><strong>Description:</strong> {check["check_description"]}</li>
                <li><strong>Violations:</strong> {check["violation_count"]:,}</li>
                <li><strong>Business Impact:</strong> {check.get("business_impact", "medium").upper()}</li>
                <li><strong>Data File:</strong> {check["csv_filepath"].split("/")[-1] if check["csv_filepath"] else "No violations"}</li>
            </ul>
        </div>
        """
    
    return create_institutional_html_template(
        title="🔧 Technical Data Quality Report",
        summary_data={
            "Total Checks": summary["total_checks"],
            "Total Violations": f"{summary['total_violations']:,}",
            "Checks with Issues": f"{summary['checks_with_violations']} of {summary['total_checks']}",
            "High Impact Violations": f"{summary['high_impact_violations']:,}",
            "Execution Time": pendulum.now().format('YYYY-MM-DD HH:mm:ss')
        },
        sections=[{
            "title": "📋 Detailed Check Results", 
            "content": check_details
        }],
        theme_color="#1976d2"
    )


def create_financial_compliance_report(aggregated_result):
    """Create focused report for bursar office."""
    financial_violations = aggregated_result["business_metrics"]["financial_compliance_issues"]
    
    return create_institutional_html_template(
        title="💰 Financial Compliance Alert",
        summary_data={
            "Students with Active Holds": f"{financial_violations:,}",
            "Compliance Status": "VIOLATIONS DETECTED" if financial_violations > 0 else "COMPLIANT",
            "Action Required": "IMMEDIATE" if financial_violations > 10 else "STANDARD"
        },
        sections=[{
            "title": "⚠️ Immediate Action Required",
            "content": f"""
            <p style="font-size:16px;color:#d32f2f;">
                <strong>{financial_violations} students are currently enrolled despite having active financial holds.</strong>
            </p>
            <p>Please review the attached data file and take appropriate action to either:</p>
            <ul>
                <li>Remove financial holds for students with resolved accounts</li>
                <li>Disenroll students with unresolved financial obligations</li>
                <li>Document approved exceptions with justification</li>
            </ul>
            """
        }],
        theme_color="#d32f2f",
        background_color="#ffebee"
    )


def create_academic_advisory_report(aggregated_result):
    """Create report for academic advisors."""
    gpa_issues = aggregated_result["business_metrics"]["academic_standing_issues"]
    prerequisite_issues = aggregated_result["business_metrics"]["academic_progression_issues"]
    
    return create_institutional_html_template(
        title="🎓 Academic Advisory Alert",
        summary_data={
            "Students Below GPA Requirements": f"{gpa_issues:,}",
            "Prerequisite Violations": f"{prerequisite_issues:,}",
            "Total Students Needing Review": f"{gpa_issues + prerequisite_issues:,}"
        },
        sections=[{
            "title": "📚 Academic Intervention Recommended",
            "content": f"""
            <p style="font-size:16px;">
                The following students may benefit from academic advising intervention:
            </p>
            <ul>
                <li><strong>GPA Below Standards:</strong> {gpa_issues} students</li>
                <li><strong>Course Prerequisites Missing:</strong> {prerequisite_issues} students</li>
            </ul>
            <p>Consider scheduling academic success meetings to review degree progress and support options.</p>
            """
        }],
        theme_color="#9c27b0"
    )


def create_executive_summary(aggregated_result):
    """Create high-level summary for executive leadership."""
    summary = aggregated_result["summary"]
    business_metrics = aggregated_result["business_metrics"]
    
    return create_institutional_html_template(
        title="📊 Executive Data Quality Summary",
        summary_data={
            "Total Data Quality Issues": f"{summary['total_violations']:,}",
            "High Priority Issues": f"{summary['high_impact_violations']:,}",
            "Financial Compliance": f"{business_metrics['financial_compliance_issues']:,} cases",
            "Academic Standing": f"{business_metrics['academic_standing_issues']:,} students"
        },
        sections=[{
            "title": "🎯 Strategic Recommendations",
            "content": """
            <p style="font-size:16px;">
                Based on this data quality analysis, we recommend:
            </p>
            <ul>
                <li><strong>Immediate:</strong> Review financial hold enforcement procedures</li>
                <li><strong>Short-term:</strong> Enhance academic advisor training on early intervention</li>
                <li><strong>Long-term:</strong> Implement automated validation in student information system</li>
            </ul>
            """
        }],
        theme_color="#1565c0"
    )


# =============================================================================
# EXAMPLE DAG USING CUSTOM VALIDATIONS
# =============================================================================

@dag(
    dag_id="advanced_data_quality_system",
    description="Advanced data quality monitoring with custom business rules and stakeholder notifications",
    start_date=pendulum.datetime(2025, 8, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "advanced", "multi-stakeholder"]
)
def advanced_data_quality_system():
    """
    Advanced Data Quality DAG demonstrating custom validation checks,
    business logic integration, and stakeholder-specific notifications.
    """
    
    # Execute all validation checks in parallel
    gpa_result = check_gpa_requirements()
    credit_hour_result = check_credit_hour_validation() 
    financial_holds_result = check_financial_holds()
    prerequisite_result = check_prerequisite_validation()
    
    # Advanced aggregation with business logic
    aggregated_result = aggregate_custom_results(
        gpa_result, 
        credit_hour_result,
        financial_holds_result,
        prerequisite_result
    )
    
    # Stakeholder-specific notifications
    notification_result = send_stakeholder_notifications(aggregated_result)
    
    # Set dependencies
    [gpa_result, credit_hour_result, financial_holds_result, prerequisite_result] >> aggregated_result >> notification_result

# Instantiate the advanced DAG
advanced_data_quality_system()
# email_themes.py
# Professional email themes and templates for higher education institutions

"""
Email Theme Library for Higher Education Data Quality Framework

This module provides pre-configured email themes and templates designed for
different types of institutional communications. Themes are designed to be
professional, accessible, and aligned with common institutional branding patterns.

Available Themes:
- University Blue (traditional academic)
- Community Green (growth and success)  
- Professional Gray (neutral and clean)
- Alert Orange (warnings and urgent issues)
- Success Green (positive outcomes)
- Graduate Purple (advanced programs)
- Financial Gold (business and financial)

Usage:
    from examples.email_themes import INSTITUTION_THEMES, create_themed_email
    
    content = create_themed_email(
        theme="university_blue",
        title="Data Quality Report", 
        data={"violations": 5},
        content_sections=[...]
    )
"""

import pendulum
from typing import Dict, List, Any


# =============================================================================
# INSTITUTIONAL COLOR THEMES
# =============================================================================

INSTITUTION_THEMES = {
    
    # Traditional University Theme - Professional Blue
    "university_blue": {
        "name": "University Blue",
        "primary_color": "#1f4788",
        "background_color": "#e8f0fe", 
        "success_color": "#2e7d32",
        "warning_color": "#f57c00",
        "error_color": "#d32f2f",
        "accent_color": "#1976d2",
        "text_color": "#333333",
        "context": "Traditional 4-year universities, formal communications",
        "emotional_tone": "Professional, trustworthy, authoritative"
    },
    
    # Community College Theme - Approachable Green
    "community_green": {
        "name": "Community Green",
        "primary_color": "#2e7d32",
        "background_color": "#e8f5e8",
        "success_color": "#1b5e20", 
        "warning_color": "#ef6c00",
        "error_color": "#c62828",
        "accent_color": "#4caf50",
        "text_color": "#2e2e2e",
        "context": "Community colleges, continuing education, accessible programs",
        "emotional_tone": "Welcoming, growth-oriented, supportive"
    },
    
    # Professional Neutral Theme - Clean Gray
    "professional_gray": {
        "name": "Professional Gray", 
        "primary_color": "#424242",
        "background_color": "#f5f5f5",
        "success_color": "#388e3c",
        "warning_color": "#f57c00",
        "error_color": "#d32f2f", 
        "accent_color": "#757575",
        "text_color": "#212121",
        "context": "Technical communications, system notifications, IT departments",
        "emotional_tone": "Clean, neutral, technical"
    },
    
    # Alert Orange Theme - Urgent Communications
    "alert_orange": {
        "name": "Alert Orange",
        "primary_color": "#f57c00",
        "background_color": "#fff3e0",
        "success_color": "#388e3c", 
        "warning_color": "#ff9800",
        "error_color": "#d84315",
        "accent_color": "#fb8c00",
        "text_color": "#bf360c",
        "context": "Data quality alerts, urgent notifications, compliance issues", 
        "emotional_tone": "Attention-getting, urgent, actionable"
    },
    
    # Success Green Theme - Positive Outcomes
    "success_green": {
        "name": "Success Green",
        "primary_color": "#2e7d32", 
        "background_color": "#e8f5e8",
        "success_color": "#1b5e20",
        "warning_color": "#f9a825",
        "error_color": "#c62828",
        "accent_color": "#43a047",
        "text_color": "#1b5e20",
        "context": "Successful completion reports, positive metrics, achievements",
        "emotional_tone": "Positive, encouraging, successful"
    },
    
    # Graduate Purple Theme - Advanced Programs
    "graduate_purple": {
        "name": "Graduate Purple",
        "primary_color": "#6a1b9a",
        "background_color": "#f3e5f5", 
        "success_color": "#388e3c",
        "warning_color": "#f57c00",
        "error_color": "#c2185b",
        "accent_color": "#8e24aa",
        "text_color": "#4a148c",
        "context": "Graduate programs, research institutions, advanced degrees",
        "emotional_tone": "Sophisticated, academic, distinguished"
    },
    
    # Financial Gold Theme - Business Communications  
    "financial_gold": {
        "name": "Financial Gold",
        "primary_color": "#f57f17",
        "background_color": "#fffde7",
        "success_color": "#388e3c",
        "warning_color": "#ff8f00", 
        "error_color": "#d32f2f",
        "accent_color": "#fbc02d",
        "text_color": "#f57f17",
        "context": "Financial communications, business operations, budget reports",
        "emotional_tone": "Important, valuable, business-focused"
    }
}


# =============================================================================
# THEMED EMAIL TEMPLATE FUNCTIONS
# =============================================================================

def create_themed_email(
    theme: str,
    title: str,
    summary_data: Dict[str, Any],
    content_sections: List[Dict[str, str]],
    footer_text: str = None,
    include_branding: bool = True
) -> str:
    """
    Create a professionally themed email using institutional color schemes.
    
    Args:
        theme (str): Theme name from INSTITUTION_THEMES
        title (str): Main email title/subject
        summary_data (dict): Key-value pairs for summary section
        content_sections (list): List of dicts with 'title' and 'content' keys
        footer_text (str): Optional custom footer text
        include_branding (bool): Include institutional branding elements
        
    Returns:
        str: Complete HTML email content
        
    Example:
        >>> email_html = create_themed_email(
        ...     theme="university_blue",
        ...     title="📊 Weekly Data Quality Report",
        ...     summary_data={"Total Checks": 5, "Violations": 2},
        ...     content_sections=[{
        ...         "title": "📋 Details",
        ...         "content": "<p>Check results...</p>"
        ...     }]
        ... )
    """
    
    if theme not in INSTITUTION_THEMES:
        raise ValueError(f"Theme '{theme}' not found. Available themes: {list(INSTITUTION_THEMES.keys())}")
    
    theme_config = INSTITUTION_THEMES[theme]
    
    # Build summary section
    summary_items = ""
    for key, value in summary_data.items():
        summary_items += f'<li><strong>{key}:</strong> {value}</li>'
    
    # Build content sections  
    sections_html = ""
    for section in content_sections:
        sections_html += f"""
        <div style="background:{theme_config['background_color']};padding:15px;border-radius:8px;margin:15px 0;border-left:4px solid {theme_config['primary_color']};">
            <h3 style="color:{theme_config['primary_color']};margin:0 0 12px 0;font-size:18px;">{section['title']}</h3>
            <div style="color:{theme_config['text_color']};line-height:1.6;">
                {section['content']}
            </div>
        </div>
        """
    
    # Default footer
    if footer_text is None:
        footer_text = "Higher Education Data Quality Framework"
    
    # Build complete email
    email_html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{title}</title>
    </head>
    <body style="margin:0;padding:20px;font-family: Arial, sans-serif;background-color:#ffffff;">
        <div style="max-width: 800px; margin: 0 auto; background: white;">
            
            <!-- Header -->
            <div style="background: linear-gradient(135deg, {theme_config['primary_color']}, {theme_config['accent_color']});padding:25px;border-radius:8px 8px 0 0;">
                <h1 style="color: white; margin: 0; font-size: 24px; text-align: center;">
                    {title}
                </h1>
                {f'<p style="color: rgba(255,255,255,0.9); text-align: center; margin: 8px 0 0 0; font-size: 14px;">Generated on {pendulum.now().format("MMMM DD, YYYY at HH:mm")} UTC</p>' if include_branding else ''}
            </div>
            
            <!-- Summary Section -->
            <div style="background:{theme_config['background_color']};padding:20px;border-left:4px solid {theme_config['primary_color']};">
                <h2 style="color:{theme_config['primary_color']};margin:0 0 15px 0;font-size:20px;">📊 Summary</h2>
                <ul style="font-size:16px; margin:0; padding-left:20px; color:{theme_config['text_color']};">
                    {summary_items}
                </ul>
            </div>
            
            <!-- Content Sections -->
            <div style="padding: 0 20px;">
                {sections_html}
            </div>
            
            <!-- Footer -->
            <div style="margin-top:30px;padding:20px;background:{theme_config['background_color']};border-radius:0 0 8px 8px;border-top:2px solid {theme_config['accent_color']};">
                <p style="margin:0;font-size:14px;color:{theme_config['text_color']};text-align:center;">
                    🤖 <em>{footer_text}</em>
                </p>
                {f'<p style="margin:8px 0 0 0;font-size:12px;color:#666;text-align:center;"><small>Theme: {theme_config["name"]} • Optimized for {theme_config["context"]}</small></p>' if include_branding else ''}
            </div>
            
        </div>
    </body>
    </html>
    """
    
    return email_html


def create_status_indicator_email(
    theme: str,
    title: str,
    status: str,  # "success", "warning", "error"
    summary_data: Dict[str, Any],
    details: str = None
) -> str:
    """
    Create an email with prominent status indicators for quick scanning.
    
    Args:
        theme (str): Theme from INSTITUTION_THEMES
        title (str): Email title
        status (str): "success", "warning", or "error" 
        summary_data (dict): Key metrics to display
        details (str): Optional detailed content
        
    Returns:
        str: HTML email with status indicators
    """
    
    theme_config = INSTITUTION_THEMES[theme]
    
    # Status configuration
    status_config = {
        "success": {
            "icon": "✅",
            "color": theme_config["success_color"],
            "bg": "#e8f5e8",
            "message": "All systems operating normally"
        },
        "warning": {
            "icon": "⚠️", 
            "color": theme_config["warning_color"],
            "bg": "#fff3e0",
            "message": "Issues detected - review required"
        },
        "error": {
            "icon": "❌",
            "color": theme_config["error_color"], 
            "bg": "#ffebee",
            "message": "Critical issues - immediate attention required"
        }
    }
    
    if status not in status_config:
        status = "warning"  # Default fallback
    
    status_info = status_config[status]
    
    # Build summary section with status styling
    summary_items = ""
    for key, value in summary_data.items():
        summary_items += f'<li><strong>{key}:</strong> {value}</li>'
    
    # Status indicator section
    status_section = {
        "title": f"{status_info['icon']} Status: {status.upper()}",
        "content": f"""
        <div style="padding:15px;background:{status_info['bg']};border-radius:5px;border-left:6px solid {status_info['color']};">
            <h4 style="color:{status_info['color']};margin:0 0 10px 0;">{status_info['message']}</h4>
            <ul style="margin:0;padding-left:20px;color:{theme_config['text_color']};">
                {summary_items}
            </ul>
        </div>
        """
    }
    
    sections = [status_section]
    
    # Add details section if provided
    if details:
        sections.append({
            "title": "📋 Additional Details",
            "content": details
        })
    
    return create_themed_email(
        theme=theme,
        title=title,
        summary_data={},  # Already included in status section
        content_sections=sections
    )


def create_multi_stakeholder_email(
    theme: str,
    title: str,
    stakeholder_sections: Dict[str, Dict[str, Any]]
) -> str:
    """
    Create an email with different sections targeted at different stakeholders.
    
    Args:
        theme (str): Theme from INSTITUTION_THEMES
        title (str): Main email title
        stakeholder_sections (dict): Sections keyed by stakeholder group
        
    Returns:
        str: Multi-section HTML email
        
    Example:
        >>> sections = {
        ...     "registrar": {
        ...         "title": "📚 For Registrar Office", 
        ...         "data": {"Classification Issues": 5},
        ...         "content": "<p>Student classification review needed...</p>"
        ...     },
        ...     "admissions": {
        ...         "title": "🎓 For Admissions Office",
        ...         "data": {"Degree Mismatches": 3}, 
        ...         "content": "<p>Degree code synchronization required...</p>"
        ...     }
        ... }
    """
    
    theme_config = INSTITUTION_THEMES[theme]
    
    # Build stakeholder sections
    content_sections = []
    overall_summary = {}
    
    for stakeholder, section_data in stakeholder_sections.items():
        # Add to overall summary
        for key, value in section_data.get("data", {}).items():
            overall_summary[f"{stakeholder.title()} - {key}"] = value
        
        # Create stakeholder section
        stakeholder_content = section_data.get("content", "")
        if section_data.get("data"):
            data_list = "".join([
                f'<li><strong>{k}:</strong> {v}</li>' 
                for k, v in section_data["data"].items()
            ])
            stakeholder_content = f'<ul style="margin:0 0 15px 0;padding-left:20px;">{data_list}</ul>' + stakeholder_content
        
        content_sections.append({
            "title": section_data["title"],
            "content": stakeholder_content
        })
    
    return create_themed_email(
        theme=theme,
        title=title,
        summary_data=overall_summary,
        content_sections=content_sections
    )


# =============================================================================
# SPECIALIZED TEMPLATES FOR COMMON SCENARIOS
# =============================================================================

def create_data_quality_alert(
    violations_by_check: Dict[str, int],
    theme: str = "alert_orange",
    severity: str = "medium"
) -> str:
    """Create standardized data quality alert email."""
    
    total_violations = sum(violations_by_check.values())
    checks_with_issues = len([v for v in violations_by_check.values() if v > 0])
    
    # Build violations list
    violations_content = ""
    for check_name, count in violations_by_check.items():
        if count > 0:
            violations_content += f'<li><strong>{check_name}:</strong> {count:,} violations</li>'
    
    if violations_content:
        violations_content = f'<ul style="margin:10px 0;padding-left:20px;">{violations_content}</ul>'
    
    severity_config = {
        "low": {"icon": "ℹ️", "label": "Low Priority"},
        "medium": {"icon": "⚠️", "label": "Medium Priority"}, 
        "high": {"icon": "🚨", "label": "High Priority"},
        "critical": {"icon": "❌", "label": "CRITICAL"}
    }
    
    sev_info = severity_config.get(severity, severity_config["medium"])
    
    return create_status_indicator_email(
        theme=theme,
        title=f"{sev_info['icon']} Data Quality Alert - {total_violations:,} Issues Detected",
        status="warning" if severity in ["low", "medium"] else "error",
        summary_data={
            "Total Violations": f"{total_violations:,}",
            "Affected Checks": f"{checks_with_issues}",
            "Severity Level": sev_info["label"]
        },
        details=f"""
        <h4>Violations by Check Type:</h4>
        {violations_content}
        <p><strong>Next Steps:</strong></p>
        <ol>
            <li>Review attached CSV files for detailed violation data</li>
            <li>Coordinate with relevant departments for remediation</li>
            <li>Monitor next execution for improvement trends</li>
        </ol>
        """
    )


def create_success_report(
    checks_completed: int,
    theme: str = "success_green"
) -> str:
    """Create standardized success report email."""
    
    return create_status_indicator_email(
        theme=theme,
        title=f"✅ Data Quality Report - All Checks Passed",
        status="success",
        summary_data={
            "Checks Completed": checks_completed,
            "Violations Found": 0,
            "Data Quality Status": "EXCELLENT"
        },
        details="""
        <p style="font-size:16px;color:#2e7d32;">
            <strong>🎉 Outstanding!</strong> All data quality validations completed successfully with no violations detected.
        </p>
        <p>Your institutional data maintains high quality standards. Continue current data management practices.</p>
        """
    )


# =============================================================================
# THEME USAGE EXAMPLES
# =============================================================================

def demonstrate_themes():
    """
    Demonstrate all available themes with sample data.
    This function can be used for testing theme appearance.
    """
    
    sample_data = {
        "Total Students": "15,247",
        "Violations Found": "23", 
        "Checks Completed": "5",
        "Success Rate": "99.85%"
    }
    
    sample_sections = [{
        "title": "📋 Validation Results",
        "content": """
        <p>The following validation checks were completed:</p>
        <ul>
            <li><strong>Class Code Validation:</strong> 12 violations</li>
            <li><strong>Degree Consistency:</strong> 8 violations</li>
            <li><strong>GPA Requirements:</strong> 3 violations</li>
        </ul>
        <p>Detailed results are available in the attached CSV files.</p>
        """
    }]
    
    print("Generating theme demonstration emails...")
    
    for theme_name in INSTITUTION_THEMES.keys():
        try:
            email_content = create_themed_email(
                theme=theme_name,
                title=f"📊 Data Quality Report - {INSTITUTION_THEMES[theme_name]['name']} Theme",
                summary_data=sample_data,
                content_sections=sample_sections
            )
            
            # Save to file for review
            filename = f"theme_demo_{theme_name}.html"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(email_content)
            
            print(f"✅ Generated: {filename}")
            
        except Exception as e:
            print(f"❌ Error generating {theme_name}: {e}")
    
    print("Theme demonstration complete!")


if __name__ == "__main__":
    # Run theme demonstration if script is executed directly
    demonstrate_themes()
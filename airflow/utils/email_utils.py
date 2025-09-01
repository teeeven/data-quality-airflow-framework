# airflow/utils/email_utils.py
# Email utility functions for institutional notifications

import os
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email import encoders
from typing import List, Optional

# Configure module-level logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def send_email(
    to: List[str],
    subject: str,
    html_content: str,
    smtp_server: Optional[str] = None,
    port: Optional[int] = None,
    sender_email: Optional[str] = None,
    password: Optional[str] = None,
    attachments: Optional[List[str]] = None,
) -> bool:
    """
    Send an email with optional file attachments using SMTP.

    This function provides a standardized way to send HTML emails with attachments
    for institutional notifications. It supports various SMTP configurations and
    falls back to environment variables for sensitive credentials.

    Reads defaults from environment variables if not provided:
      - AIRFLOW__SMTP__SMTP_HOST (default: smtp.office365.com) 
      - AIRFLOW__SMTP__SMTP_PORT (default: 587)
      - AIRFLOW__SMTP__SMTP_USER (sender email)
      - AIRFLOW__SMTP__SMTP_PASSWORD

    Parameters:
        to (List[str]): List of recipient email addresses
        subject (str): Email subject line
        html_content (str): HTML body content of the email
        smtp_server (str, optional): SMTP server hostname
        port (int, optional): SMTP port number (587 for TLS)
        sender_email (str, optional): Sender email address
        password (str, optional): SMTP password or app-specific password
        attachments (List[str], optional): List of file paths to attach

    Returns:
        bool: True if email sent successfully

    Raises:
        ValueError: If required SMTP credentials are missing
        Exception: For SMTP connection or file attachment errors

    Example:
        >>> send_email(
        ...     to=["data_team@institution.edu", "another_email@institution.edu],
        ...     subject="Data Quality Report",
        ...     html_content="<h1>Report Complete</h1>",
        ...     attachments=["/path/to/results.csv"]
        ... )
    """
    # Resolve configuration from parameters or environment variables
    smtp_server = smtp_server or os.getenv("AIRFLOW__SMTP__SMTP_HOST", "smtp.office365.com")
    port = port or int(os.getenv("AIRFLOW__SMTP__SMTP_PORT", 587))
    sender_email = sender_email or os.getenv("AIRFLOW__SMTP__SMTP_USER")
    password = password or os.getenv("AIRFLOW__SMTP__SMTP_PASSWORD")

    # Validate required credentials are available
    if not sender_email or not password:
        logger.error("SMTP credentials not provided: SMTP_USER='%s', SMTP_PASSWORD set=%s",
                     sender_email, bool(password))
        raise ValueError("SMTP_USER and SMTP_PASSWORD must be set as environment variables or parameters.")

    # Build the multipart email message
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = ", ".join(to)
    message["Subject"] = subject
    message.attach(MIMEText(html_content, "html"))

    # Attach files if provided
    for filepath in attachments or []:
        try:
            with open(filepath, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            filename = os.path.basename(filepath)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename=\"{filename}\""
            )
            message.attach(part)
            logger.debug("Successfully attached file: %s", filepath)
        except Exception as e:
            logger.exception("Failed to attach file '%s': %s", filepath, e)
            raise

    # Establish SMTP connection and send email
    try:
        with smtplib.SMTP(smtp_server, port) as server:
            server.ehlo()  # Identify ourselves to SMTP server
            server.starttls()  # Enable TLS encryption
            server.ehlo()  # Re-identify over TLS connection
            server.login(sender_email, password)  # Authenticate
            server.sendmail(sender_email, to, message.as_string())
        
        logger.info("Email sent successfully to: %s", to)
        return True
        
    except Exception as e:
        logger.exception("Error sending email via SMTP server %s:%s - %s", smtp_server, port, e)
        raise


def create_institutional_html_template(
    title: str,
    summary_data: dict,
    sections: List[dict],
    theme_color: str = "#2e7d32",
    background_color: str = "#e8f5e8"
) -> str:
    """
    Create a standardized HTML email template for institutional communications.
    
    Args:
        title (str): Main title of the email
        summary_data (dict): Key-value pairs for summary section
        sections (List[dict]): List of content sections with title and content
        theme_color (str): Primary color for headers and accents
        background_color (str): Background color for content sections
        
    Returns:
        str: Formatted HTML content
    """
    summary_items = "".join([
        f"<li><strong>{key}:</strong> {value}</li>"
        for key, value in summary_data.items()
    ])
    
    section_content = ""
    for section in sections:
        section_content += f"""
        <div style="background:{background_color};padding:15px;border-radius:5px;margin:15px 0;">
            <h3 style="color:{theme_color};margin:0 0 10px 0;">{section['title']}</h3>
            <div>{section['content']}</div>
        </div>
        """
    
    html_template = f"""
    <div style="font-family: Arial, sans-serif; max-width: 800px;">
        <h1 style="color: {theme_color}; border-bottom: 3px solid {theme_color};">
            {title}
        </h1>
        
        <div style="background:{background_color};padding:15px;border-radius:5px;margin:15px 0;">
            <h2 style="color:{theme_color};margin:0 0 10px 0;">📋 Summary</h2>
            <ul style="font-size:16px; margin:0; padding-left:1.2em;">
                {summary_items}
            </ul>
        </div>
        
        {section_content}
        
        <div style="margin-top:20px;padding:10px;background:#f5f5f5;border-radius:5px;">
            <p style="margin:0;font-size:14px;color:#666;">
                🤖 <em>Generated by Institutional Data Quality System</em>
            </p>
        </div>
    </div>
    """
    
    return html_template
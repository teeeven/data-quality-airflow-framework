# airflow/utils/dq_utils.py
# Data Quality utility functions for Airflow DAGs

import logging
from datetime import datetime
from typing import Optional, Tuple
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

# Module-level logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def execute_data_quality_check(
    sql_file_path: str,
    conn_id: str = "institutional_database",
    check_name: Optional[str] = None
) -> Tuple[pd.DataFrame, int]:
    """
    Execute a data quality check SQL query and return results.
    
    This function provides a standardized way to execute data quality validation
    queries from SQL files. It handles file loading, query execution, and result
    processing with comprehensive error handling and logging.
    
    Args:
        sql_file_path (str): Path to SQL file relative to airflow/sql/
        conn_id (str): Airflow connection ID for database (default: "institutional_database")
        check_name (str, optional): Name of the check for logging purposes
        
    Returns:
        Tuple[pd.DataFrame, int]: Query results DataFrame and violation count
        
    Raises:
        FileNotFoundError: If the SQL file cannot be found
        Exception: If query execution fails
        
    Example:
        >>> results_df, count = execute_data_quality_check(
        ...     sql_file_path="class_code_validation.sql",
        ...     conn_id="prod_db",
        ...     check_name="class_codes"
        ... )
        >>> print(f"Found {count} violations")
    """
    check_name = check_name or sql_file_path
    logger.info(f"Executing data quality check: {check_name}")
    
    # Construct full path to SQL file
    sql_full_path = Path("/usr/local/airflow/airflow/sql") / sql_file_path
    
    if not sql_full_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_full_path}")
    
    # Read SQL file
    try:
        with open(sql_full_path, 'r') as f:
            sql_query = f.read()
        logger.info(f"Loaded SQL query from: {sql_full_path}")
    except Exception as e:
        logger.error(f"Error reading SQL file {sql_full_path}: {e}")
        raise
    
    # Execute query using Airflow connection
    hook = MsSqlHook(mssql_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()
    
    try:
        with engine.connect() as conn:
            result_df = pd.read_sql(text(sql_query), conn)
        
        violation_count = len(result_df)
        logger.info(f"Data quality check '{check_name}' found {violation_count} violations")
        
        return result_df, violation_count
        
    except Exception as e:
        logger.error(f"Error executing data quality check '{check_name}': {e}")
        raise


def save_results_to_csv(
    results_df: pd.DataFrame,
    check_name: str,
    output_dir: str = "/usr/local/airflow/include/output/data_quality_system"
) -> Optional[str]:
    """
    Save data quality check results to CSV file with timestamp.
    
    Creates timestamped CSV files for data quality violations, enabling
    audit trails and detailed analysis of quality issues. Only creates
    files when violations are detected.
    
    Args:
        results_df (pd.DataFrame): Results DataFrame containing violations
        check_name (str): Name of the check for filename generation
        output_dir (str): Directory to save CSV files (default: standard output directory)
        
    Returns:
        Optional[str]: Path to saved CSV file, None if no results to save
        
    Example:
        >>> filepath = save_results_to_csv(violations_df, "class_codes")
        >>> if filepath:
        ...     print(f"Violations saved to: {filepath}")
    """
    if results_df.empty:
        logger.info(f"No violations found for '{check_name}' - no CSV file created")
        return None
    
    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Generate filename with timestamp for unique identification
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"dq_{check_name}_{timestamp}.csv"
    filepath = output_path / filename
    
    try:
        results_df.to_csv(filepath, index=False)
        logger.info(f"Saved {len(results_df)} violations to: {filepath}")
        return str(filepath)
    except Exception as e:
        logger.error(f"Error saving CSV file {filepath}: {e}")
        raise


def validate_check_configuration(
    sql_file_path: str,
    conn_id: str,
    check_name: str
) -> bool:
    """
    Validate data quality check configuration before execution.
    
    Args:
        sql_file_path (str): Path to SQL file
        conn_id (str): Database connection ID
        check_name (str): Name of the data quality check
        
    Returns:
        bool: True if configuration is valid
        
    Raises:
        ValueError: If configuration is invalid
    """
    # Validate SQL file exists
    sql_full_path = Path("/usr/local/airflow/airflow/sql") / sql_file_path
    if not sql_full_path.exists():
        raise ValueError(f"SQL file not found: {sql_full_path}")
    
    # Validate required parameters
    if not check_name or not check_name.strip():
        raise ValueError("Check name cannot be empty")
        
    if not conn_id or not conn_id.strip():
        raise ValueError("Connection ID cannot be empty")
    
    logger.info(f"Configuration validated for check: {check_name}")
    return True
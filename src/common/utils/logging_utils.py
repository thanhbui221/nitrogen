"""
Utility functions for logging configuration and management.
"""

import logging
import sys
from typing import Optional
from datetime import datetime
from pathlib import Path

def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    log_format: Optional[str] = None
) -> None:
    """
    Configure logging with specified settings.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for logging output
        log_format: Optional custom log format string
    """
    # Set default format if not provided
    if log_format is None:
        log_format = (
            "%(asctime)s [%(levelname)s] "
            "%(name)s:%(lineno)d - %(message)s"
        )
        
    # Create formatter
    formatter = logging.Formatter(log_format)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level.upper())
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Add file handler if log file specified
    if log_file:
        # Create log directory if needed
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    

def get_logger(name: str) -> logging.Logger:
    """
    Get logger with specified name.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)

class JobLogger:
    """Logger class for ETL job execution."""
    
    def __init__(
        self,
        job_name: str,
        log_dir: str = "logs"
    ):
        self.job_name = job_name
        self.start_time = None
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up job-specific log file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.log_dir / f"{job_name}_{timestamp}.log"
        
        setup_logging(
            log_file=str(log_file),
            log_format=(
                "%(asctime)s [%(levelname)s] "
                f"{job_name} - %(message)s"
            )
        )
        
        self.logger = get_logger(job_name)
    
    def start_job(self) -> None:
        """Log job start."""
        self.start_time = datetime.now()
        self.logger.info(
            f"Starting job '{self.job_name}' at {self.start_time}"
        )
    
    def end_job(self) -> None:
        """Log job completion."""
        if self.start_time:
            duration = datetime.now() - self.start_time
            self.logger.info(
                f"Job '{self.job_name}' completed in {duration}"
            )
    
    def log_error(self, error: Exception) -> None:
        """
        Log error details.
        
        Args:
            error: Exception to log
        """
        self.logger.error(
            f"Error in job '{self.job_name}': {str(error)}",
            exc_info=True
        )
    
    def log_step(self, step_name: str, message: str) -> None:
        """
        Log step execution details.
        
        Args:
            step_name: Name of the step
            message: Log message
        """
        self.logger.info(f"Step '{step_name}': {message}")
    
    def log_metric(self, metric_name: str, value: any) -> None:
        """
        Log metric value.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
        """
        self.logger.info(f"Metric '{metric_name}': {value}")

# Example usage:
"""
# Initialize job logger
job_logger = JobLogger("data_transformation")

try:
    # Start job
    job_logger.start_job()
    
    # Log step execution
    job_logger.log_step("extract", "Reading source data")
    
    # Log metrics
    job_logger.log_metric("records_processed", 1000)
    
    # End job
    job_logger.end_job()
    
except Exception as e:
    # Log any errors
    job_logger.log_error(e)
    raise
""" 
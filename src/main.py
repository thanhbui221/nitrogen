from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file

from common.config import load_job_config
from common.job_runner import JobRunner
from factory.register_components import register_components
import sys
import logging

def main():
    """Main entry point for the ETL application"""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    if len(sys.argv) != 2:
        print("Usage: main.py <config_path>")
        sys.exit(1)

    config_path = sys.argv[1]
    
    try:
        # Register components
        register_components()
        
        # Load configuration
        logger.info(f"Loading configuration from {config_path}")
        config = load_job_config(config_path)
        
        # Create and run job
        logger.info(f"Starting job: {config.name}")
        runner = JobRunner(config)
        runner.run()
        
        logger.info(f"Successfully completed job: {config.name}")
        
    except Exception as e:
        logger.error(f"Error running job: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 
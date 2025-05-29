from src.common.config import load_job_config
from src.common.job_runner import JobRunner
import sys

def main():
    """Main entry point for the ETL application"""
    if len(sys.argv) != 2:
        print("Usage: main.py <config_path>")
        sys.exit(1)

    config_path = sys.argv[1]
    
    try:
        # Load configuration
        config = load_job_config(config_path)
        
        # Create and run job
        runner = JobRunner(config)
        runner.run()
        
        print(f"Successfully completed job: {config.job_name}")
        
    except Exception as e:
        print(f"Error running job: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 
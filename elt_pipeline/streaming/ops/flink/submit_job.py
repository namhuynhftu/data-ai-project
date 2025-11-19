"""
Submit Flink SQL job to Flink cluster running in Docker.

This script reads the SQL file and submits it to the Flink JobManager
via the Flink SQL Client running in the Docker container.
"""
import logging
import subprocess
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def submit_flink_sql_job(sql_file: str, flink_container: str = "flink_jobmanager"):
    """
    Submit a Flink SQL job to the Flink cluster.
    
    Args:
        sql_file: Path to the SQL file containing Flink SQL statements
        flink_container: Name of the Flink JobManager container
    """
    sql_path = Path(sql_file)
    
    if not sql_path.exists():
        logger.error(f"SQL file not found: {sql_file}")
        return False
    
    logger.info("=" * 60)
    logger.info("Flink SQL Job Submission")
    logger.info(f"   SQL File: {sql_path.name}")
    logger.info(f"   Target: {flink_container}")
    logger.info("=" * 60)
    
    try:
        # Read SQL file
        sql_content = sql_path.read_text()
        logger.info(f"Loaded SQL file ({len(sql_content)} characters)")
        
        # Copy SQL file into Flink container
        logger.info("Copying SQL file to Flink container...")
        copy_cmd = [
            "docker", "cp",
            str(sql_path.absolute()),
            f"{flink_container}:/tmp/job.sql"
        ]
        
        result = subprocess.run(copy_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Failed to copy SQL file: {result.stderr}")
            return False
        
        logger.info("SQL file copied to container")
        
        # Execute SQL job using Flink SQL Client
        logger.info("Submitting job to Flink cluster...")
        logger.info("   This will run in detached mode...")
        
        exec_cmd = [
            "docker", "exec", "-d", flink_container,
            "/opt/flink/bin/sql-client.sh",
            "-f", "/tmp/job.sql"
        ]
        
        result = subprocess.run(exec_cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Failed to submit job: {result.stderr}")
            return False
        
        logger.info("Job submitted successfully!")
        logger.info("")
        logger.info("To monitor the job:")
        logger.info(f"   1. Flink Web UI: http://localhost:8082")
        logger.info(f"   2. Check logs: docker logs -f {flink_container}")
        logger.info(f"   3. List jobs: docker exec {flink_container} /opt/flink/bin/flink list")
        logger.info("")
        logger.info("Note: The job will run continuously until manually stopped")
        
        return True
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return False


def main():
    """Main entry point."""
    # Path to SQL file
    sql_file = "elt_pipeline/streaming/ops/flink/transaction_monitor.sql"
    
    logger.info("Checking Flink cluster...")
    
    # Check if Flink container is running
    check_cmd = ["docker", "ps", "--filter", "name=flink_jobmanager", "--format", "{{.Names}}"]
    result = subprocess.run(check_cmd, capture_output=True, text=True)
    
    if "flink_jobmanager" not in result.stdout:
        logger.error("Flink JobManager container is not running!")
        logger.error("   Start it with: docker-compose -f docker/docker-compose.streaming.yml up -d")
        sys.exit(1)
    
    logger.info("Flink cluster is running")
    
    # Submit job
    success = submit_flink_sql_job(sql_file)
    
    if success:
        logger.info("=" * 60)
        logger.info("SUCCESS: Flink job is now running!")
        logger.info("=" * 60)
        sys.exit(0)
    else:
        logger.error("=" * 60)
        logger.error("FAILED: Could not submit job")
        logger.error("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()

"""
Submit Flink SQL job to Flink cluster using SQL Client in Docker.

This script executes SQL via Flink's SQL client running in the jobmanager container.
"""
import subprocess
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def submit_flink_sql_job():
    """Submit Flink SQL job using docker exec."""
    
    sql_file = Path(__file__).parent / "transaction_monitor_job.sql"
    
    if not sql_file.exists():
        logger.error(f"❌ SQL file not found: {sql_file}")
        return False
    
    logger.info("=" * 60)
    logger.info("🚀 Submitting Flink SQL Job to Cluster")
    logger.info("=" * 60)
    
    # Read SQL file
    with open(sql_file, 'r') as f:
        sql_content = f.read()
    
    logger.info(f"📄 SQL Job:\n{sql_content[:200]}...")
    
    # Execute SQL in Flink SQL client via docker exec
    # Note: We use 'flink-jobmanager' container which has SQL client
    command = [
        "docker", "exec", "-i", "flink_jobmanager",
        "/opt/flink/bin/sql-client.sh", "-f", "/dev/stdin"
    ]
    
    try:
        logger.info("📤 Submitting to Flink cluster...")
        
        result = subprocess.run(
            command,
            input=sql_content.encode('utf-8'),
            capture_output=True,
            text=False,
            timeout=30
        )
        
        stdout = result.stdout.decode('utf-8', errors='ignore')
        stderr = result.stderr.decode('utf-8', errors='ignore')
        
        if result.returncode == 0:
            logger.info("✅ SQL job submitted successfully!")
            logger.info(f"Output:\n{stdout}")
            return True
        else:
            logger.error(f"❌ Job submission failed (exit code: {result.returncode})")
            logger.error(f"STDOUT:\n{stdout}")
            logger.error(f"STDERR:\n{stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("❌ Submission timed out after 30 seconds")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False


def check_flink_status():
    """Check if Flink cluster is running."""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=flink", "--format", "{{.Names}}\t{{.Status}}"],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0 and result.stdout.strip():
            logger.info("✅ Flink cluster is running:")
            for line in result.stdout.strip().split('\n'):
                logger.info(f"   {line}")
            return True
        else:
            logger.error("❌ Flink cluster is not running")
            logger.error("   Start with: docker-compose -f docker/docker-compose.streaming.yml up -d")
            return False
            
    except Exception as e:
        logger.error(f"❌ Failed to check Flink status: {e}")
        return False


if __name__ == "__main__":
    logger.info("Flink SQL Job Submission")
    logger.info("=" * 60)
    
    # Check Flink is running
    if not check_flink_status():
        exit(1)
    
    # Submit SQL job
    if submit_flink_sql_job():
        logger.info("=" * 60)
        logger.info("✅ Job submitted! Check Flink UI at http://localhost:8081")
        logger.info("=" * 60)
    else:
        logger.error("=" * 60)
        logger.error("❌ Job submission failed")
        logger.error("=" * 60)
        exit(1)

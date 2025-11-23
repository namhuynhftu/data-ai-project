
import logging
import subprocess
import requests
from pathlib import Path
from typing import Dict, List, Optional, Union
from datetime import datetime
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FlinkJobManager:
    """Manager class for Apache Flink jobs via REST API and CLI."""
    
    def __init__(
        self,
        jobmanager_host: str = "localhost",
        jobmanager_port: int = 8082,
        container_name: str = "flink_jobmanager"
    ):
        """
        Initialize Flink Job Manager
        """
        self.base_url = f"http://{jobmanager_host}:{jobmanager_port}"
        self.container_name = container_name
        self.logger = logger
        self.sql_file = '../sql/transaction_monitor_job.sql'
        
    def check_cluster_health(self) -> Dict:
        """
        Check if Flink cluster is running and healthy.
        """
        try:
            # Check Docker containers
            result = subprocess.run(
                ["docker", "ps", "--filter", f"name={self.container_name}", 
                 "--format", "{{.Names}}\t{{.Status}}"],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode != 0 or not result.stdout.strip():
                return {
                    "healthy": False,
                    "message": "Flink containers are not running",
                    "containers": []
                }
            
            # Check REST API
            response = requests.get(f"{self.base_url}/config", timeout=5)
            response.raise_for_status()
            
            # Get cluster overview
            overview = requests.get(f"{self.base_url}/overview", timeout=5).json()
            
            return {
                "healthy": True,
                "message": "Flink cluster is running",
                "containers": result.stdout.strip().split('\n'),
                "taskmanagers": overview.get("taskmanagers", 0),
                "slots_total": overview.get("slots-total", 0),
                "slots_available": overview.get("slots-available", 0),
                "jobs_running": overview.get("jobs-running", 0),
                "flink_version": overview.get("flink-version", "unknown")
            }
            
        except subprocess.TimeoutExpired:
            return {"healthy": False, "message": "Docker command timed out"}
        except requests.RequestException as e:
            return {"healthy": False, "message": f"REST API error: {str(e)}"}
        except Exception as e:
            return {"healthy": False, "message": f"Unexpected error: {str(e)}"}
    
    def list_jobs(
        self,
        status: Optional[str] = None,
        include_completed: bool = True
    ) -> List[Dict]:
        """
        List all Flink jobs with their details.
        """
        try:
            response = requests.get(f"{self.base_url}/jobs/overview", timeout=10)
            response.raise_for_status()
            data = response.json()
            
            jobs = data.get("jobs", [])
            
            # Filter by status if specified
            if status:
                jobs = [j for j in jobs if j.get("state") == status.upper()]
            
            # Filter out completed jobs if requested
            if not include_completed:
                jobs = [j for j in jobs if j.get("state") == "RUNNING"]
            
            # Enrich job information
            enriched_jobs = []
            for job in jobs:
                job_info = {
                    "job_id": job.get("jid"),
                    "name": job.get("name"),
                    "state": job.get("state"),
                    "start_time": self._format_timestamp(job.get("start-time")),
                    "end_time": self._format_timestamp(job.get("end-time")),
                    "duration_ms": job.get("duration"),
                    "duration": self._format_duration(job.get("duration")),
                    "tasks": job.get("tasks", {}),
                    "last_modification": self._format_timestamp(job.get("last-modification"))
                }
                enriched_jobs.append(job_info)
            
            return enriched_jobs
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to list jobs: {e}")
            return []
    

    def submit_sql_job(
        self,
        sql_file: Union[str, Path],
        job_name: Optional[str] = None
    ) -> Dict:
        """
        Submit a Flink SQL job from a file.
        """
        sql_path = Path(sql_file)
        
        if not sql_path.exists():
            return {
                "success": False,
                "message": f"SQL file not found: {sql_file}"
            }
        
        job_name = job_name or sql_path.stem
        
        try:
            # Read SQL content
            with open(sql_path, 'r') as f:
                sql_content = f.read()
            
            # Submit via SQL client in JobManager container
            command = [
                "docker", "exec", "-i", self.container_name,
                "/opt/flink/bin/sql-client.sh", "-f", "/dev/stdin"
            ]
            
            self.logger.info(f"Submitting Flink SQL job: {job_name}")
            
            result = subprocess.run(
                command,
                input=sql_content.encode('utf-8'),
                capture_output=True,
                text=False,
                timeout=60
            )
            
            stdout = result.stdout.decode('utf-8', errors='ignore')
            stderr = result.stderr.decode('utf-8', errors='ignore')
            
            if result.returncode == 0:
                # Extract job ID from output
                job_id = self._extract_job_id_from_output(stdout)
                
                self.logger.info(f"Job submitted successfully: {job_name}")
                if job_id:
                    self.logger.info(f"Job ID: {job_id}")
                
                return {
                    "success": True,
                    "message": "Job submitted successfully",
                    "job_name": job_name,
                    "job_id": job_id
                }
            else:
                self.logger.error(f"Job submission failed: {stderr}")
                return {
                    "success": False,
                    "message": "Job submission failed",
                    "error": stderr
                }
                
        except subprocess.TimeoutExpired:
            return {
                "success": False,
                "message": "Job submission timed out after 60 seconds"
            }
        except Exception as e:
            self.logger.error(f"Unexpected error submitting job: {e}")
            return {
                "success": False,
                "message": f"Unexpected error: {str(e)}"
            }
    
    def cancel_job(
        self,
        job_id: str,
        savepoint: bool = False,
        savepoint_dir: Optional[str] = None
    ) -> Dict:
        """
        Cancel a running Flink job.
        """
        try:
            # Build cancel URL
            if savepoint:
                url = f"{self.base_url}/jobs/{job_id}/stop"
                params = {}
                if savepoint_dir:
                    params['targetDirectory'] = savepoint_dir
                response = requests.post(url, params=params, timeout=30)
            else:
                url = f"{self.base_url}/jobs/{job_id}?mode=cancel"
                response = requests.patch(url, timeout=30)
            
            if response.status_code == 202:
                self.logger.info(f"Job {job_id} cancellation initiated")
                return {
                    "success": True,
                    "message": "Job cancellation initiated",
                    "job_id": job_id
                }
            else:
                error_msg = response.json().get("errors", [response.text])[0]
                self.logger.error(f"Failed to cancel job: {error_msg}")
                return {
                    "success": False,
                    "message": "Failed to cancel job",
                    "error": error_msg
                }
                
        except requests.RequestException as e:
            self.logger.error(f"Failed to cancel job {job_id}: {e}")
            return {
                "success": False,
                "message": f"Request failed: {str(e)}"
            }
    
    def get_running_jobs(self) -> List[Dict]:
        """Get only running jobs."""
        return self.list_jobs(status="RUNNING", include_completed=False)
    
    def get_failed_jobs(self) -> List[Dict]:
        """Get only failed jobs."""
        return self.list_jobs(status="FAILED", include_completed=True)
    
    def monitor_job(self, job_id: str, interval: int = 5, duration: int = 60):
        """
        Monitor a job's progress in real-time.
        """
        import time
        
        self.logger.info(f"Monitoring job {job_id} for {duration} seconds...")
        start_time = time.time()
        
        while (time.time() - start_time) < duration:
            details = self.get_job_details(job_id)
            
            if not details:
                self.logger.error(f"Job {job_id} not found")
                break
            
            state = details.get("state")
            metrics = details.get("metrics", {})
            
            self.logger.info(
                f"Job: {details.get('name')} | "
                f"State: {state} | "
                f"Records In: {metrics.get('records_in', 0)} | "
                f"Records Out: {metrics.get('records_out', 0)}"
            )
            
            if state in ["FINISHED", "FAILED", "CANCELED"]:
                self.logger.info(f"Job reached terminal state: {state}")
                break
            
            time.sleep(interval)
    
    def cleanup_failed_jobs(self) -> Dict:
        """
        Clean up failed jobs (they remain in history).
        """
        failed_jobs = self.get_failed_jobs()
        
        self.logger.info(f"Found {len(failed_jobs)} failed jobs")
        
        # Note: Flink doesn't provide REST API to delete job history
        # Jobs will be automatically cleaned based on jobmanager.archive.fs.dir config
        
        return {
            "failed_jobs_count": len(failed_jobs),
            "message": "Failed jobs are tracked. Configure jobmanager.archive.fs.dir for cleanup",
            "failed_jobs": [j["job_id"] for j in failed_jobs]
        }
    
    @staticmethod
    def _format_timestamp(timestamp_ms: Optional[int]) -> Optional[str]:
        """Format millisecond timestamp to readable string."""
        if timestamp_ms is None or timestamp_ms < 0:
            return None
        return datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
    
    @staticmethod
    def _format_duration(duration_ms: Optional[int]) -> Optional[str]:
        """Format millisecond duration to readable string."""
        if duration_ms is None or duration_ms < 0:
            return None
        
        seconds = duration_ms // 1000
        minutes = seconds // 60
        hours = minutes // 60
        days = hours // 24
        
        if days > 0:
            return f"{days}d {hours % 24}h {minutes % 60}m"
        elif hours > 0:
            return f"{hours}h {minutes % 60}m {seconds % 60}s"
        elif minutes > 0:
            return f"{minutes}m {seconds % 60}s"
        else:
            return f"{seconds}s"
    
    @staticmethod
    def _extract_job_id_from_output(output: str) -> Optional[str]:
        """Extract job ID from SQL client output."""
        import re
        match = re.search(r'Job ID:\s*([a-f0-9]{32})', output)
        if match:
            return match.group(1)
        return None



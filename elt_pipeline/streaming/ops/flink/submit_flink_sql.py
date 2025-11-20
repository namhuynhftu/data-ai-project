
import json

from flink_manager import FlinkJobManager

def main():
    """CLI interface for Flink Job Manager."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Flink Job Manager CLI")
    parser.add_argument("--host", default="localhost", help="Flink JobManager host")
    parser.add_argument("--port", type=int, default=8082, help="Flink REST API port")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # List command
    subparsers.add_parser("list", help="List all jobs")
    subparsers.add_parser("list-running", help="List running jobs")
    subparsers.add_parser("list-failed", help="List failed jobs")

    # Submit command

    submit_parser = subparsers.add_parser("submit", help="Submit SQL job")
    submit_parser.add_argument("sql_file", help="Path to SQL file")
    submit_parser.add_argument("--name", help="Job name")
    
    # Cancel command
    cancel_parser = subparsers.add_parser("cancel", help="Cancel a job")
    cancel_parser.add_argument("job_id", help="Job ID to cancel")
    cancel_parser.add_argument("--savepoint", action="store_true", help="Create savepoint")
    
    # Monitor command
    monitor_parser = subparsers.add_parser("monitor", help="Monitor a job")
    monitor_parser.add_argument("job_id", help="Job ID to monitor")
    monitor_parser.add_argument("--interval", type=int, default=5, help="Check interval")
    monitor_parser.add_argument("--duration", type=int, default=60, help="Monitor duration")
    
    args = parser.parse_args()
    
    manager = FlinkJobManager(jobmanager_host=args.host, jobmanager_port=args.port)
    
    if args.command == "list":
        jobs = manager.list_jobs()
        print(json.dumps(jobs, indent=2))
    
    elif args.command == "list-running":
        jobs = manager.get_running_jobs()
        print(json.dumps(jobs, indent=2))
        
    elif args.command == "health":
        health = manager.check_cluster_health()
        print(json.dumps(health, indent=2))
        
    elif args.command == "submit":
        result = manager.submit_sql_job(args.sql_file, args.name)
        print(json.dumps(result, indent=2))
        
    elif args.command == "cancel":
        result = manager.cancel_job(args.job_id, savepoint=args.savepoint)
        print(json.dumps(result, indent=2))
        
    elif args.command == "monitor":
        manager.monitor_job(args.job_id, args.interval, args.duration)
        
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

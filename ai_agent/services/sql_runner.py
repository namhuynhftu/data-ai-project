"""SQL query execution service for Snowflake and PostgreSQL."""

import os
from typing import List, Dict, Any, Optional
from enum import Enum
from pathlib import Path
import snowflake.connector
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Load environment variables from project config
root_path = Path(__file__).parent.parent.parent
config_path = root_path / "config" / "app" / "development.env"
load_dotenv(str(config_path))


class DatabaseType(str, Enum):
    """Supported database types."""
    SNOWFLAKE = "snowflake"
    POSTGRES = "postgres"


class SQLRunner:
    """Service for executing SQL queries on data warehouses."""
    
    def __init__(self):
        """Initialize SQL runner with database connections."""
        self.snowflake_config = self._get_snowflake_config()
        self.postgres_config = self._get_postgres_config()
    
    def _get_snowflake_config(self) -> Dict[str, Any]:
        """Get Snowflake configuration from environment."""
        return {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "private_key_file": os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH"),
            "private_key_file_pwd": os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW_DATA"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
            "authenticator": "SNOWFLAKE_JWT"
        }
    
    def _get_postgres_config(self) -> Dict[str, Any]:
        """Get PostgreSQL configuration from environment."""
        return {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", 5432)),
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "database": os.getenv("POSTGRES_DB")
        }
    
    def _get_postgres_schema(self) -> str:
        """Get PostgreSQL schema from environment."""
        return os.getenv("POSTGRES_SCHEMA", "public")
    
    def execute_snowflake(
        self,
        query: str,
        max_rows: int = 100
    ) -> Dict[str, Any]:
        """
        Execute query on Snowflake.
        
        Args:
            query: SQL query to execute
            max_rows: Maximum number of rows to return
            
        Returns:
            Dictionary with columns, data, and metadata
        """
        conn = None
        try:
            conn = snowflake.connector.connect(**self.snowflake_config)
            cursor = conn.cursor()
            
            # Add LIMIT clause if not present
            if "LIMIT" not in query.upper():
                query = f"{query.rstrip(';')} LIMIT {max_rows}"
            
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch rows
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            data = [dict(zip(columns, row)) for row in rows]
            
            cursor.close()
            
            return {
                "success": True,
                "columns": columns,
                "data": data,
                "row_count": len(data),
                "database": "snowflake"
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "database": "snowflake"
            }
        finally:
            if conn:
                conn.close()
    
    def execute_postgres(
        self,
        query: str,
        max_rows: int = 100
    ) -> Dict[str, Any]:
        """
        Execute query on PostgreSQL.
        
        Args:
            query: SQL query to execute
            max_rows: Maximum number of rows to return
            
        Returns:
            Dictionary with columns, data, and metadata
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.postgres_config)
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Set search_path to the configured schema
            schema = self._get_postgres_schema()
            cursor.execute(f"SET search_path TO {schema}, public")
            
            # Add LIMIT clause if not present
            if "LIMIT" not in query.upper():
                query = f"{query.rstrip(';')} LIMIT {max_rows}"
            
            cursor.execute(query)
            
            # Fetch rows as dictionaries
            rows = cursor.fetchall()
            
            # Get column names
            columns = [desc.name for desc in cursor.description] if cursor.description else []
            
            # Convert RealDictRow to regular dict
            data = [dict(row) for row in rows]
            
            cursor.close()
            
            return {
                "success": True,
                "columns": columns,
                "data": data,
                "row_count": len(data),
                "database": "postgres"
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "database": "postgres"
            }
        finally:
            if conn:
                conn.close()
    
    def execute(
        self,
        query: str,
        database: DatabaseType,
        max_rows: int = 100
    ) -> Dict[str, Any]:
        """
        Execute query on specified database.
        
        Args:
            query: SQL query to execute
            database: Target database type
            max_rows: Maximum number of rows to return
            
        Returns:
            Query results
        """
        if database == DatabaseType.SNOWFLAKE:
            return self.execute_snowflake(query, max_rows)
        elif database == DatabaseType.POSTGRES:
            return self.execute_postgres(query, max_rows)
        else:
            return {
                "success": False,
                "error": f"Unsupported database type: {database}"
            }
    
    def test_connections(self) -> Dict[str, bool]:
        """
        Test database connections.
        
        Returns:
            Dictionary with connection status for each database
        """
        results = {}
        
        # Test Snowflake
        try:
            result = self.execute_snowflake("SELECT 1 as test", max_rows=1)
            results["snowflake"] = result.get("success", False)
        except Exception:
            results["snowflake"] = False
        
        # Test PostgreSQL
        try:
            result = self.execute_postgres("SELECT 1 as test", max_rows=1)
            results["postgres"] = result.get("success", False)
        except Exception:
            results["postgres"] = False
        
        return results
    
    def get_schema_info(self, database: DatabaseType) -> Dict[str, Any]:
        """
        Get schema information for a database.
        
        Args:
            database: Target database
            
        Returns:
            Schema information
        """
        if database == DatabaseType.SNOWFLAKE:
            query = """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'ANALYTICS'
            ORDER BY table_name, ordinal_position
            """
            return self.execute_snowflake(query, max_rows=1000)
        
        elif database == DatabaseType.POSTGRES:
            query = """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'kafka_streaming'
            ORDER BY table_name, ordinal_position
            """
            return self.execute_postgres(query, max_rows=1000)
        
        return {"success": False, "error": "Unsupported database"}


# Singleton instance
_sql_runner = None


def get_sql_runner() -> SQLRunner:
    """Get or create SQL runner singleton."""
    global _sql_runner
    if _sql_runner is None:
        _sql_runner = SQLRunner()
    return _sql_runner

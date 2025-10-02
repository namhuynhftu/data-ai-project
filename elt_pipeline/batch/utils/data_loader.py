import pandas as pd
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class DataLoader(ABC):
    """Abstract base class for data loading operations."""
    
    def __init__(self, params: Dict[str, Any]):
        self.params = params
        self._connection = None

    def get_db_connection(self) -> Any:
        """Establish and return a database connection."""
        pass
    

    def extract_data(self, sql: str) -> pd.DataFrame:
        """Extract data from the database using the provided SQL query."""
        pass

    def load_data(self, pd_data: pd.DataFrame, params: Dict[str, Any]) -> int:
        """Load pandas DataFrame to destination."""
        pass
    
    def get_watermark(self, table_name: str, watermark: str) -> Optional[str]:
        """Get watermark value for incremental loading."""
        pass
    
    def __enter__(self):
        """Context manager entry."""
        self._connection = self.get_db_connection()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with connection cleanup."""
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass  # Ignore cleanup errors



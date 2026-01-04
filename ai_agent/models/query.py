"""Pydantic models for API requests and responses."""

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from enum import Enum


class DatabaseType(str, Enum):
    """Available database types."""
    SNOWFLAKE = "snowflake"
    POSTGRES = "postgres"
    DOCUMENTS = "documents"
    AUTO = "auto"


class QueryRequest(BaseModel):
    """Request model for query endpoint."""
    query: str = Field(..., description="Natural language query")
    database: DatabaseType = Field(
        default=DatabaseType.AUTO,
        description="Target database or 'auto' for intelligent routing"
    )
    max_results: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Maximum number of results to return"
    )
    include_sql: bool = Field(
        default=True,
        description="Include generated SQL in response"
    )


class SourceInfo(BaseModel):
    """Information about data source."""
    type: str = Field(..., description="Source type (sql/document)")
    database: Optional[str] = Field(None, description="Database name")
    sql: Optional[str] = Field(None, description="Executed SQL query")
    documents: Optional[List[str]] = Field(None, description="Referenced documents")


class QueryResponse(BaseModel):
    """Response model for query endpoint."""
    answer: str = Field(..., description="Natural language answer")
    sources: List[SourceInfo] = Field(
        default=[],
        description="Data sources used"
    )
    data: Optional[List[Dict[str, Any]]] = Field(
        None,
        description="Raw data from SQL queries"
    )
    confidence: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Confidence score"
    )
    query_type: str = Field(..., description="Detected query type")


class UploadPDFRequest(BaseModel):
    """Request model for PDF upload."""
    filename: str = Field(..., description="PDF filename")
    description: Optional[str] = Field(
        None,
        description="Document description"
    )


class UploadPDFResponse(BaseModel):
    """Response model for PDF upload."""
    success: bool
    message: str
    chunks_created: int
    filename: str


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    databases: Dict[str, bool]
    vector_store: bool
    documents_indexed: int

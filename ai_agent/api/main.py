"""FastAPI application for AI agent."""

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import os
from pathlib import Path

from models.query import (
    QueryRequest,
    QueryResponse,
    UploadPDFRequest,
    HealthResponse,
    DatabaseType,
    SourceInfo
)
from services.agent import get_agent
from services.pdf_loader import get_pdf_loader
from services.rag_retriever import get_rag_retriever
from services.sql_runner import get_sql_runner

# Initialize FastAPI app
app = FastAPI(
    title="Data Warehouse AI Agent",
    description="Intelligent agent for querying Snowflake, PostgreSQL, and PDF documents",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
agent = get_agent()
pdf_loader = get_pdf_loader()
rag_retriever = get_rag_retriever()
sql_runner = get_sql_runner()


@app.get("/", tags=["Root"])
def root():
    """Root endpoint."""
    return {
        "message": "Data Warehouse AI Agent API",
        "version": "1.0.0",
        "endpoints": {
            "query": "/query",
            "upload_pdf": "/upload-pdf",
            "health": "/health",
            "databases": "/databases",
            "index_documents": "/index-documents"
        }
    }


@app.post("/query", response_model=QueryResponse, tags=["Query"])
def query(request: QueryRequest) -> QueryResponse:
    """
    Query the data warehouses or documents.
    
    Args:
        request: Query request with question and optional database
        
    Returns:
        Query response with answer and sources
    """
    try:
        # Get database type string
        if request.database == DatabaseType.AUTO:
            db_str = "auto"
        elif request.database == DatabaseType.SNOWFLAKE:
            db_str = "snowflake"
        elif request.database == DatabaseType.POSTGRES:
            db_str = "postgres"
        elif request.database == DatabaseType.DOCUMENTS:
            db_str = "documents"
        else:
            db_str = "auto"
        
        # Query agent
        result = agent.answer_query(
            query=request.query,
            database=db_str,
            max_results=request.max_results
        )
        
        # Build response
        sources = []
        for source in result.get("sources", []):
            if source.get("type") == "sql":
                sources.append(
                    SourceInfo(
                        type="sql",
                        database=source.get("database"),
                        sql_query=source.get("sql")
                    )
                )
            elif source.get("type") == "document":
                sources.append(
                    SourceInfo(
                        type="document",
                        documents=source.get("documents", [])
                    )
                )
        
        return QueryResponse(
            answer=result.get("answer", ""),
            sources=sources,
            sql_query=result.get("sql"),
            data=result.get("data"),
            row_count=len(result.get("data", []))
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload-pdf", tags=["Documents"])
def upload_pdf(file: UploadFile = File(...)):
    """
    Upload and index a PDF document.
    
    Args:
        file: PDF file to upload
        
    Returns:
        Success message with indexing details
    """
    try:
        # Validate file type
        if not file.filename.endswith(".pdf"):
            raise HTTPException(status_code=400, detail="Only PDF files are supported")
        
        # Create documents directory if not exists
        docs_dir = Path("./documents")
        docs_dir.mkdir(exist_ok=True)
        
        # Save file
        file_path = docs_dir / file.filename
        with open(file_path, "wb") as f:
            content = file.file.read()
            f.write(content)
        
        # Process and index PDF
        chunks = pdf_loader.process_pdf(str(file_path))
        
        # Index in vector store
        rag_retriever.index_documents(chunks)
        
        return {
            "success": True,
            "message": f"Successfully uploaded and indexed {file.filename}",
            "filename": file.filename,
            "chunks_created": len(chunks),
            "total_indexed": rag_retriever.get_indexed_count()
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/index-documents", tags=["Documents"])
def index_documents():
    """
    Index all PDF documents in the documents directory.
    
    Returns:
        Indexing status and count
    """
    try:
        docs_dir = Path("./documents")
        if not docs_dir.exists():
            docs_dir.mkdir()
            return {
                "success": True,
                "message": "Documents directory created",
                "indexed_count": 0
            }
        
        # Process all PDFs
        chunks = pdf_loader.process_all_pdfs(str(docs_dir))
        
        # Index in vector store
        if chunks:
            rag_retriever.index_documents(chunks)
        
        return {
            "success": True,
            "message": f"Indexed {len(chunks)} chunks from documents",
            "chunks_count": len(chunks),
            "total_indexed": rag_retriever.get_indexed_count()
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health", response_model=HealthResponse, tags=["Health"])
def health_check() -> HealthResponse:
    """
    Check system health and connectivity.
    
    Returns:
        Health status for all components
    """
    status = {
        "api": "healthy",
        "snowflake": "unknown",
        "postgres": "unknown",
        "vector_store": "unknown"
    }
    
    # Test database connections
    try:
        db_status = sql_runner.test_connections()
        status["snowflake"] = "healthy" if db_status.get("snowflake") else "unhealthy"
        status["postgres"] = "healthy" if db_status.get("postgres") else "unhealthy"
    except Exception:
        status["snowflake"] = "error"
        status["postgres"] = "error"
    
    # Test vector store
    try:
        count = rag_retriever.get_indexed_count()
        status["vector_store"] = "healthy"
    except Exception:
        status["vector_store"] = "error"
    
    # Overall status
    all_healthy = all(v == "healthy" for v in status.values())
    
    return HealthResponse(
        status="healthy" if all_healthy else "degraded",
        components=status,
        indexed_documents=rag_retriever.get_indexed_count()
    )


@app.get("/databases", tags=["Info"])
def list_databases():
    """
    List available databases and their schemas.
    
    Returns:
        Database information
    """
    try:
        databases = []
        
        # Snowflake schema
        snowflake_schema = sql_runner.get_schema_info(DatabaseType.SNOWFLAKE)
        if snowflake_schema.get("success"):
            tables = set()
            for row in snowflake_schema.get("data", []):
                table_name = row.get("table_name") or row.get("TABLE_NAME")
                if table_name:
                    tables.add(table_name)
            
            databases.append({
                "name": "snowflake",
                "type": "data_warehouse",
                "description": "Historical analytical data warehouse",
                "tables": sorted(list(tables))
            })
        
        # PostgreSQL schema
        postgres_schema = sql_runner.get_schema_info(DatabaseType.POSTGRES)
        if postgres_schema.get("success"):
            tables = set()
            for row in postgres_schema.get("data", []):
                table_name = row.get("table_name") or row.get("TABLE_NAME")
                if table_name:
                    tables.add(table_name)
            
            databases.append({
                "name": "postgres",
                "type": "streaming_database",
                "description": "Real-time streaming data",
                "tables": sorted(list(tables))
            })
        
        # Document store
        databases.append({
            "name": "documents",
            "type": "vector_store",
            "description": "PDF documentation and policies",
            "indexed_count": rag_retriever.get_indexed_count()
        })
        
        return {
            "success": True,
            "databases": databases
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

"""Main AI agent orchestration service."""

import os
from typing import Dict, Any, List
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from services.sql_runner import get_sql_runner, DatabaseType
from services.rag_retriever import get_rag_retriever
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class DataWarehouseAgent:
    """Intelligent agent for querying data warehouses and documents."""
    
    def __init__(self, model: str = "gpt-4o-mini", temperature: float = 0.1):
        """
        Initialize the agent.
        
        Args:
            model: OpenAI model to use
            temperature: Model temperature
        """
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY not found in environment variables")
        
        self.llm = ChatOpenAI(
            model=model,
            temperature=temperature,
            openai_api_key=api_key
        )
        
        self.sql_runner = get_sql_runner()
        self.rag_retriever = get_rag_retriever()
    
    def classify_query(self, query: str) -> Dict[str, Any]:
        """
        Classify the query type and determine appropriate data source.
        
        Args:
            query: User query
            
        Returns:
            Dictionary with query type and recommended database
        """
        prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a query classifier. Analyze the user's query and determine:
1. Query type: 'sql', 'document', or 'hybrid'
2. If SQL query, which database: 'snowflake' (historical/analytical data) or 'postgres' (real-time streaming data)

Snowflake contains:
- Customer data (olist_customers_dataset)
- Orders data (olist_orders_dataset)
- Products data (olist_products_dataset)
- Order items, payments, reviews, sellers, geolocation
- Historical e-commerce analytics

PostgreSQL contains:
- Real-time streaming data
- User alerts for high-value transactions
- Recent transaction monitoring

Documents contain:
- Data pipeline documentation
- Data masking policies
- Compliance information
- Setup guides

Return ONLY a JSON object with these fields:
{{"query_type": "sql|document|hybrid", "database": "snowflake|postgres|documents", "reasoning": "brief explanation"}}"""),
            ("user", "{query}")
        ])
        
        chain = prompt | self.llm
        response = chain.invoke({"query": query})
        
        # Parse response
        try:
            import json
            result = json.loads(response.content)
            return result
        except Exception:
            # Default to SQL on Snowflake if parsing fails
            return {
                "query_type": "sql",
                "database": "snowflake",
                "reasoning": "Default classification"
            }
    
    def generate_sql(self, query: str, database: str) -> str:
        """
        Generate SQL query from natural language.
        
        Args:
            query: Natural language query
            database: Target database
            
        Returns:
            Generated SQL query
        """
        # Get schema information
        db_type = DatabaseType.SNOWFLAKE if database == "snowflake" else DatabaseType.POSTGRES
        schema_info = self.sql_runner.get_schema_info(db_type)
        
        # Format schema for context
        schema_context = "Available tables and columns:\n"
        if schema_info.get("success"):
            current_table = None
            for row in schema_info.get("data", []):
                table_name = row.get("table_name") or row.get("TABLE_NAME")
                column_name = row.get("column_name") or row.get("COLUMN_NAME")
                data_type = row.get("data_type") or row.get("DATA_TYPE")
                
                if table_name != current_table:
                    schema_context += f"\n{table_name}:\n"
                    current_table = table_name
                schema_context += f"  - {column_name} ({data_type})\n"
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", f"""You are a SQL expert. Generate a valid SQL query for {database}.

{schema_context}

Important notes:
- For Snowflake: Use uppercase for table/column names
- For PostgreSQL: Use lowercase
- Always include appropriate JOINs
- Add LIMIT clause for large result sets
- Use clear aliases
- Return ONLY the SQL query, no explanations"""),
            ("user", "{query}")
        ])
        
        chain = prompt | self.llm
        response = chain.invoke({"query": query})
        
        # Clean up SQL
        sql = response.content.strip()
        # Remove markdown code blocks if present
        sql = sql.replace("```sql", "").replace("```", "").strip()
        
        return sql
    
    def answer_with_sql(
        self,
        query: str,
        database: str,
        max_results: int = 10
    ) -> Dict[str, Any]:
        """
        Answer query using SQL.
        
        Args:
            query: User query
            database: Target database
            max_results: Maximum results to return
            
        Returns:
            Answer with SQL results
        """
        # Generate SQL
        sql = self.generate_sql(query, database)
        
        # Execute SQL
        db_type = DatabaseType.SNOWFLAKE if database == "snowflake" else DatabaseType.POSTGRES
        result = self.sql_runner.execute(sql, db_type, max_rows=max_results)
        
        if not result.get("success"):
            return {
                "answer": f"Error executing query: {result.get('error')}",
                "sql": sql,
                "data": None,
                "sources": [{"type": "sql", "database": database, "sql": sql}]
            }
        
        # Generate natural language answer
        prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a data analyst. Provide a clear, concise answer based on the query results.
Summarize key findings and insights. Format numbers appropriately."""),
            ("user", """User query: {query}

SQL executed: {sql}

Results ({row_count} rows):
{data}

Provide a natural language answer:""")
        ])
        
        # Format data for context
        data_str = "\n".join([str(row) for row in result.get("data", [])[:5]])  # First 5 rows
        
        chain = prompt | self.llm
        response = chain.invoke({
            "query": query,
            "sql": sql,
            "row_count": result.get("row_count", 0),
            "data": data_str
        })
        
        return {
            "answer": response.content,
            "sql": sql,
            "data": result.get("data", []),
            "sources": [{"type": "sql", "database": database, "sql": sql}]
        }
    
    def answer_with_documents(self, query: str, top_k: int = 4) -> Dict[str, Any]:
        """
        Answer query using RAG from documents.
        
        Args:
            query: User query
            top_k: Number of documents to retrieve
            
        Returns:
            Answer with document sources
        """
        # Retrieve relevant documents
        docs = self.rag_retriever.retrieve(query, top_k=top_k)
        
        if not docs:
            return {
                "answer": "I couldn't find relevant information in the documents.",
                "sql": None,
                "data": None,
                "sources": [{"type": "document", "documents": []}]
            }
        
        # Format context
        context = self.rag_retriever.format_context(docs)
        
        # Generate answer
        prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a helpful assistant. Answer the user's question based on the provided context.
Cite the document sources in your answer. If the context doesn't contain the answer, say so."""),
            ("user", """Question: {query}

Context from documents:
{context}

Answer:""")
        ])
        
        chain = prompt | self.llm
        response = chain.invoke({"query": query, "context": context})
        
        # Extract document sources
        doc_sources = [doc.get("source") for doc in docs]
        
        return {
            "answer": response.content,
            "sql": None,
            "data": None,
            "sources": [{"type": "document", "documents": doc_sources}]
        }
    
    def answer_query(
        self,
        query: str,
        database: str = "auto",
        max_results: int = 10
    ) -> Dict[str, Any]:
        """
        Answer user query using appropriate data source.
        
        Args:
            query: User query
            database: Target database or 'auto' for intelligent routing
            max_results: Maximum results to return
            
        Returns:
            Answer with sources and data
        """
        # Classify query if auto mode
        if database == "auto":
            classification = self.classify_query(query)
            query_type = classification.get("query_type", "sql")
            database = classification.get("database", "snowflake")
        else:
            query_type = "document" if database == "documents" else "sql"
        
        # Route to appropriate handler
        if query_type == "document" or database == "documents":
            result = self.answer_with_documents(query)
            result["query_type"] = "document"
        
        elif query_type == "sql":
            result = self.answer_with_sql(query, database, max_results)
            result["query_type"] = "sql"
        
        elif query_type == "hybrid":
            # Handle hybrid queries (SQL + Documents)
            sql_result = self.answer_with_sql(query, database, max_results)
            doc_result = self.answer_with_documents(query)
            
            # Combine results
            combined_prompt = ChatPromptTemplate.from_messages([
                ("system", "Combine the SQL results and document information to provide a comprehensive answer."),
                ("user", """Question: {query}

SQL Answer: {sql_answer}

Document Answer: {doc_answer}

Provide a unified answer:""")
            ])
            
            chain = combined_prompt | self.llm
            response = chain.invoke({
                "query": query,
                "sql_answer": sql_result.get("answer"),
                "doc_answer": doc_result.get("answer")
            })
            
            result = {
                "answer": response.content,
                "sql": sql_result.get("sql"),
                "data": sql_result.get("data"),
                "sources": sql_result.get("sources", []) + doc_result.get("sources", []),
                "query_type": "hybrid"
            }
        
        else:
            result = {
                "answer": "I couldn't determine how to answer this query.",
                "sql": None,
                "data": None,
                "sources": [],
                "query_type": "unknown"
            }
        
        return result


# Singleton instance
_agent = None


def get_agent() -> DataWarehouseAgent:
    """Get or create agent singleton."""
    global _agent
    if _agent is None:
        _agent = DataWarehouseAgent()
    return _agent

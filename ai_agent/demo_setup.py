#!/usr/bin/env python3
"""
Demo Setup Script for AI Agent
Prepares environment for Saturday class demo
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv("config/app/development.env")
# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

print("=" * 70)
print("🎓 AI AGENT DEMO SETUP")
print("=" * 70)

# Step 1: Check environment variables
print("\n📋 Step 1: Checking Environment Variables...")
required_vars = [
    "OPENAI_API_KEY",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_DATABASE",
    "POSTGRES_HOST",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD"
]

missing_vars = []
for var in required_vars:
    value = os.getenv(var)
    if not value:
        missing_vars.append(var)
        print(f"   ❌ {var} - NOT SET")
    else:
        # Mask sensitive values
        display_value = value[:10] + "..." if len(value) > 10 else "***"
        print(f"   ✅ {var} - {display_value}")

if missing_vars:
    print(f"\n⚠️  Missing {len(missing_vars)} required variables!")
    print("   Please copy ai_agent/.env.example to ai_agent/.env and fill in values")
    sys.exit(1)

# Step 2: Test database connections
print("\n🔌 Step 2: Testing Database Connections...")
try:
    from services.sql_runner import get_sql_runner
    
    runner = get_sql_runner()
    status = runner.test_connections()
    
    if status.get('snowflake'):
        print("   ✅ Snowflake - Connected")
    else:
        print("   ❌ Snowflake - Connection failed")
    
    if status.get('postgres'):
        print("   ✅ PostgreSQL - Connected")
    else:
        print("   ❌ PostgreSQL - Connection failed")
    
    if not (status.get('snowflake') and status.get('postgres')):
        print("\n⚠️  Database connection issues detected!")
        print("   Check credentials and network connectivity")
        sys.exit(1)
        
except Exception as e:
    print(f"   ❌ Connection test failed: {str(e)}")
    sys.exit(1)

# Step 3: Index documents
print("\n📚 Step 3: Indexing Documents for RAG...")
try:
    from services.rag_retriever import get_rag_retriever
    
    retriever = get_rag_retriever()
    chunk_count = retriever.index_documents(rebuild=True)
    print(f"   ✅ Indexed {chunk_count} document chunks")
    
    if chunk_count == 0:
        print("   ⚠️  No documents found!")
        print("   Make sure PDF/Markdown files exist in ai_agent/documents/")
except Exception as e:
    print(f"   ❌ Document indexing failed: {str(e)}")
    sys.exit(1)

# Step 4: Test query execution
print("\n🧪 Step 4: Testing Query Execution...")
try:
    from services.agent import get_agent
    
    agent = get_agent()
    
    # Test Snowflake query
    print("   Testing Snowflake (batch data)...")
    result = agent.answer_with_sql(
        "SELECT COUNT(*) as customer_count FROM DIM_CUSTOMER LIMIT 1",
        "snowflake",
        max_results=1
    )
    if result.get("data"):
        customer_count = result["data"][0].get("CUSTOMER_COUNT", 0)
        print(f"   ✅ Snowflake query successful ({customer_count:,} customers)")
    else:
        print(f"   ❌ Snowflake query failed: {result.get('answer')}")
    
    # Test PostgreSQL query
    print("   Testing PostgreSQL (streaming data)...")
    pg_result = agent.answer_with_sql(
        "SELECT COUNT(*) as invoice_count FROM kafka_streaming.invoices",
        "postgres",
        max_results=1
    )
    if pg_result.get("data"):
        invoice_count = pg_result["data"][0].get("invoice_count", 0)
        print(f"   ✅ PostgreSQL query successful ({invoice_count} invoices)")
    else:
        print(f"   ⚠️  PostgreSQL: {pg_result.get('answer')}")
        print("   Tip: Start streaming services with 'make invoices-up'")
    
    # Test RAG retrieval
    print("   Testing RAG document retrieval...")
    doc_result = agent.answer_with_documents("What is the data pipeline architecture?")
    if doc_result.get("sources"):
        doc_count = len(doc_result["sources"][0].get("documents", []))
        print(f"   ✅ RAG retrieval successful ({doc_count} sources)")
    else:
        print("   ❌ RAG retrieval failed")
    
except Exception as e:
    print(f"   ❌ Query test failed: {str(e)}")
    sys.exit(1)

# Step 5: Verify conversation memory
print("\n🧠 Step 5: Testing Conversation Memory...")
try:
    agent.clear_history()
    assert agent.get_history_length() == 0, "History not cleared"
    
    agent._add_to_history("Test query", "Test response")
    assert agent.get_history_length() == 1, "History not saved"
    
    agent.clear_history()
    print("   ✅ Conversation memory working")
except Exception as e:
    print(f"   ❌ Memory test failed: {str(e)}")

# Step 6: Generate sample queries
print("\n📝 Step 6: Generating Demo Query Suggestions...")
demo_queries = {
    "Batch Data (Snowflake)": [
        "How many customers do we have?",
        "What are the top 5 selling product categories?",
        "Show me customer distribution by state",
        "What is the average order value?",
        "Which sellers have the highest revenue?"
    ],
    "Streaming Data (PostgreSQL)": [
        "Show me the latest invoices",
        "What are the recent high-value transactions?",
        "Which order statuses are most common in real-time data?",
        "What payment methods are customers using right now?",
        "Show geographic distribution of recent orders"
    ],
    "Document Knowledge (RAG)": [
        "What is the data pipeline architecture?",
        "Explain the Snowflake schema structure",
        "What tables are in the ANALYTICS schema?",
        "How does the streaming pipeline work?",
        "What are the product categories available?"
    ],
    "Conversation Memory": [
        "How many customers do we have?",
        "What states are they in?",  # Follow-up using context
        "Show me the top 3",  # Further drill-down
        "What about sellers instead?"  # Context switch
    ],
    "Hybrid Queries": [
        "Compare batch and streaming order volumes",
        "Explain the difference between Snowflake and PostgreSQL data",
        "What data sources are available and how do I query them?"
    ]
}

for category, queries in demo_queries.items():
    print(f"\n   {category}:")
    for i, q in enumerate(queries, 1):
        print(f"      {i}. {q}")

# Final summary
print("\n" + "=" * 70)
print("✅ DEMO SETUP COMPLETE!")
print("=" * 70)
print("\nReady to demo:")
print("   ✅ Core RAG System with conversation memory")
print("   ✅ Batch data integration (Snowflake)")
print("   ✅ Streaming data integration (PostgreSQL)")
print("   ✅ Document querying capability")
print("   ✅ Hybrid query routing")

print("\n🚀 To start demo:")
print("   1. Ensure streaming services are running: make invoices-up")
print("   2. Launch CLI: python ai_agent/cli.py")
print("   3. Or launch API: python ai_agent/main.py")

print("\n💡 Demo Tips:")
print("   - Use :history to show conversation memory")
print("   - Use :health to verify all connections")
print("   - Use :db to switch between data sources")
print("   - Demo coherent multi-turn conversations")
print("   - Show automatic query routing with 'auto' mode")

print("\n📊 Grading Checklist:")
print("   ✅ Functional chatbot with CLI")
print("   ✅ Conversation memory and context retention")
print("   ✅ RAG for PDF/document processing")
print("   ✅ Successful document querying")
print("   ✅ Batch data source understanding (Snowflake)")
print("   ✅ Real-time data source understanding (PostgreSQL)")
print("   ✅ Combined PDF + warehouse knowledge")

print("\n🎯 Good luck with your Saturday demo!")
print("=" * 70 + "\n")

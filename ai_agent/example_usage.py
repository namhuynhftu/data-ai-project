"""Example usage of the AI Agent."""

import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
from services.agent import get_agent

# Load environment
load_dotenv()

print("🤖 AI Agent Example\n")
print("=" * 60)

# Initialize agent
print("\n📦 Initializing agent...")
agent = get_agent()
print("✅ Agent initialized")

# Example queries
examples = [
    {
        "query": "How many customers are in the database?",
        "database": "snowflake",
        "description": "Simple count query on Snowflake"
    },
    {
        "query": "What are the top 5 cities by customer count?",
        "database": "snowflake",
        "description": "Aggregation query on Snowflake"
    },
    {
        "query": "Show me recent high-value transaction alerts",
        "database": "postgres",
        "description": "Real-time data from PostgreSQL"
    },
    {
        "query": "What is the data masking policy?",
        "database": "documents",
        "description": "Document search using RAG"
    },
    {
        "query": "How is customer PII protected in the pipeline?",
        "database": "auto",
        "description": "Auto-routing (likely hybrid: SQL + documents)"
    }
]

print("\n" + "=" * 60)
print("📝 Example Queries")
print("=" * 60)

for i, example in enumerate(examples, 1):
    print(f"\n{i}. {example['description']}")
    print(f"   Query: \"{example['query']}\"")
    print(f"   Database: {example['database']}")
    
    # Uncomment to run the query
    # try:
    #     result = agent.answer_query(
    #         query=example['query'],
    #         database=example['database'],
    #         max_results=5
    #     )
    #     
    #     print(f"\n   Answer: {result['answer'][:200]}...")
    #     
    #     if result.get('sql'):
    #         print(f"\n   SQL: {result['sql'][:100]}...")
    #     
    #     if result.get('data'):
    #         print(f"   Results: {len(result['data'])} rows")
    #
    # except Exception as e:
    #     print(f"   ❌ Error: {str(e)}")

print("\n" + "=" * 60)
print("💡 To run these examples:")
print("1. Uncomment the code in this file")
print("2. Ensure .env is configured")
print("3. Run: python example_usage.py")
print("=" * 60)

print("\n🚀 Quick Start:")
print("""
from services.agent import get_agent

agent = get_agent()
result = agent.answer_query("How many customers?", database="snowflake")
print(result['answer'])
""")

print("\n📚 For more examples, see SETUP.md")

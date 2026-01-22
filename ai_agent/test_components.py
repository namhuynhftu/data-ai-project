"""Test script for AI Agent components."""

import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
project_root = Path(__file__).parent.parent
config_path = project_root / "config" / "app" / "development.env"
# Load environment
load_dotenv(config_path)

print("🧪 AI Agent Component Tests\n")
print("=" * 60)


def test_embeddings():
    """Test OpenAI embeddings service."""
    print("\n1️⃣ Testing Embeddings Service...")
    try:
        from services.embeddings import get_embedding_service
        
        service = get_embedding_service()
        
        # Test query embedding
        embedding = service.embed_query("test query")
        print(f"   ✅ Generated embedding with dimension: {len(embedding)}")
        
        # Test batch embeddings
        embeddings = service.embed_documents(["doc1", "doc2", "doc3"])
        print(f"   ✅ Generated {len(embeddings)} document embeddings")
        
        return True
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        return False


def test_vector_store():
    """Test ChromaDB vector store."""
    print("\n2️⃣ Testing Vector Store...")
    try:
        from services.vector_store import get_vector_store
        from langchain_core.documents import Document
        
        store = get_vector_store()
        
        # Test add documents
        test_docs = [
            Document(page_content="Test document 1", metadata={"source": "test"}),
            Document(page_content="Test document 2", metadata={"source": "test"})
        ]
        
        store.add_documents(test_docs)
        count = store.get_collection_count()
        print(f"   ✅ Added documents, total count: {count}")
        
        # Test search
        results = store.similarity_search("test", k=2)
        print(f"   ✅ Search returned {len(results)} results")
        
        return True
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        return False


def test_sql_runner():
    """Test SQL runner for both databases."""
    print("\n3️⃣ Testing SQL Runner...")
    try:
        from services.sql_runner import get_sql_runner, DatabaseType
        
        runner = get_sql_runner()
        
        # Test connections
        status = runner.test_connections()
        print(f"   Snowflake: {'✅' if status['snowflake'] else '❌'}")
        print(f"   PostgreSQL: {'✅' if status['postgres'] else '❌'}")
        
        # Test schema fetch
        if status['snowflake']:
            schema = runner.get_schema_info(DatabaseType.SNOWFLAKE)
            if schema['success']:
                print(f"   ✅ Retrieved Snowflake schema ({schema['row_count']} columns)")
        
        if status['postgres']:
            schema = runner.get_schema_info(DatabaseType.POSTGRES)
            if schema['success']:
                print(f"   ✅ Retrieved PostgreSQL schema ({schema['row_count']} columns)")
        
        return status['snowflake'] or status['postgres']
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        return False


def test_rag_retriever():
    """Test RAG retriever."""
    print("\n4️⃣ Testing RAG Retriever...")
    try:
        from services.rag_retriever import get_rag_retriever
        
        retriever = get_rag_retriever()
        
        # Check indexed count
        count = retriever.get_indexed_count()
        print(f"   ✅ Indexed documents: {count}")
        
        if count > 0:
            # Test retrieval
            results = retriever.retrieve("test query", top_k=3)
            print(f"   ✅ Retrieved {len(results)} documents")
        else:
            print(f"   ⚠️  No documents indexed yet")
        
        return True
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        return False


def test_agent():
    """Test main agent."""
    print("\n5️⃣ Testing Agent Service...")
    try:
        from services.agent import get_agent
        
        agent = get_agent()
        
        # Test query classification
        classification = agent.classify_query("How many customers are there?")
        print(f"   ✅ Query classification: {classification['query_type']} -> {classification['database']}")
        
        # Test simple query (if databases are connected)
        try:
            result = agent.answer_query("How many tables are in the database?", database="snowflake", max_results=5)
            print(f"   ✅ Query executed successfully")
            print(f"      Answer: {result['answer'][:100]}...")
        except Exception as e:
            print(f"   ⚠️  Query execution skipped: {str(e)[:50]}")
        
        return True
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        return False


def main():
    """Run all tests."""
    results = []
    
    # Check environment
    print("\n🔍 Environment Check...")
    openai_key = os.getenv("OPENAI_API_KEY")
    print(f"   OPENAI_API_KEY: {'✅ Set' if openai_key else '❌ Missing'}")
    
    snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
    print(f"   SNOWFLAKE_ACCOUNT: {'✅ Set' if snowflake_account else '❌ Missing'}")
    
    postgres_host = os.getenv("POSTGRES_HOST")
    print(f"   POSTGRES_HOST: {'✅ Set' if postgres_host else '❌ Missing'}")
    
    # Run tests
    results.append(("Embeddings", test_embeddings()))
    results.append(("Vector Store", test_vector_store()))
    results.append(("SQL Runner", test_sql_runner()))
    results.append(("RAG Retriever", test_rag_retriever()))
    results.append(("Agent", test_agent()))
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 Test Summary\n")
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"   {name:20s} {status}")
    
    print(f"\n   Total: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed!")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")
    
    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

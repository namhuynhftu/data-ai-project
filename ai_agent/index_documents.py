#!/usr/bin/env python3
"""Index documents for RAG retrieval."""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
from services.rag_retriever import get_rag_retriever

# Load environment
root_path = Path(__file__).parent.parent
config_path = root_path / "config" / "app" / "development.env"
load_dotenv(str(config_path))

print("=" * 60)
print("📚 Indexing Documents for RAG")
print("=" * 60)

try:
    retriever = get_rag_retriever()
    
    print("\n🔄 Processing documents in ai_agent/documents/...")
    chunk_count = retriever.index_documents(rebuild=True)
    
    print(f"\n✅ Successfully indexed {chunk_count} document chunks!")
    print("\nYou can now query your documents in the CLI.")
    print("=" * 60)
    
except Exception as e:
    print(f"\n❌ Error: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

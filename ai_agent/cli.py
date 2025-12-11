"""Simple CLI interface for testing the AI agent."""

import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
from services.agent import get_agent

# Load environment
load_dotenv()


def print_banner():
    """Print welcome banner."""
    print("\n" + "=" * 60)
    print("🤖 AI Agent CLI")
    print("=" * 60)
    print("\nQuery your data warehouses and documents in natural language!")
    print("\nAvailable databases:")
    print("  • snowflake - Historical analytical data")
    print("  • postgres  - Real-time streaming data")
    print("  • documents - PDF documentation (RAG)")
    print("  • auto      - Intelligent routing (default)")
    print("\nCommands:")
    print("  :help     - Show this help")
    print("  :exit     - Exit the CLI")
    print("  :health   - Check system health")
    print("  :db <db>  - Switch database (snowflake|postgres|documents|auto)")
    print("=" * 60 + "\n")


def check_health():
    """Check system health."""
    from services.sql_runner import get_sql_runner
    from services.rag_retriever import get_rag_retriever
    
    print("\n🔍 System Health Check\n")
    
    try:
        runner = get_sql_runner()
        status = runner.test_connections()
        
        print(f"Snowflake:   {'✅ Connected' if status['snowflake'] else '❌ Not connected'}")
        print(f"PostgreSQL:  {'✅ Connected' if status['postgres'] else '❌ Not connected'}")
        
        retriever = get_rag_retriever()
        doc_count = retriever.get_indexed_count()
        print(f"Documents:   ✅ {doc_count} chunks indexed")
        
        return True
    except Exception as e:
        print(f"❌ Health check failed: {str(e)}")
        return False


def main():
    """Main CLI loop."""
    print_banner()
    
    # Check environment
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ Error: OPENAI_API_KEY not found in environment")
        print("Please configure .env file first.")
        return
    
    # Initialize agent
    print("📦 Initializing agent...")
    try:
        agent = get_agent()
        print("✅ Agent ready!\n")
    except Exception as e:
        print(f"❌ Failed to initialize agent: {str(e)}")
        return
    
    # Default database
    current_db = "auto"
    
    # Main loop
    while True:
        try:
            # Get user input
            user_input = input(f"\n[{current_db}] Query: ").strip()
            
            if not user_input:
                continue
            
            # Handle commands
            if user_input.startswith(":"):
                cmd = user_input[1:].lower().split()
                
                if cmd[0] == "exit":
                    print("\n👋 Goodbye!")
                    break
                
                elif cmd[0] == "help":
                    print_banner()
                    continue
                
                elif cmd[0] == "health":
                    check_health()
                    continue
                
                elif cmd[0] == "db":
                    if len(cmd) < 2:
                        print("Usage: :db <snowflake|postgres|documents|auto>")
                    else:
                        db = cmd[1].lower()
                        if db in ["snowflake", "postgres", "documents", "auto"]:
                            current_db = db
                            print(f"✅ Switched to {current_db}")
                        else:
                            print("❌ Invalid database. Use: snowflake, postgres, documents, or auto")
                    continue
                
                else:
                    print(f"❌ Unknown command: {cmd[0]}")
                    print("Type :help for available commands")
                    continue
            
            # Execute query
            print(f"\n🔄 Querying {current_db}...")
            
            result = agent.answer_query(
                query=user_input,
                database=current_db,
                max_results=10
            )
            
            # Display results
            print(f"\n{'=' * 60}")
            print(f"📝 Answer:\n")
            print(result.get("answer", "No answer provided"))
            print(f"\n{'=' * 60}")
            
            # Show SQL if available
            if result.get("sql"):
                print(f"\n💾 SQL Query:")
                print(f"{result['sql']}\n")
            
            # Show data preview if available
            if result.get("data"):
                row_count = len(result["data"])
                print(f"\n📊 Results: {row_count} rows")
                
                if row_count > 0:
                    print("\nPreview (first 3 rows):")
                    for i, row in enumerate(result["data"][:3], 1):
                        print(f"{i}. {row}")
            
            # Show sources
            if result.get("sources"):
                print(f"\n📚 Sources:")
                for source in result["sources"]:
                    source_type = source.get("type", "unknown")
                    if source_type == "sql":
                        db = source.get("database", "unknown")
                        print(f"  • Database: {db}")
                    elif source_type == "document":
                        docs = source.get("documents", [])
                        print(f"  • Documents: {len(docs)} files")
            
            print(f"\n{'=' * 60}")
        
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        
        except Exception as e:
            print(f"\n❌ Error: {str(e)}")
            print("Try again or type :help for commands")


if __name__ == "__main__":
    main()

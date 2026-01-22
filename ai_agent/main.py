"""Main entrypoint for the AI Agent."""

import sys
import os
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from api.main import app
import uvicorn

if __name__ == "__main__":
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Get configuration
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    
    print(f"🚀 Starting AI Agent API on {host}:{port}")
    print(f"📚 Docs available at http://{host}:{port}/docs")
    
    # Run server
    uvicorn.run(
        "api.main:app",  # Import string for reload support
        host=host,
        port=port,
        reload=True,  # Enable auto-reload during development
        log_level="info"
    )

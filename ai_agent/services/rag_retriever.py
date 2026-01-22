"""RAG retrieval service for querying documents."""

from typing import List, Dict, Any
from langchain_core.documents import Document
from services.vector_store import get_vector_store
from services.pdf_loader import get_pdf_loader


class RAGRetriever:
    """Service for retrieving relevant documents using RAG."""
    
    def __init__(self, top_k: int = 4):
        """
        Initialize RAG retriever.
        
        Args:
            top_k: Number of documents to retrieve
        """
        self.top_k = top_k
        self.vector_store = get_vector_store()
        self.pdf_loader = get_pdf_loader()
    
    def retrieve(
        self,
        query: str,
        top_k: int = None,
        score_threshold: float = 0.0
    ) -> List[Dict[str, Any]]:
        """
        Retrieve relevant documents for a query.
        
        Args:
            query: Query text
            top_k: Number of documents to retrieve (overrides default)
            score_threshold: Minimum relevance score (default 0.0 to return all results)
            
        Returns:
            List of dictionaries with document content and metadata
        """
        k = top_k or self.top_k
        
        # Search for similar documents
        results = self.vector_store.similarity_search_with_score(
            query=query,
            k=k
        )
        
        # Filter by score threshold and format results
        retrieved_docs = []
        for doc, score in results:
            # For Chroma, score is distance - lower is better
            # We'll just use inverse for display purposes
            similarity = max(0, 2 - score)  # Scale so lower distances = higher similarity
            
            # Always include results unless score is extremely high
            if score < 10:  # Reasonable distance threshold
                retrieved_docs.append({
                    "content": doc.page_content,
                    "metadata": doc.metadata,
                    "similarity": similarity,
                    "source": doc.metadata.get("source", "unknown")
                })
        
        return retrieved_docs
    
    def format_context(self, documents: List[Dict[str, Any]]) -> str:
        """
        Format retrieved documents as context for LLM.
        
        Args:
            documents: List of retrieved documents
            
        Returns:
            Formatted context string
        """
        if not documents:
            return "No relevant documents found."
        
        context_parts = []
        for i, doc in enumerate(documents, 1):
            source = doc.get("source", "unknown")
            content = doc.get("content", "")
            similarity = doc.get("similarity", 0)
            
            context_parts.append(
                f"[Document {i} - {source} (relevance: {similarity:.2f})]\n{content}\n"
            )
        
        return "\n".join(context_parts)
    
    def index_documents(self, rebuild: bool = False) -> int:
        """
        Index all PDF documents in the vector store.
        
        Args:
            rebuild: If True, rebuild index from scratch
            
        Returns:
            Number of chunks indexed
        """
        print("Loading PDF documents...")
        documents = self.pdf_loader.process_all_pdfs()
        
        if rebuild:
            print("Rebuilding vector store...")
            self.vector_store.rebuild_index(documents)
        else:
            print("Adding documents to vector store...")
            self.vector_store.add_documents(documents)
        
        return len(documents)
    
    def get_indexed_count(self) -> int:
        """
        Get number of indexed documents.
        
        Returns:
            Number of documents in vector store
        """
        return self.vector_store.get_collection_count()


# Singleton instance
_rag_retriever = None


def get_rag_retriever() -> RAGRetriever:
    """Get or create RAG retriever singleton."""
    global _rag_retriever
    if _rag_retriever is None:
        _rag_retriever = RAGRetriever()
    return _rag_retriever

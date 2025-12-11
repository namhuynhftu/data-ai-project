"""Vector store service using ChromaDB for document storage and retrieval."""

import os
from typing import List, Optional
from pathlib import Path
from langchain_community.vectorstores import Chroma
from langchain_core.documents import Document
from services.embeddings import get_embedding_service


class VectorStoreService:
    """Service for managing vector database."""
    
    def __init__(self, persist_directory: str = None, collection_name: str = "documents"):
        """
        Initialize vector store service.
        
        Args:
            persist_directory: Directory to persist vector store
            collection_name: Name of the collection
        """
        self.persist_directory = persist_directory or os.getenv(
            "VECTOR_STORE_PATH", "./vector_store"
        )
        Path(self.persist_directory).mkdir(exist_ok=True)
        
        self.collection_name = collection_name
        self.embeddings = get_embedding_service().embeddings
        
        # Initialize or load vector store
        self.vector_store = Chroma(
            collection_name=collection_name,
            embedding_function=self.embeddings,
            persist_directory=self.persist_directory
        )
    
    def add_documents(self, documents: List[Document]) -> List[str]:
        """
        Add documents to vector store.
        
        Args:
            documents: List of Document objects
            
        Returns:
            List of document IDs
        """
        if not documents:
            return []
        
        # Add documents to vector store
        ids = self.vector_store.add_documents(documents)
        
        # Persist changes
        self.vector_store.persist()
        
        print(f"Added {len(documents)} documents to vector store")
        return ids
    
    def similarity_search(
        self,
        query: str,
        k: int = 4,
        filter: Optional[dict] = None
    ) -> List[Document]:
        """
        Search for similar documents.
        
        Args:
            query: Query text
            k: Number of results to return
            filter: Metadata filter
            
        Returns:
            List of similar Documents
        """
        return self.vector_store.similarity_search(
            query=query,
            k=k,
            filter=filter
        )
    
    def similarity_search_with_score(
        self,
        query: str,
        k: int = 4,
        filter: Optional[dict] = None
    ) -> List[tuple]:
        """
        Search for similar documents with relevance scores.
        
        Args:
            query: Query text
            k: Number of results to return
            filter: Metadata filter
            
        Returns:
            List of (Document, score) tuples
        """
        return self.vector_store.similarity_search_with_score(
            query=query,
            k=k,
            filter=filter
        )
    
    def delete_collection(self):
        """Delete the entire collection."""
        self.vector_store.delete_collection()
        print(f"Deleted collection: {self.collection_name}")
    
    def get_collection_count(self) -> int:
        """
        Get number of documents in collection.
        
        Returns:
            Number of documents
        """
        try:
            collection = self.vector_store._collection
            return collection.count()
        except Exception:
            return 0
    
    def rebuild_index(self, documents: List[Document]):
        """
        Rebuild vector store from scratch.
        
        Args:
            documents: List of Document objects
        """
        # Delete existing collection
        self.delete_collection()
        
        # Reinitialize vector store
        self.vector_store = Chroma(
            collection_name=self.collection_name,
            embedding_function=self.embeddings,
            persist_directory=self.persist_directory
        )
        
        # Add documents
        self.add_documents(documents)


# Singleton instance
_vector_store = None


def get_vector_store() -> VectorStoreService:
    """Get or create vector store singleton."""
    global _vector_store
    if _vector_store is None:
        _vector_store = VectorStoreService()
    return _vector_store

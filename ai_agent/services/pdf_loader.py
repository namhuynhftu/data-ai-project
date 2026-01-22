"""PDF document loading and processing service."""

import os
from typing import List, Dict, Any
from pathlib import Path
from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document


class PDFLoaderService:
    """Service for loading and processing PDF documents."""
    
    def __init__(
        self,
        documents_path: str = None,
        chunk_size: int = 1000,
        chunk_overlap: int = 200
    ):
        """
        Initialize PDF loader service.
        
        Args:
            documents_path: Path to PDF documents directory
            chunk_size: Size of text chunks for splitting
            chunk_overlap: Overlap between chunks
        """
        self.documents_path = Path(
            documents_path or os.getenv("DOCUMENTS_PATH", "./documents")
        )
        self.documents_path.mkdir(exist_ok=True)
        
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=len,
            separators=["\n\n", "\n", " ", ""]
        )
    
    def load_pdf(self, pdf_path: str) -> List[Document]:
        """
        Load a single PDF file.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            List of Document objects
        """
        loader = PyPDFLoader(pdf_path)
        documents = loader.load()
        
        # Add metadata
        for doc in documents:
            doc.metadata["source_file"] = Path(pdf_path).name
            doc.metadata["file_path"] = pdf_path
        
        return documents
    
    def load_all_pdfs(self) -> List[Document]:
        """
        Load all PDF files from documents directory.
        
        Returns:
            List of all Document objects
        """
        all_documents = []
        
        for pdf_file in self.documents_path.glob("*.pdf"):
            print(f"Loading: {pdf_file.name}")
            try:
                documents = self.load_pdf(str(pdf_file))
                all_documents.extend(documents)
            except Exception as e:
                print(f"Error loading {pdf_file.name}: {e}")
        
        return all_documents
    
    def split_documents(self, documents: List[Document]) -> List[Document]:
        """
        Split documents into smaller chunks.
        
        Args:
            documents: List of Document objects
            
        Returns:
            List of split Document objects
        """
        return self.text_splitter.split_documents(documents)
    
    def process_pdf(self, pdf_path: str) -> List[Document]:
        """
        Load and split a PDF file.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            List of split Document objects
        """
        documents = self.load_pdf(pdf_path)
        return self.split_documents(documents)
    
    def process_all_pdfs(self) -> List[Document]:
        """
        Load and split all PDF files.
        
        Returns:
            List of all split Document objects
        """
        documents = self.load_all_pdfs()
        return self.split_documents(documents)
    
    def get_pdf_info(self) -> Dict[str, Any]:
        """
        Get information about available PDFs.
        
        Returns:
            Dictionary with PDF statistics
        """
        pdf_files = list(self.documents_path.glob("*.pdf"))
        
        return {
            "documents_path": str(self.documents_path),
            "pdf_count": len(pdf_files),
            "pdf_files": [f.name for f in pdf_files]
        }


# Singleton instance
_pdf_loader = None


def get_pdf_loader() -> PDFLoaderService:
    """Get or create PDF loader singleton."""
    global _pdf_loader
    if _pdf_loader is None:
        _pdf_loader = PDFLoaderService()
    return _pdf_loader

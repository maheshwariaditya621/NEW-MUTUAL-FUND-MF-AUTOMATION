"""
Base extractor abstract class.

Defines the interface for AMC-specific extractors.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Tuple


class BaseExtractor(ABC):
    """
    Abstract base class for AMC-specific extractors.
    
    Each AMC will have its own extractor class that inherits from this base class
    and implements the abstract methods.
    
    Example:
        class HDFCExtractor(BaseExtractor):
            def get_amc_name(self) -> str:
                return "HDFC Mutual Fund"
            
            def extract_scheme_metadata(self, file_path: str) -> Dict[str, Any]:
                # Extract scheme metadata from Excel file
                ...
            
            def extract_holdings(self, file_path: str) -> List[Dict[str, Any]]:
                # Extract holdings from Excel file
                ...
    """
    
    @abstractmethod
    def get_amc_name(self) -> str:
        """
        Get the canonical AMC name.
        
        Returns:
            Canonical AMC name (e.g., "HDFC Mutual Fund")
        """
        pass
    
    @abstractmethod
    def extract_scheme_metadata(self, file_path: str) -> Dict[str, Any]:
        """
        Extract scheme metadata from file.
        
        Args:
            file_path: Path to source file (Excel, PDF, etc.)
            
        Returns:
            Dictionary with keys:
                - scheme_name: str (canonical, without plan/option suffixes)
                - plan_type: str ("Direct" or "Regular")
                - option_type: str ("Growth", "Dividend", or "IDCW")
                - year: int
                - month: int
                - scheme_category: str (optional)
                - scheme_code: str (optional)
        """
        pass
    
    @abstractmethod
    def extract_holdings(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract equity holdings from file.
        
        Args:
            file_path: Path to source file (Excel, PDF, etc.)
            
        Returns:
            List of holding dictionaries with keys:
                - isin: str
                - company_name: str
                - quantity: int
                - market_value_inr: float/Decimal
                - percent_of_nav: float/Decimal
                - exchange_symbol: str (optional)
                - sector: str (optional)
                - industry: str (optional)
        """
        pass
    
    def extract_all(self, file_path: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Extract both metadata and holdings from file.
        
        Args:
            file_path: Path to source file
            
        Returns:
            Tuple of (metadata, holdings)
        """
        metadata = self.extract_scheme_metadata(file_path)
        holdings = self.extract_holdings(file_path)
        
        return metadata, holdings

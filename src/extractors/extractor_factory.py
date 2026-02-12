from datetime import datetime
from typing import Optional
from src.extractors.base_extractor import BaseExtractor
from src.extractors.hdfc_extractor_v1 import HDFCExtractorV1
from src.extractors.icici_extractor_v1 import ICICIExtractorV1
from src.extractors.generic_extractor import GenericExtractor

class ExtractorFactory:
    """
    Factory to return the appropriate version of an AMC extractor
    based on the processing month/year.
    """

    @staticmethod
    def get_extractor(amc_slug: str, year: int, month: int) -> Optional[BaseExtractor]:
        """
        Returns the extractor instance for the given AMC and date.
        """
        amc_slug = amc_slug.lower()
        
        # HDFC Versioning Logic
        if amc_slug == "hdfc":
            return HDFCExtractorV1()

        if amc_slug == "sbi":
            return SBIExtractorV1()

        if amc_slug in ["icici", "icici_pru"]:
            return ICICIExtractorV1()

        # Add other AMCs as they are implemented
        # if amc_slug == "icici":
        #     return ICICIExtractorV1()

        return None

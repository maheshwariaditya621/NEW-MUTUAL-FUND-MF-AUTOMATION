
from typing import Optional
from src.extractors.base_extractor import BaseExtractor
from src.extractors.hdfc_extractor_v1 import HDFCExtractorV1
from src.extractors.icici_extractor_v1 import ICICIExtractorV1
from src.extractors.sbi_extractor_v1 import SBIExtractorV1
from src.extractors.hsbc_extractor_v1 import HSBCExtractorV1
from src.extractors.kotak_extractor_v1 import KotakExtractorV1
from src.extractors.ppfas_extractor_v1 import PPFASExtractorV1
from src.extractors.axis_extractor_v1 import AxisExtractorV1
from src.extractors.bajaj_extractor_v1 import BajajExtractorV1
from src.extractors.absl_extractor_v1 import ABSLExtractorV1
from src.extractors.angelone_extractor_v1 import AngelOneExtractorV1
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

        if amc_slug == "hsbc":
            return HSBCExtractorV1()

        if amc_slug == "kotak":
            return KotakExtractorV1()
            
        if amc_slug == "ppfas":
            return PPFASExtractorV1()
            
        if amc_slug == "axis":
            return AxisExtractorV1()

        if amc_slug == "bajaj":
            return BajajExtractorV1()

        if amc_slug == "absl":
            return ABSLExtractorV1()

        if amc_slug == "angelone":
            return AngelOneExtractorV1()

        return None

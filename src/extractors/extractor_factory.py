
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
from src.extractors.nippon_extractor_v1 import NipponExtractorV1
from src.extractors.mirae_extractor_v1 import MiraeExtractorV1
from src.extractors.motilal_extractor_v1 import MotilalExtractorV1
from src.extractors.quant_extractor_v1 import QuantExtractorV1
from src.extractors.quantum_extractor_v1 import QuantumExtractorV1
from src.extractors.common_extractor_v1 import CommonExtractorV1


ADDITIONAL_AMC_NAMES = {
    "bandhan": "Bandhan Mutual Fund",
    "baroda": "Baroda BNP Paribas Mutual Fund",
    "boi": "Bank of India Mutual Fund",
    "canara": "Canara Robeco Mutual Fund",
    "capitalmind": "Capitalmind Mutual Fund",
    "choice": "Choice Mutual Fund",
    "dsp": "DSP Mutual Fund",
    "edelweiss": "Edelweiss Mutual Fund",
    "franklin": "Franklin Templeton Mutual Fund",
    "groww": "Groww Mutual Fund",
    "helios": "Helios Mutual Fund",
    "invesco": "Invesco Mutual Fund",
    "iti": "ITI Mutual Fund",
    "jio_br": "Jio BlackRock Mutual Fund",
    "jmfinancial": "JM Financial Mutual Fund",
    "lic": "LIC Mutual Fund",
    "mahindra": "Mahindra Manulife Mutual Fund",
    "mirae_asset": "Mirae Asset Mutual Fund",
    "motilal": "Motilal Oswal Mutual Fund",
    "navi": "Navi Mutual Fund",
    "nippon": "Nippon India Mutual Fund",
    "nj": "NJ Mutual Fund",
    "old_bridge": "Old Bridge Mutual Fund",
    "pgim_india": "PGIM India Mutual Fund",
    "quant": "Quant Mutual Fund",
    "quantum": "Quantum Mutual Fund",
    "samco": "Samco Mutual Fund",
    "sundaram": "Sundaram Mutual Fund",
    "tata": "Tata Mutual Fund",
    "taurus": "Taurus Mutual Fund",
    "threesixtyone": "360 ONE Mutual Fund",
    "trust": "Trust Mutual Fund",
    "unifi": "Unifi Mutual Fund",
    "union": "Union Mutual Fund",
    "uti": "UTI Mutual Fund",
    "wealth_company": "The Wealth Company Mutual Fund",
    "whiteoak": "WhiteOak Capital Mutual Fund",
}

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

        if amc_slug == "nippon":
            return NipponExtractorV1()

        if amc_slug == "mirae_asset":
            return MiraeExtractorV1()

        if amc_slug == "motilal":
            return MotilalExtractorV1()
        
        if amc_slug == "quant":
            return QuantExtractorV1()
        
        if amc_slug == "quantum":
            return QuantumExtractorV1()

        if amc_slug in ADDITIONAL_AMC_NAMES:
            # Default to CommonExtractorV1 if no dedicated version exists
            return CommonExtractorV1(amc_slug=amc_slug, amc_name=ADDITIONAL_AMC_NAMES[amc_slug])

        return None

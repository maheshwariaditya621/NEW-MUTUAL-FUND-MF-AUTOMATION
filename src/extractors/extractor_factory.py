
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
from src.extractors.edelweiss_extractor_v1 import EdelweissExtractorV1
from src.extractors.lic_extractor_v1 import LICExtractorV1
from src.extractors.tata_extractor_v1 import TataExtractorV1
from src.extractors.bandhan_extractor_v1 import BandhanExtractorV1
from src.extractors.baroda_extractor_v1 import BarodaExtractorV1
from src.extractors.canara_extractor_v1 import CanaraExtractorV1
from src.extractors.boi_extractor_v1 import BOIExtractorV1
from src.extractors.capitalmind_extractor_v1 import CapitalmindExtractorV1
from src.extractors.choice_extractor_v1 import ChoiceExtractorV1
from src.extractors.dsp_extractor_v1 import DSPExtractorV1
from src.extractors.franklin_extractor_v1 import FranklinExtractorV1
from src.extractors.groww_extractor_v1 import GrowwExtractorV1
from src.extractors.helios_extractor_v1 import HeliosExtractorV1
from src.extractors.invesco_extractor_v1 import InvescoExtractorV1
from src.extractors.iti_extractor_v1 import ITIExtractorV1
from src.extractors.jio_br_extractor_v1 import JioBRExtractorV1
from src.extractors.jm_financial_extractor_v1 import JMFinancialExtractorV1
from src.extractors.mahindra_manulife_extractor_v1 import MahindraManulifeExtractorV1
from src.extractors.navi_extractor_v1 import NaviExtractorV1
from src.extractors.nj_extractor_v1 import NJExtractorV1
from src.extractors.old_bridge_extractor_v1 import OldBridgeExtractorV1
from src.extractors.pgim_india_extractor_v1 import PGIMIndiaExtractorV1
from src.extractors.samco_extractor_v1 import SamcoExtractorV1
from src.extractors.sundaram_extractor_v1 import SundaramExtractorV1
from src.extractors.taurus_extractor_v1 import TaurusExtractorV1
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

        if amc_slug == "navi":
            return NaviExtractorV1()

        if amc_slug == "nj":
            return NJExtractorV1()

        if amc_slug == "old_bridge":
            return OldBridgeExtractorV1()

        if amc_slug == "pgim_india":
            return PGIMIndiaExtractorV1()

        if amc_slug == "samco":
            return SamcoExtractorV1()

        if amc_slug == "sundaram":
            return SundaramExtractorV1()

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

        EXTRACTOR_MAP = {
            "hdfc": lambda: HDFCExtractorV1(),
            "sbi": lambda: SBIExtractorV1(),
            "icici": lambda: ICICIExtractorV1(),
            "icici_pru": lambda: ICICIExtractorV1(),
            "hsbc": lambda: HSBCExtractorV1(),
            "kotak": lambda: KotakExtractorV1(),
            "ppfas": lambda: PPFASExtractorV1(),
            "axis": lambda: AxisExtractorV1(),
            "bajaj": lambda: BajajExtractorV1(),
            "absl": lambda: ABSLExtractorV1(),
            "angelone": lambda: AngelOneExtractorV1(),
            "nippon": lambda: NipponExtractorV1(),
            "mirae_asset": lambda: MiraeExtractorV1(),
            "motilal": lambda: MotilalExtractorV1(),
            "quant": lambda: QuantExtractorV1(),
            "quantum": lambda: QuantumExtractorV1(),
            "edelweiss": lambda: EdelweissExtractorV1(),
            "lic": lambda: LICExtractorV1(),
            "tata": lambda: TataExtractorV1(),
            "bandhan": lambda: BandhanExtractorV1(),
            "baroda": lambda: BarodaExtractorV1(),
            "canara": lambda: CanaraExtractorV1(),
            "boi": lambda: BOIExtractorV1(),
            "capitalmind": lambda: CapitalmindExtractorV1(),
            "choice": lambda: ChoiceExtractorV1(),
            "dsp": lambda: DSPExtractorV1(),
            "franklin": lambda: FranklinExtractorV1(),
            "groww": GrowwExtractorV1,
            "helios": HeliosExtractorV1,
    "invesco": InvescoExtractorV1,
    "iti": ITIExtractorV1,
    "jio_br": JioBRExtractorV1,
    "jmfinancial": JMFinancialExtractorV1,
    "mahindra": MahindraManulifeExtractorV1,
    "taurus": TaurusExtractorV1,
}

        extractor_func = EXTRACTOR_MAP.get(amc_slug)
        if extractor_func:
            return extractor_func()

        if amc_slug in ADDITIONAL_AMC_NAMES:
            # Default to CommonExtractorV1 if no dedicated version exists
            return CommonExtractorV1(amc_slug=amc_slug, amc_name=ADDITIONAL_AMC_NAMES[amc_slug])

        return None

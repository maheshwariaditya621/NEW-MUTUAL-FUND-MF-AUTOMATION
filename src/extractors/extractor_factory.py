
from typing import Optional
from src.extractors.base_extractor import BaseExtractor
from src.config.constants import AMC_WEALTH
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
from src.extractors.threesixtyone_extractor_v1 import ThreeSixtyOneExtractorV1
from src.extractors.trust_extractor_v1 import TrustExtractorV1
from src.extractors.unifi_extractor_v1 import UnifiExtractorV1
from src.extractors.union_extractor_v1 import UnionExtractorV1
from src.extractors.uti_extractor_v1 import UTIExtractorV1
from src.extractors.wealth_extractor_v1 import WealthExtractorV1
from src.extractors.whiteoak_extractor_v1 import WhiteOakExtractorV1
from src.extractors.common_extractor_v1 import CommonExtractorV1


ADDITIONAL_AMC_NAMES = {
    "abakkus": "ABAKKUS MUTUAL FUND",
    "bandhan": "BANDHAN MUTUAL FUND",
    "baroda": "BARODA BNP PARIBAS MUTUAL FUND",
    "boi": "BANK OF INDIA MUTUAL FUND",
    "canara": "CANARA ROBECO MUTUAL FUND",
    "capitalmind": "CAPITALMIND MUTUAL FUND",
    "choice": "CHOICE MUTUAL FUND",
    "dsp": "DSP MUTUAL FUND",
    "edelweiss": "EDELWEISS MUTUAL FUND",
    "franklin": "FRANKLIN TEMPLETON MUTUAL FUND",
    "groww": "GROWW MUTUAL FUND",
    "helios": "HELIOS MUTUAL FUND",
    "invesco": "INVESCO MUTUAL FUND",
    "iti": "ITI MUTUAL FUND",
    "jio_br": "JIO BLACKROCK MUTUAL FUND",
    "jmfinancial": "JM FINANCIAL MUTUAL FUND",
    "lic": "LIC MUTUAL FUND",
    "mahindra": "MAHINDRA MANULIFE MUTUAL FUND",
    "mirae_asset": "MIRAE ASSET MUTUAL FUND",
    "motilal": "MOTILAL OSWAL MUTUAL FUND",
    "navi": "NAVI MUTUAL FUND",
    "nippon": "NIPPON INDIA MUTUAL FUND",
    "nj": "NJ MUTUAL FUND",
    "old_bridge": "OLD BRIDGE MUTUAL FUND",
    "pgim_india": "PGIM INDIA MUTUAL FUND",
    "quant": "QUANT MUTUAL FUND",
    "quantum": "QUANTUM MUTUAL FUND",
    "samco": "SAMCO MUTUAL FUND",
    "sundaram": "SUNDARAM MUTUAL FUND",
    "tata": "TATA MUTUAL FUND",
    "taurus": "TAURUS MUTUAL FUND",
    "threesixtyone": "360 ONE MUTUAL FUND",
    "trust": "TRUST MUTUAL FUND",
    "unifi": "UNIFI MUTUAL FUND",
    "union": "UNION MUTUAL FUND",
    "uti": "UTI MUTUAL FUND",
    "wealth_company": AMC_WEALTH,
    "whiteoak": "WHITEOAK CAPITAL MUTUAL FUND",
}

from src.extractors.abakkus_extractor_v1 import AbakkusExtractorV1

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
        
        if amc_slug == "abakkus":
            return AbakkusExtractorV1()
        
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
    "threesixtyone": ThreeSixtyOneExtractorV1,
    "trust": TrustExtractorV1,
    "unifi": UnifiExtractorV1,
    "union": UnionExtractorV1,
    "uti": UTIExtractorV1,
    "wealth_company": WealthExtractorV1,
    "whiteoak": WhiteOakExtractorV1,
}

        extractor_func = EXTRACTOR_MAP.get(amc_slug)
        if extractor_func:
            return extractor_func()

        if amc_slug in ADDITIONAL_AMC_NAMES:
            # Default to CommonExtractorV1 if no dedicated version exists
            return CommonExtractorV1(amc_slug=amc_slug, amc_name=ADDITIONAL_AMC_NAMES[amc_slug])

        return None

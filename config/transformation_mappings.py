"""
Data transformation mappings and lookups.
Manages canonical commodity names, market identifiers, and localization.
"""

from typing import Dict, Optional

# Crop/Commodity name mapping: Ukrainian/raw → Canonical English name
CROP_NAME_MAP: Dict[str, str] = {
    # GrainTrade UA - crop field (Ukrainian, case-insensitive match)
    "пшениця": "Wheat",
    "пшениця 4 клас": "Wheat",
    "пшениця 3 клас": "Wheat",
    "кукурудза": "Corn",
    "соя": "Soybeans",
    "насіння соняшника": "Sunflower",
    "ріпак": "Canola",
    "ячмінь": "Barley",
    
    # Tripoli Land - category_name (English and Ukrainian)
    "wheat 3rd grade": "Wheat",
    "wheat": "Wheat",
    "corn": "Corn",
    "barley": "Barley",
    "soybeans": "Soybeans",
    "sunflower": "Sunflower",
    "canola": "Canola",
    
    # YFinance - common commodity names (from CBOT futures descriptions)
    "wheat": "Wheat",
    "corn": "Corn",
    "soybeans": "Soybeans",
    "oats": "Oats",
    "rough rice": "Rice",
}

# YFinance ticker → commodity mapping for futures
YFINANCE_TICKER_MAP: Dict[str, Dict[str, str]] = {
    "ZW=F": {"name": "Wheat", "unit": "ton", "category": "futures"},
    "ZC=F": {"name": "Corn", "unit": "ton", "category": "futures"},
    "ZS=F": {"name": "Soybeans", "unit": "ton", "category": "futures"},
    "ZO=F": {"name": "Oats", "unit": "ton", "category": "futures"},
    "ZR=F": {"name": "Rice", "unit": "ton", "category": "futures"},
}

# Market name mappings by source
MARKET_MAPPINGS: Dict[str, Dict[str, str]] = {
    "yfinance": {
        "name_template": "CBOT {commodity}",  # e.g., "CBOT Wheat"
        "exchange": "CBOT",
        "country": "US",
        "timezone": "America/Chicago",
    },
    "graintradecomua": {
        "name": "GrainTrade UA",
        "exchange": "GrainTrade",
        "country": "Ukraine",
        "timezone": "Europe/Kyiv",
    },
    "tripoli_land": {
        "name_template": "{company} - {location}",  # e.g., "Nibulon - Mykolaiv Terminal"
        "exchange": "Tripoli Land",
        "country": "Ukraine",
        "timezone": "Europe/Kyiv",
    },
}

# Delivery term standardization
DELIVERY_TERM_NORMALIZE: Dict[str, str] = {
    "FCA": "FCA",  # Free Carrier
    "CPT": "CPT",  # Carriage and Insurance Paid To
    "CIF": "CIF",  # Cost, Insurance and Freight
    "FOB": "FOB",  # Free on Board
}

# Price type classification
PRICE_TYPE: Dict[str, str] = {
    "yfinance": "futures_close",
    "graintradecomua": "spot",
    "tripoli_land": "bid",
    "currency": "fx_rate",
}


def normalize_crop_name(crop_name: Optional[str]) -> Optional[str]:
    """
    Normalize crop/commodity name to canonical form.
    
    Args:
        crop_name: Raw crop name (Ukrainian, English, or mixed)
    
    Returns:
        Canonical commodity name (English), or None if not recognized
    
    Example:
        normalize_crop_name("пшениця") → "Wheat"
        normalize_crop_name("corn") → "Corn"
    """
    if not crop_name:
        return None
    
    # Exact match first
    normalized = crop_name.lower().strip()
    if normalized in CROP_NAME_MAP:
        return CROP_NAME_MAP[normalized]
    
    # Substring match for longer names
    for key, value in CROP_NAME_MAP.items():
        if key in normalized or normalized in key:
            return value
    
    return None


def get_market_name(source: str, **kwargs) -> str:
    """
    Generate market name from source and attributes.
    
    Args:
        source: Data source identifier ('yfinance', 'graintradecomua', 'tripoli_land')
        **kwargs: Context-specific fields (commodity, company, location, etc.)
    
    Returns:
        Standardized market name
    
    Example:
        get_market_name("yfinance", commodity="Wheat") → "CBOT Wheat"
        get_market_name("tripoli_land", company="Nibulon", location="Mykolaiv Terminal")
            → "Nibulon - Mykolaiv Terminal"
    """
    mapping = MARKET_MAPPINGS.get(source, {})
    
    if "name_template" in mapping:
        return mapping["name_template"].format(**kwargs)
    elif "name" in mapping:
        return mapping["name"]
    
    return f"{source.replace('_', ' ').title()}"


def get_market_info(source: str) -> Dict[str, str]:
    """
    Get standard market info for a source.
    
    Args:
        source: Data source identifier
    
    Returns:
        Dict with exchange, country, timezone
    """
    mapping = MARKET_MAPPINGS.get(source, {})
    return {
        "exchange": mapping.get("exchange", source),
        "country": mapping.get("country", ""),
        "timezone": mapping.get("timezone", "UTC"),
    }


def get_price_type(source: str) -> str:
    """
    Get price type classification for a source.
    
    Args:
        source: Data source identifier
    
    Returns:
        Price type ('futures_close', 'spot', 'bid', 'fx_rate')
    """
    return PRICE_TYPE.get(source, "spot")

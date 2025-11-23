from dataclasses import dataclass
from typing import Optional, List

@dataclass
class PriceSuggestionResponse:
    sku: str
    current_price: float
    suggested_price: float
    expected_revenue: float
    expected_profit: float
    elasticity: float
    confidence: float
    reason: str
    actions: List[str]

@dataclass
class PriceFeedbackRequest:
    vendor_id: str
    sku: str
    suggested_price: float
    action: str
    custom_price: Optional[float]
    timestamp: str

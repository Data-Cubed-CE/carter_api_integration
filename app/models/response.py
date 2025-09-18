from pydantic import BaseModel, Field, validator
from datetime import datetime, date
from typing import Any, Dict, List, Literal, Optional
from decimal import Decimal

# ISO 4217 Currency Codes (common ones)
VALID_CURRENCIES = {
    'USD', 'EUR', 'GBP', 'PLN'
}

class Offer(BaseModel):
    """
    Standardized hotel offer from any provider.
    
    Simplified model containing only fields actually used in production responses.
    All provider responses are normalized to this unified format.
    """
    
    # Provider Information
    provider: str = Field(..., description="Provider name (rate_hawk, goglobal, tbo)", example="rate_hawk")
    
    # Hotel Identification  
    supplier_hotel_id: Optional[str] = Field(None, description="Provider's internal hotel ID")
    hotel_id: Optional[str] = Field(None, description="Standardized hotel ID") 
    hotel_name: Optional[str] = Field(None, description="Hotel name as provided by supplier")
    
    # Room Information
    supplier_room_code: Optional[str] = Field(None, description="Provider's room type/rate code")
    room_name: Optional[str] = Field(None, description="Human-readable room description")
    room_category: Optional[str] = Field(None, description="Room category (Standard/Premium/Suite/Other)", example="Suite")
    room_mapping_id: Optional[str] = Field(None, description="Unique room mapping identifier")
    
    # Pricing
    total_price: Optional[Decimal] = Field(None, description="Total stay price", example=Decimal("3300.0"))
    currency: Optional[str] = Field(None, description="Price currency code (ISO 4217)", example="EUR")
    
    # Booking Details
    meal_plan: Optional[str] = Field(None, description="Meal plan code (BB/HB/AI/RO/etc)", example="RO")
    free_cancellation_until: Optional[str] = Field(None, description="Free cancellation deadline", example="2025-12-21T11:00:00")
    
    # Room Features & Amenities
    room_features: Optional[List[str]] = Field(None, description="Room features list", example=["bathroom", "internet", "king bed"])
    amenities: Optional[List[str]] = Field(None, description="Hotel/room amenities", example=["non-smoking", "king-bed"])
    
    @validator('currency')
    def validate_currency(cls, v):
        """Validate currency against ISO 4217 codes"""
        if v is not None and v.upper() not in VALID_CURRENCIES:
            raise ValueError(f"Invalid currency code: {v}. Must be a valid ISO 4217 code.")
        return v.upper() if v else None

    class Config:
        json_schema_extra = {
            "example": {
                "provider": "rate_hawk",
                "supplier_hotel_id": "banyan_tree_krabi_7", 
                "hotel_id": "13100360",
                "hotel_name": "Banyan Tree Krabi",
                "supplier_room_code": "m-81376b0f-276f-512d-a1eb-c71ee57465cb",
                "room_name": "Wellbeing Sanctuary Double Pool Suite (king size bed)",
                "room_category": "Suite",
                "room_mapping_id": None,
                "total_price": 3300.0,
                "currency": "EUR", 
                "meal_plan": "RO",
                "free_cancellation_until": "2025-12-21T11:00:00",
                "room_features": ["bathroom", "internet", "king bed"],
                "amenities": ["non-smoking", "king-bed"]
            }
        }

class ProviderResult(BaseModel):
    """Individual provider search result with status and data."""
    
    status: Literal["success", "error", "skipped"] = Field(..., description="Provider execution status")
    data: Optional[List[Offer]] = Field(None, description="List of hotel offers (null if error/skipped)")
    error: Optional[str] = Field(None, description="Error message if status is 'error'")

class MetaInfo(BaseModel):
    """Request metadata and processing statistics."""
    
    request_id: str = Field(..., description="Unique request identifier", example="req_1758031923_dae4ec9b")
    timestamp: datetime = Field(..., description="Request start time", example="2025-09-16T16:12:10.974682")
    total_providers: int = Field(..., description="Number of providers queried", example=3)
    successful_providers: int = Field(..., description="Number of providers that returned results", example=3)
    total_results: int = Field(..., description="Total offers across all providers", example=4) 
    processing_time_ms: float = Field(..., description="Total processing time in milliseconds", example=7675.376)
    provider_breakdown: Optional[Dict[str, Dict[str, Any]]] = Field(
        None, 
        description="Detailed breakdown of results per provider",
        example={
            "rate_hawk": {"status": "success", "offers_count": 4, "processing_time_ms": 7568},
            "goglobal": {"status": "success", "offers_count": 0, "processing_time_ms": 1029}
        }
    )



class HotelSearchResponse(BaseModel):
    """
    Complete hotel search response with aggregated results.
    
    Contains all search results and metadata from multiple providers.
    """
    
    meta: MetaInfo = Field(..., description="Request metadata and processing statistics")
    search_criteria: Dict[str, Any] = Field(..., description="Echo of submitted search parameters")
    data: List[Offer] = Field(..., description="All hotel offers from all providers")
    user: Optional[str] = Field(None, description="System user who made the request", example="user83njnk3k")

    class Config:
        json_schema_extra = {
            "example": {
                "meta": {
                    "request_id": "search_1721865432",
                    "timestamp": "2025-07-15T10:30:32Z",
                    "total_providers": 2,
                    "successful_providers": 2,
                    "total_results": 15,
                    "processing_time_ms": 3250
                },
                "search_criteria": {
                    "hotel_name": "Banyan Tree Krabi",
                    "check_in": "2025-08-15",
                    "check_out": "2025-08-18",
                    "adults": 2,
                    "children_ages": [10, 12]
                },
                "data": [
                    {
                        "provider": "rate_hawk",
                        "supplier_hotel_id": "12345",
                        "hotel_name": "Banyan Tree Krabi",
                        "total_price": 1250.00,
                        "currency": "EUR",
                        "room_name": "Deluxe Pool Villa",
                        "meal_plan": "BB"
                    },
                    {
                        "provider": "goglobal",
                        "supplier_hotel_id": "67890", 
                        "hotel_name": "Banyan Tree Krabi",
                        "total_price": 1180.00,
                        "currency": "EUR",
                        "room_name": "Premium Ocean Villa",
                        "meal_plan": "HB"
                    }
                ],
                "user": "user83njnk3k"
            }
        }

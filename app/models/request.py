from pydantic import BaseModel, Field
from datetime import date
from typing import Optional, Literal, List


class HotelSearchRequest(BaseModel):
    """
    Hotel search request model with comprehensive validation.

    **Required Fields:**
    - hotel_names: List of exact hotel names from mappings endpoint (min 1, max 10)
    - check_in/check_out: Future dates with minimum 1 night stay
    - adults: Number of adult guests (1-10)

    **Optional Fields:**
    - children_ages: List of child ages (0-17 years)
    - nationality: Guest nationality for pricing
    - currency: Preferred currency for results
    - meal_types: Meal plan filters (BB, AI, HB, etc.)
    - providers: List of specific providers to search
    """

    hotel_names: List[str] = Field(
        ...,
        example=["Banyan Tree Krabi"],
        description="List of hotel names as returned by /hotels/mappings endpoint. For single hotel: ['Hotel Name']. For multiple hotels: ['Hotel 1', 'Hotel 2', 'Hotel 3']",
        min_items=1,
        max_items=10
    )

    city: Optional[str] = Field(
        None,
        example="Krabi",
        description="City name for reference (optional, hotel mapping takes precedence)",
        max_length=999999
    )

    check_in: date = Field(
        ...,
        example="2025-08-15",
        description="Check-in date (YYYY-MM-DD format, must be future date)"
    )

    check_out: date = Field(
        ...,
        example="2025-08-18",
        description="Check-out date (YYYY-MM-DD format, must be after check-in)"
    )

    adults: int = Field(
        ...,
        example=2,
        ge=1,
        le=10,
        description="Number of adult guests (minimum 1, maximum 10)"
    )

    children_ages: Optional[List[int]] = Field(
        None,
        example=[10, 12],
        description="List of ages for each child (0-17 years). Length determines children count.",
        max_items=10
    )

    rooms: Optional[int] = Field(
        None,
        example=1,
        ge=1,
        le=5,
        description="Number of rooms requested (defaults to 1)"
    )

    room_category: Optional[Literal["Standard", "Premium", "Apartament", "Other"]] = Field(
        None,
        example="Standard",
        description="Preferred room category for filtering results. Matches room_category from response."
    )

    nationality: Optional[str] = Field(
        "PL",
        example="PL",
        description="ISO 3166-1 alpha-2 nationality code for pricing optimization",
        min_length=2,
        max_length=2,
        pattern=r"^[A-Z]{2}$"
    )

    currency: Optional[str] = Field(
        "EUR",
        example="EUR",
        description="ISO 4217 currency code for price display",
        min_length=3,
        max_length=3,
        pattern=r"^[A-Z]{3}$"
    )

    meal_types: Optional[List[str]] = Field(
        None,
        example=["BB", "HB", "AI"],
        description="List of meal plan filters: BB (Bed & Breakfast), HB (Half Board), HBD (Half Board Dinner), AI (All Inclusive), FB (Full Board), RO (Room Only), etc. Returns offers matching ANY of the specified meal types. See /meal-types endpoint for complete list."
    )

    providers: Optional[List[str]] = Field(
        None,
        example=["rate_hawk", "tbo", "goglobal"],
        description="List of specific providers to search. Available options: 'rate_hawk', 'tbo', 'goglobal'. If empty or not provided, all available providers will be used for search.",
        max_items=10
    )

    user: Optional[str] = Field(
        None,
        example="user97kw86", 
        description="System user making the request")
    
    # Computed field - number of children (derived from children_ages)

    children: Optional[int] = Field(
        None,
        description="Number of children (automatically computed from children_ages)"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "hotel_names": ["Banyan Tree Krabi"],
                "city": "Krabi",
                "check_in": "2025-08-15",
                "check_out": "2025-08-18",
                "adults": 2,
                "children_ages": [10, 12],
                "rooms": 1,
                "room_category": "Standard",
                "nationality": "PL",
                "currency": "EUR",
                "meal_types": ["BB", "HB"],
                "providers": ["rate_hawk", "tbo"],
                 "user": "user97kw86" 
            }
        }

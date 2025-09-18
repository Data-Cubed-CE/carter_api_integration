# Hotel Aggregator App

This module contains the main business logic for aggregating hotel offers from multiple providers.

## Structure
- `services/aggregator.py`: Core aggregation logic, parallel provider search, result normalization.
- `services/universal_provider.py`: Abstracts provider adapters, manages provider loading, circuit breakers, and parallel search.
- `models/`: Data models for requests and responses.
- `config.py`: Configuration management for providers.

## API Integration Pattern
- Each provider implements a standard interface (search, normalize).
- Aggregator runs all providers in parallel using asyncio.
- Results are normalized to a unified schema for easy comparison.
- Circuit breaker pattern ensures resilience against provider failures.

## Usage
- Import `hotel_aggregator` or `universal_provider` for hotel search aggregation.
- Extend by adding new provider adapters in `services/providers/` and updating config.

## Testing
- See `/tests` for unit and integration tests covering core logic and providers.

## Onboarding
- Start by reviewing `aggregator.py` and `universal_provider.py` for main flow.
- Provider adapters (e.g., GoGlobal, RateHawk) are in `services/providers/`.
- Data mapping logic is in `hotel_mapping.py` and CSV files in `/data`.

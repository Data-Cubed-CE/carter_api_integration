# Carter API Integrator - System Agregacji Hoteli

## Przegląd

**Carter API Integrator** to system agregacji hoteli działający w chmurze Azure, który umożliwia wyszukiwanie i porównywanie ofert hotelowych z wielu platform rezerwacyjnych jednocześnie. 

### 🎯 **Główny cel**
Stworzenie **uniwersalnego API** dla agencji podróży, które agreguje dane z różnych systemów rezerwacyjnych i dostarcza **ustandaryzowane, porównywalne wyniki** w jednym miejscu.

### 🏗️ **Architektura wysokopoziomowo**
- **Serverless** - Azure Functions v2 zapewnia automatyczną skalowalność i optymalizację kosztów
- **Microservices** - Modularna architektura z niezależnymi adapterami providerów  
- **Event-driven** - Asynchroniczne przetwarzanie zapewnia wysoką wydajność
- **Cloud-native** - Pełna integracja z ekosystemem usług Azure

### 🔄 **Przepływ danych**
```
Żądanie klienta → Walidacja API → Orkiestracja providerów → Przetwarzanie danych → Zunifikowana odpowiedź

```

### **Dostawcy hotelowi**
- **Rate Hawk** (worldota.net)
- **GoGlobal** 
- **TBO** 
- stan na 2025-09-18

### **Kluczowe funkcjonalności**
- **Równoległe wyszukiwanie** - Równoległe odpytywanie wszystkich providerów
- **Mapowanie hoteli** - Automatyczna standaryzacja nazw hoteli i typów pokoi
- **Circuit Breaker** - Zabezpieczenie przed awariami zewnętrznych API
- **Monitorowanie w czasie rzeczywistym** - Logowanie i metryki wydajności

### **Wartość biznesowa**
- **Jeden punkt dostępu** zamiast integracji z wieloma providerami
- **Zunifikowane formaty** danych eliminują różnice między dostawcami  
- **Skalowanie** - Azure Functions skaluje się zgodnie z zapotrzebowaniem

### **Stack technologiczny**
- **Azure Functions v2** - Platforma serverless do hostingu aplikacji
- **FastAPI** - Nowoczesny framework webowy dla Python
- **aiohttp** - Biblioteka do asynchronicznych połączeń HTTP
- **Pandas & RapidFuzz** - Narzędzia do przetwarzania danych
- **Azure Services** - Key Vault, SQL Database, Blob Storage, Application Insights

## Architecture Overview

### High-Level Architecture

```
                              CARTER API INTEGRATOR
    
    ┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
    │   CLIENT APPS   │ HTTPS │  AZURE FUNCTIONS│ HTTP  │  EXTERNAL APIs  │
    │                 │──────▶│      v2         │──────▶│                 │
    │ • Web Apps      │       │                 │       │ • Rate Hawk     │
    │ • Mobile Apps   │       │ function_app.py │       │ • GoGlobal      │
    │ • API Clients   │       │ app/main.py     │       │ • TBO           │
    └─────────────────┘       │ (FastAPI Core)  │       └─────────────────┘
                              └─────────────────┘
                                      │
                              ┌─────────────────┐
                              │ UNIVERSAL       │
                              │ PROVIDER        │
                              │                 │
                              │ • Orchestration │
                              │ • Circuit Break │
                              │ • Concurrent    │
                              └─────────────────┘
                                      │
                    ┌─────────────────┼────────────────┐
                    │                 │                │
            ┌───────▼─────────┐ ┌─────▼──────┐ ┌───────▼─────────┐
            │  RATE HAWK      │ │ GOGLOBAL   │ │     TBO         │
            │  PROVIDER       │ │ PROVIDER   │ │   PROVIDER      │
            │                 │ │            │ │                 │
            │ • JSON API      │ │ • XML API  │ │ • JSON API      │
            │ • BasicAuth     │ │ • AgencyID │ │ • Rate Limits   │
            └─────────────────┘ └────────────┘ └─────────────────┘
                                       │
                              ┌─────────────────┐
                              │ DATA PROCESSING │
                              │ & MAPPING       │
                              │                 │
                              │ • Hotel Mapping │
                              │ • Room Mapping  │
                              │ • Meal Mapping  │
                              └─────────────────┘
                                       │
                              ┌─────────────────┐
                              │ FINAL RESPONSE  │
                              │                 │
                              │ • Aggregated    │
                              │ • Standardized  │
                              │ • JSON Format   │
                              └─────────────────┘

    ┌─────────────────────────────────────────────────────────────────┐
    │                 WSPIERAJĄCE USŁUGI AZURE                        │
    │                                                                 │
    │  Key Vault ── Sekrety i dane uwierzytelniające API              │
    │  SQL Database ── Mapowania hoteli/pokoi oraz cache              │
    │  Blob Storage ── Logi, pliki danych oraz kopie zapasowe         │
    │  App Insights ── Monitorowanie, metryki oraz alerty             │
    └─────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════
                            PRZEPŁYW ŻĄDANIA
═══════════════════════════════════════════════════════════════════════════════

    Żądanie klienta
         │
         ▼
    Azure Functions ──── Uwierzytelnianie i routing
         │
         ▼
    Walidacja API ── Modele żądań oraz CORS
         │
         ▼
    Universal Provider ── Ładowanie konfiguracji
         │
         ▼
    ┌─────────────────────────────────────────┐
    │        Równoległe WYWOŁANIA API        │
    │                                         │
    │  Rate Hawk ──┐                          │
    │              │                          │
    │  GoGlobal ───┼── Wykonywanie równoległe │
    │              │                          │
    │  TBO ────────┘                          │
    └─────────────────────────────────────────┘
         │
         ▼
    ┌─────────────────────────────────────────┐
    │       PRZETWARZANIE ODPOWIEDZI          │
    │                                         │
    │  • Standaryzacja nazw hoteli            │
    │  • Klasyfikacja typów pokoi             │
    │  • Normalizacja planów wyżywienia       │
    │  • Porównanie cen i ranking             │
    │  • Obsługa błędów oraz fallbacki        │
    └─────────────────────────────────────────┘
         │
         ▼
    Standaryzowana odpowiedź JSON ────> Aplikacje klienckie

═══════════════════════════════════════════════════════════════════════════════
```

### Główne komponenty

1. **Aplikacja Azure Functions** (`function_app.py`)
   - Punkt wejścia dla żądań HTTP
   - Model programowania Azure Functions v2
   - Zarządzanie sesjami i cleanup

2. **Aplikacja FastAPI** (`app/main.py`)
   - Endpointy RESTful API
   - Modele żądań/odpowiedzi
   - Konfiguracja CORS
   - Dokumentacja API

3. **System Universal Provider** (`app/services/universal_provider.py`)
   - Warstwa abstrakcji providerów
   - Implementacja wzorca Circuit Breaker
   - Równoległe wykonywanie wyszukiwań na platformach rezerwacyjnych

4. **Adaptery providerów** (`app/services/providers/`)
   - Indywidualne integracje API dla każdej platformy
   - Normalizacja żądań i standaryzacja odpowiedzi


## Struktura projektu

```
carter_api_integrator/
├── function_app.py                 # Punkt wejścia Azure Functions
├── app/                           # Główna aplikacja
│   ├── main.py                   # Aplikacja FastAPI
│   ├── config.py                 # Zarządzanie konfiguracją
│   ├── models/                   # Modele żądań/odpowiedzi
│   │   ├── request.py
│   │   └── response.py
│   ├── services/                 # Usługi logiki biznesowej
│   │   ├── universal_provider.py # Orkiestracja providerów
│   │   ├── providers/           # Indywidualne adaptery API
│   │   │   ├── rate_hawk.py
│   │   │   ├── goglobal.py
│   │   │   └── tbo.py
│   │   ├── azure_keyvault_service.py
│   │   ├── azure_sql_connector.py
│   │   ├── blob_storage.py
│   │   ├── circuit_breaker.py
│   │   ├── database_operations.py
│   │   ├── hotel_mapping.py
│   │   ├── meal_mapping.py
│   │   ├── room_mapping.py
│   │   └── session_manager.py
│   ├── data/                    # Przetwarzanie danych i mapowanie
│   │   ├── hotel_mapper/        # Standaryzacja hoteli
│   │   └── room_mapper/         # Standaryzacja typów pokoi
│   ├── utils/                   # Narzędzia pomocnicze
│   │   └── logger.py
│   └── config/                  # Pliki konfiguracyjne
│       └── room_mappings_config.yaml
├── logs/                        # Logi aplikacji
├── requirements.txt             # Zależności Python
├── host.json                   # Konfiguracja Azure Functions
└── local.settings.json         # Ustawienia deweloperskie
```

## Endpointy API

### Health Check
- `GET /health` - Status kondycji systemu
- `GET /health/providers` - Status kondycji poszczególnych providerów

### Wyszukiwanie hoteli
- `POST /search/hotels` - Wyszukiwanie hoteli u wielu providerów
- Parametry: cel podróży, daty zameldowania/wymeldowania, goście, pokoje

### Zarządzanie providerami
- `GET /providers/status` - Dostępność providerów
- `POST /providers/{provider}/reset` - Reset Circuit Breaker

### Mapowanie danych
- Narzędzia mapowania hoteli, pokoi i planów żywienia
- Endpointy standaryzacji dla spójności danych

## Stack technologiczny

### Technologie podstawowe
- **Python 3.9+** - Język programowania
- **Azure Functions v2** - Platforma serverless compute  
- **FastAPI** - Framework webowy dla rozwoju API
- **aiohttp** - Biblioteka HTTP asynchroniczna

### Usługi Azure
- **Azure Functions** - Hosting serverless
- **Azure Key Vault** - Zarządzanie sekretami
- **Azure SQL Database** - Przechowywanie danych 
- **Azure Blob Storage** - Przechowywanie plików JSON z responsami
- **Azure Monitor** - Monitorowanie aplikacji

### Integracje zewnętrzne
- **Rate Hawk API** - Dostawca
- **GoGlobal API** - Dostawca 
- **TBO API** - Dostawca

## Przepływ danych

1. **Przetwarzanie żądań**
   - Klient wysyła żądanie wyszukiwania hoteli
   - FastAPI waliduje i przetwarza żądanie
   - Żądanie kierowane do Universal Provider

2. **Orkiestracja providerów**
   - Universal Provider rozprowadza zapytanie do wielu providerów
   - Circuit Breaker chroni przed awariami providerów
   - Wykonywanie równoległe dla optymalnej wydajności

3. **Normalizacja odpowiedzi**
   - Surowe odpowiedzi providerów normalizowane do standardowego formatu
   - Zastosowane mapowania hoteli/planów żywienia

4. **Dostarczenie odpowiedzi**
   - Standaryzowana odpowiedź zwrócona do klienta
   - Dołączone kompleksowe metadane
   - Obsługa błędów dla awarii

## Konfiguracja

### Zmienne środowiskowe
- Poświadczenia przez Key Vault

### Konfiguracja providerów
Każdy adapter providera konfiguruje:
- Endpointy API i uwierzytelnianie
- Mapowanie żądań/odpowiedzi
- Progi Circuit Breaker
- Ustawienia timeout

## Deployment

### Local Development
1. Install dependencies: `pip install -r requirements.txt`
2. Zdobądź pliki .env i local.settings.json
3. Uruchowm w termianalu: `func start`

### Postman Collection
1. Użyj Postman collection do łatwego wykonywania zapytań (dostępny w repo)

### Azure Production
- Wdrożenie przez Azure Functions deployment
- Konfiguracja sekretów Key Vault
- Ustawienie monitorowania Application Insights
- Konfiguracja połączeń SQL Database

## Monitorowanie i logowanie

### Poziomy logowania
- **System Level**: Operacje Azure Functions
- **Application Level**: Żądania/odpowiedzi API
- **Provider Level**: Interakcje z zewnętrznymi API
- **Data Level**: Operacje mapowania i transformacji

### Pliki logów
- Ogólne logi aplikacji
- Logi specyficzne dla providerów  
- Śledzenie utraty danych
- Informacje debugowania

## Charakterystyki wydajności

- **Concurrent Processing**: Multiple providers searched simultaneously
- **Connection Pooling**: Optimized HTTP connections
- **Circuit Breakers**: Automatic failure handling
- **Caching**: Response caching for frequent queries
- **Session Management**: Efficient resource utilization

## Error Handling

- Standardized error response format
- Circuit breaker pattern for provider failures
- Graceful degradation when providers unavailable
- Comprehensive error logging and tracking

## Security

- All sensitive data stored in Azure Key Vault
- Secure HTTP connections (HTTPS only)
- Environment-based configuration
- Audit logging for compliance

# Dokumentacja skryptów i przepływ wykonania

## Przepływ wykonania aplikacji

### 1. Sekwencja startowa

```
Start Azure Functions → function_app.py → FastAPI app/main.py → Inicjalizacja usług
```

#### 1.1 Punkt wejścia Azure Functions (`function_app.py`)
**Znaczenie**: Główny punkt wejścia dla Azure Functions v2
**Funkcje**:
- Inicjalizacja Azure Functions App
- Definicja HTTP endpoints (`/health`, `/search`)
- Context manager dla zarządzania sesjami
- Cleanup zasobów po wykonaniu żądań
- Routing żądań do aplikacji FastAPI

**Kolejność wykonania**: **PIERWSZY** - uruchamiany przy deployment funkcji na Azure

#### 1.2 Rdzeń aplikacji (`app/main.py`)
**Znaczenie**: Główna logika API i routing
**Funkcje**:
- Definicja wszystkich endpointów API
- Konfiguracja CORS
- Walidacja żądań/odpowiedzi 
- Obsługa błędów i zarządzanie odpowiedziami HTTP
- Integracja z Universal Provider

**Kolejność wykonania**: Inicjalizowane przez `function_app.py` przy pierwszym żądaniu

### 2. Warstwa konfiguracji i usług

#### 2.1 Menedżer konfiguracji (`app/config.py`)
**Znaczenie**: Centralne zarządzanie konfiguracją aplikacji
**Funkcje**:
- Ładowanie zmiennych środowiskowych
- Integracja z Azure Key Vault dla sekretów
- Konfiguracja providerów
- Walidacja
- Mechanizmy fallback dla rozwoju lokalnego

**Kolejność wykonania**: Ładowane przy starcie aplikacji (import-time)

#### 2.2 Manager sesji (`app/services/session_manager.py`)
**Znaczenie**: Centralne zarządzanie połączeniami HTTP
**Funkcje**:
- Connection pooling dla każdego providera
- Optymalizacja sesji HTTP z keep-alive
- DNS caching i connection reuse
- Thread-safe tworzenie sesji
- Automatyczne czyszczenie sesji

**Kolejność wykonania**: Inicjalizowane przy pierwszym użyciu

#### 2.3 Circuit Breaker (`app/services/circuit_breaker.py`)
**Znaczenie**: Wzorzec fault tolerance dla external APIs
**Funkcje**:
- Monitorowanie niepowodzeń providerów (gdy wykryje błędy w sekwencji to otwiera bloker)
- Automatyczne otwieranie przy awariach
- Testowanie stanu half-open
- Ochrona timeout

**Kolejność wykonania**: Inicjalizowane dla każdego providera przy starcie

### 3. Warstwa orkiestracji providerów

#### 3.1 Universal Provider (`app/services/universal_provider.py`)
**Znaczenie**: Orkiestrator wszystkich providerów hotelowych
**Funkcje**:
- Dynamiczne ładowanie adapterów providerów
- Równoległe wykonywanie wyszukiwań u providerów
- Integracja Circuit Breaker
- Zarządzanie dostępnością providerów
- Agregacja i normalizacja odpowiedzi

**Kolejność wykonania**: Inicjalizowany przy pierwszym żądaniu wyszukiwania

#### 3.2 Adaptery providerów - Integracja zewnętrznych API

##### Provider Rate Hawk (`app/services/providers/rate_hawk.py`)
**Znaczenie**: Integracja z worldota.net API (ETG V3)
**Funkcje**:
- Pobranie ofert hotelowych

**Kolejność wykonania**: Ładowane dynamicznie przez Universal Provider

##### Provider GoGlobal (`app/services/providers/goglobal.py`)
**Znaczenie**: Integracja z GoGlobal XML API
**Funkcje**:
- Pobranie ofert hotelowych

**Kolejność wykonania**: Ładowane dynamicznie przez Universal Provider

##### TBO Provider (`app/services/providers/tbo.py`)
**Znaczenie**: Integracja z TBO Hotel API
**Funkcje**:
- Pobranie ofert hotelowych

**Kolejność wykonania**: Ładowane dynamicznie przez Universal Provider

### 4. Data Models Layer

#### 4.1 Request Models (`app/models/request.py`)
**Znaczenie**: Walidacja i definicja incoming requests
**Funkcje**:
- Pydantic model dla HotelSearchRequest
- Field validation (dates, guest counts, hotel names)
- Request sanitization
- API documentation schemas

**Kolejność wykonania**: Używane przy każdym API request dla validation

#### 4.2 Response Models (`app/models/response.py`)
**Znaczenie**: Standardized response format
**Funkcje**:
- Unified Offer model dla wszystkich providerów
- Response validation i serialization
- Currency code validation (ISO 4217)
- Metadata i provider information

**Kolejność wykonania**: Używane przy zwracaniu responses z API

### 5. Data Processing & Standardization

#### 5.1 Hotel Mapping Engine (`app/data/hotel_mapper/hotel_mapper.py`)
**Znaczenie**: Standaryzacja nazw hoteli między providerami
**Funkcje**:
- Fuzzy matching algorytmy (rapidfuzz)
- Hotel brand recognition
- Stop words filtering
- Phonetic matching (metaphone)
- Multi-provider hotel deduplication

**Kolejność wykonania**: Wywoływane podczas response processing

#### 5.2 Room Mapping Engine (`app/data/room_mapper/room_mapper_prod.py`)
**Znaczenie**: Standaryzacja typów pokoi i kategorii
**Funkcje**:
- Production-grade room classification
- Multi-criteria scoring algorithms
- Capacity i bedroom count analysis
- Room view i amenity categorization
- Thread-safe processing z concurrent futures

**Kolejność wykonania**: Używane offline do przygotowania mapping data

#### 5.3 Data Processing Scripts
**room_mapper/** directory zawiera pomocnicze skrypty:
- `process_all_files.py` - Batch processing wszystkich mapping files
- `process_ratehawk_complete.py` - Rate Hawk specific standardization
- `process_goglobal_complete.py` - GoGlobal specific processing
- `process_tbo_complete.py` - TBO specific normalization
- `universal_room_parser.py` - Universal room parsing logic

**Kolejność wykonania**: Używane offline do przygotowania mapping data

### 6. Supporting Services

#### 6.1 Azure Integration Services

##### Azure Key Vault Service (`app/services/azure_keyvault_service.py`)
**Znaczenie**: Secure secrets management
**Funkcje**:
- Provider API credentials retrieval
- Database connection strings
- Certificate management
- Fallback dla local development

**Kolejność wykonania**: Ładowane przy config initialization

##### Azure SQL Connector (`app/services/azure_sql_connector.py`)
**Znaczenie**: Database connectivity
**Funkcje**:
- SQL Server connection management
- Connection pooling
- Query execution helpers
- Transaction management

**Kolejność wykonania**: Ładowane lazy przy database operations

##### Blob Storage Service (`app/services/blob_storage.py`)
**Znaczenie**: File storage i logging
**Funkcje**:
- Log file uploads
- Data file storage
- Backup operations
- File retrieval helpers

**Kolejność wykonania**: Używane dla logging i data persistence

#### 6.2 Business Logic Services

##### Hotel Mapping Service (`app/services/hotel_mapping.py`)
**Znaczenie**: Business logic dla hotel standardization
**Funkcje**:
- Hotel matching algorithms
- Mapping cache management
- Provider-specific hotel normalization
- Conflict resolution

##### Room Mapping Service (`app/services/room_mapping.py`)
**Znaczenie**: Business logic dla room categorization
**Funkcje**:
- Room type classification
- Category mapping
- Room feature extraction
- Standardization rules

##### Meal Mapping Service (`app/services/meal_mapping.py`)
**Znaczenie**: Meal plan standardization
**Funkcje**:
- Meal type normalization (BB, HB, AI, etc.)
- Provider-specific meal plan mapping
- Dietary requirement handling

**Kolejność wykonania**: Używane podczas response processing

### 7. Utilities & Infrastructure

#### 7.1 Logger (`app/utils/logger.py`)
**Znaczenie**: Centralized logging system
**Funkcje**:
- Structured logging configuration
- Multiple log levels (debug, providers, general)
- File rotation management
- Session-based logging

**Kolejność wykonania**: Inicjalizowane przy starcie aplikacji

#### 7.2 Database Operations (`app/services/database_operations.py`)
**Znaczenie**: Database abstraction layer
**Funkcje**:
- CRUD operations
- Query builders
- Connection management
- Error handling

**Kolejność wykonania**: Używane przy database interactions

### 8. Configuration Files

#### 8.1 Room Mappings Config (`app/config/room_mappings_config.yaml`)
**Znaczenie**: Room categorization rules
**Funkcje**:
- Room type definitions
- Mapping rules configuration
- Category hierarchies
- Custom mappings per provider

#### 8.2 Azure Functions Config (`host.json`)
**Znaczenie**: Azure Functions runtime configuration
**Funkcje**:
- Function timeout settings
- HTTP route configuration
- Logging levels
- Extension bindings

#### 8.3 Local Development (`local.settings.json`)
**Znaczenie**: Local development environment setup
**Funkcje**:
- Environment variables
- Connection strings
- Provider credentials (development)
- Debug configurations

### 9. Request Processing Flow (Runtime)

```
1. HTTP Request → function_app.py
2. Route to FastAPI → app/main.py  
3. Request validation → app/models/request.py
4. Configuration load → app/config.py
5. Universal Provider → app/services/universal_provider.py
6. Concurrent Provider calls:
   ├── Rate Hawk → app/services/providers/rate_hawk.py
   ├── GoGlobal → app/services/providers/goglobal.py
   └── TBO → app/services/providers/tbo.py
7. Response processing:
   ├── Hotel mapping → app/data/hotel_mapper/
   ├── Room mapping → app/data/room_mapper/
   └── Meal mapping → app/services/meal_mapping.py
8. Response formatting → app/models/response.py
9. HTTP Response → Client
10. Session cleanup → function_app.py
```

### 10. Critical Dependencies & Initialization Order

1. **Azure Functions Runtime** (host.json)
2. **Environment Configuration** (local.settings.json / Azure App Settings)
3. **Key Vault Integration** (azure_keyvault_service.py)
4. **Config Manager** (config.py)
5. **Session Manager** (session_manager.py) 
6. **Circuit Breakers** (circuit_breaker.py)
7. **Provider Adapters** (providers/)
8. **Universal Provider** (universal_provider.py)
9. **Data Processing Engines** (hotel_mapper/, room_mapper/)
10. **API Layer** (main.py)

### 11. Error Handling Flow

```
Provider Error → Circuit Breaker → Universal Provider → Error Response
Database Error → Database Operations → Fallback → Degraded Response
Configuration Error → Config Validation → Startup Failure → Health Check Fail
```

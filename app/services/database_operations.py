"""
Przykładowe operacje na bazie danych Azure SQL używające Azure SQL Connectora.
Ten moduł zawiera przykłady najczęściej używanych operacji na bazie danych.
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, date
from app.services.azure_sql_connector import AzureSQLConnector, create_azure_sql_connector_from_env
from app.config import Config

# Initialize logger
logger = logging.getLogger(__name__)


class DatabaseOperations:
    """Klasa zawierająca przykładowe operacje na bazie danych."""
    
    def __init__(self, connector: AzureSQLConnector):
        self.connector = connector
        self.logger = logging.getLogger(__name__)
    
    async def create_sample_tables(self) -> bool:
        """
        Tworzy przykładowe tabele w bazie danych.
        """
        try:
            # Tabela hoteli
            hotels_table = """
            CREATE TABLE IF NOT EXISTS hotels (
                id INT IDENTITY(1,1) PRIMARY KEY,
                hotel_id NVARCHAR(100) NOT NULL UNIQUE,
                hotel_name NVARCHAR(255) NOT NULL,
                city NVARCHAR(100) NOT NULL,
                country NVARCHAR(100) NOT NULL,
                rating DECIMAL(2,1),
                created_at DATETIME2 DEFAULT GETDATE(),
                updated_at DATETIME2 DEFAULT GETDATE()
            )
            """
            
            # Tabela pokoi
            rooms_table = """
            CREATE TABLE IF NOT EXISTS rooms (
                id INT IDENTITY(1,1) PRIMARY KEY,
                hotel_id NVARCHAR(100) NOT NULL,
                room_code NVARCHAR(100) NOT NULL,
                room_name NVARCHAR(255) NOT NULL,
                room_type NVARCHAR(100),
                capacity INT,
                price DECIMAL(10,2),
                currency NVARCHAR(3),
                created_at DATETIME2 DEFAULT GETDATE(),
                FOREIGN KEY (hotel_id) REFERENCES hotels(hotel_id)
            )
            """
            
            # Tabela rezerwacji
            bookings_table = """
            CREATE TABLE IF NOT EXISTS bookings (
                id INT IDENTITY(1,1) PRIMARY KEY,
                booking_id NVARCHAR(100) NOT NULL UNIQUE,
                hotel_id NVARCHAR(100) NOT NULL,
                room_code NVARCHAR(100) NOT NULL,
                guest_name NVARCHAR(255) NOT NULL,
                guest_email NVARCHAR(255),
                check_in_date DATE NOT NULL,
                check_out_date DATE NOT NULL,
                total_price DECIMAL(10,2) NOT NULL,
                currency NVARCHAR(3) NOT NULL,
                status NVARCHAR(50) DEFAULT 'confirmed',
                created_at DATETIME2 DEFAULT GETDATE(),
                updated_at DATETIME2 DEFAULT GETDATE(),
                FOREIGN KEY (hotel_id) REFERENCES hotels(hotel_id)
            )
            """
            
            # Tworzenie tabel
            await self.connector.execute_query(hotels_table, fetch_results=False)
            await self.connector.execute_query(rooms_table, fetch_results=False)
            await self.connector.execute_query(bookings_table, fetch_results=False)
            
            self.logger.info("Sample tables created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create sample tables: {str(e)}")
            return False
    
    async def insert_sample_data(self) -> bool:
        """
        Wstawia przykładowe dane do tabel.
        """
        try:
            # Przykładowe hotele
            hotels_data = [
                {
                    "hotel_id": "HTL001",
                    "hotel_name": "Grand Hotel Warsaw",
                    "city": "Warsaw",
                    "country": "Poland",
                    "rating": 4.5
                },
                {
                    "hotel_id": "HTL002", 
                    "hotel_name": "Krakow Palace Hotel",
                    "city": "Krakow",
                    "country": "Poland",
                    "rating": 4.3
                },
                {
                    "hotel_id": "HTL003",
                    "hotel_name": "Gdansk Seaside Resort",
                    "city": "Gdansk", 
                    "country": "Poland",
                    "rating": 4.7
                }
            ]
            
            # Wstawienie hoteli
            await self.connector.bulk_insert("hotels", hotels_data)
            
            # Przykładowe pokoje
            rooms_data = [
                {
                    "hotel_id": "HTL001",
                    "room_code": "STD001",
                    "room_name": "Standard Single Room",
                    "room_type": "Single",
                    "capacity": 1,
                    "price": 250.00,
                    "currency": "PLN"
                },
                {
                    "hotel_id": "HTL001",
                    "room_code": "DBL001", 
                    "room_name": "Deluxe Double Room",
                    "room_type": "Double",
                    "capacity": 2,
                    "price": 450.00,
                    "currency": "PLN"
                },
                {
                    "hotel_id": "HTL002",
                    "room_code": "STE001",
                    "room_name": "Presidential Suite",
                    "room_type": "Suite",
                    "capacity": 4,
                    "price": 850.00,
                    "currency": "PLN"
                }
            ]
            
            # Wstawienie pokoi
            await self.connector.bulk_insert("rooms", rooms_data)
            
            # Przykładowe rezerwacje
            bookings_data = [
                {
                    "booking_id": "BKG001",
                    "hotel_id": "HTL001",
                    "room_code": "STD001",
                    "guest_name": "Jan Kowalski",
                    "guest_email": "jan.kowalski@email.com",
                    "check_in_date": date(2025, 10, 15),
                    "check_out_date": date(2025, 10, 18),
                    "total_price": 750.00,
                    "currency": "PLN",
                    "status": "confirmed"
                },
                {
                    "booking_id": "BKG002",
                    "hotel_id": "HTL002",
                    "room_code": "STE001", 
                    "guest_name": "Anna Nowak",
                    "guest_email": "anna.nowak@email.com",
                    "check_in_date": date(2025, 11, 1),
                    "check_out_date": date(2025, 11, 5),
                    "total_price": 3400.00,
                    "currency": "PLN",
                    "status": "pending"
                }
            ]
            
            # Wstawienie rezerwacji
            await self.connector.bulk_insert("bookings", bookings_data)
            
            self.logger.info("Sample data inserted successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to insert sample data: {str(e)}")
            return False
    
    async def get_hotels_by_city(self, city: str) -> List[Dict[str, Any]]:
        """
        Pobiera hotele z określonego miasta.
        """
        query = """
        SELECT hotel_id, hotel_name, city, country, rating, created_at
        FROM hotels 
        WHERE city = ?
        ORDER BY rating DESC
        """
        
        return await self.connector.execute_query(query, [city])
    
    async def get_available_rooms(self, hotel_id: str, check_in: date, check_out: date) -> List[Dict[str, Any]]:
        """
        Pobiera dostępne pokoje w hotelu w określonych datach.
        """
        query = """
        SELECT r.room_code, r.room_name, r.room_type, r.capacity, r.price, r.currency
        FROM rooms r
        WHERE r.hotel_id = ? 
        AND r.room_code NOT IN (
            SELECT room_code 
            FROM bookings 
            WHERE hotel_id = ?
            AND status != 'cancelled'
            AND NOT (check_out_date <= ? OR check_in_date >= ?)
        )
        ORDER BY r.price ASC
        """
        
        return await self.connector.execute_query(
            query, 
            [hotel_id, hotel_id, check_in, check_out]
        )
    
    async def create_booking(self, booking_data: Dict[str, Any]) -> bool:
        """
        Tworzy nową rezerwację.
        """
        query = """
        INSERT INTO bookings 
        (booking_id, hotel_id, room_code, guest_name, guest_email, 
         check_in_date, check_out_date, total_price, currency, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            await self.connector.execute_query(
                query, 
                [
                    booking_data.get('booking_id'),
                    booking_data.get('hotel_id'),
                    booking_data.get('room_code'),
                    booking_data.get('guest_name'),
                    booking_data.get('guest_email'),
                    booking_data.get('check_in_date'),
                    booking_data.get('check_out_date'),
                    booking_data.get('total_price'),
                    booking_data.get('currency'),
                    booking_data.get('status', 'confirmed')
                ],
                fetch_results=False
            )
            
            self.logger.info(f"Booking {booking_data.get('booking_id')} created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create booking: {str(e)}")
            return False
    
    async def get_booking_details(self, booking_id: str) -> Optional[Dict[str, Any]]:
        """
        Pobiera szczegóły rezerwacji.
        """
        query = """
        SELECT b.*, h.hotel_name, r.room_name, r.room_type
        FROM bookings b
        JOIN hotels h ON b.hotel_id = h.hotel_id
        JOIN rooms r ON b.hotel_id = r.hotel_id AND b.room_code = r.room_code
        WHERE b.booking_id = ?
        """
        
        results = await self.connector.execute_query(query, [booking_id])
        return results[0] if results else None
    
    async def update_booking_status(self, booking_id: str, status: str) -> bool:
        """
        Aktualizuje status rezerwacji.
        """
        query = """
        UPDATE bookings 
        SET status = ?, updated_at = GETDATE()
        WHERE booking_id = ?
        """
        
        try:
            await self.connector.execute_query(query, [status, booking_id], fetch_results=False)
            self.logger.info(f"Booking {booking_id} status updated to {status}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update booking status: {str(e)}")
            return False
    
    async def get_revenue_report(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """
        Generuje raport przychodów w określonym okresie.
        """
        query = """
        SELECT 
            h.hotel_name,
            h.city,
            COUNT(b.id) as total_bookings,
            SUM(b.total_price) as total_revenue,
            b.currency,
            AVG(b.total_price) as avg_booking_value
        FROM bookings b
        JOIN hotels h ON b.hotel_id = h.hotel_id
        WHERE b.created_at >= ? 
        AND b.created_at <= ?
        AND b.status != 'cancelled'
        GROUP BY h.hotel_name, h.city, b.currency
        ORDER BY total_revenue DESC
        """
        
        return await self.connector.execute_query(query, [start_date, end_date])
    
    async def cleanup_sample_data(self) -> bool:
        """
        Usuwa przykładowe dane z tabel.
        """
        try:
            # Usuń dane w odpowiedniej kolejności (z powodu foreign keys)
            await self.connector.execute_query("DELETE FROM bookings", fetch_results=False)
            await self.connector.execute_query("DELETE FROM rooms", fetch_results=False)
            await self.connector.execute_query("DELETE FROM hotels", fetch_results=False)
            
            self.logger.info("Sample data cleaned up successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup sample data: {str(e)}")
            return False


# Funkcje pomocnicze do testowania connectora
async def test_azure_sql_connection():
    """Testuje połączenie z Azure SQL."""
    try:
        # Utworzenie connectora z konfiguracji środowiska
        connector = create_azure_sql_connector_from_env()
        
        # Test połączenia
        if connector.test_connection():
            print("✅ Azure SQL connection successful!")
            
            # Sprawdzenie wersji bazy danych
            results = await connector.execute_query("SELECT @@VERSION as version")
            if results:
                print(f"Database version: {results[0]['version'][:100]}...")
            
            # Test podstawowego zapytania
            test_query = "SELECT GETDATE() as current_time, DB_NAME() as database_name"
            results = await connector.execute_query(test_query)
            if results:
                print(f"Current time: {results[0]['current_time']}")
                print(f"Database name: {results[0]['database_name']}")
            
            return True
        else:
            print("❌ Azure SQL connection failed!")
            return False
            
    except Exception as e:
        print(f"❌ Error testing Azure SQL connection: {str(e)}")
        return False


async def demo_database_operations():
    """Demonstracja operacji na bazie danych."""
    try:
        # Utworzenie connectora
        connector = create_azure_sql_connector_from_env()
        operations = DatabaseOperations(connector)
        
        print("\n🚀 Starting Azure SQL Database Demo...")
        
        # 1. Tworzenie tabel
        print("\n📋 Creating sample tables...")
        if await operations.create_sample_tables():
            print("✅ Sample tables created")
        
        # 2. Wstawianie przykładowych danych
        print("\n📊 Inserting sample data...")
        if await operations.insert_sample_data():
            print("✅ Sample data inserted")
        
        # 3. Pobieranie hoteli z Warszawy
        print("\n🏨 Getting hotels in Warsaw...")
        warsaw_hotels = await operations.get_hotels_by_city("Warsaw")
        for hotel in warsaw_hotels:
            print(f"  - {hotel['hotel_name']} (Rating: {hotel['rating']})")
        
        # 4. Sprawdzanie dostępnych pokoi
        print("\n🛏️ Checking available rooms in HTL001...")
        check_in = date(2025, 12, 1)
        check_out = date(2025, 12, 3)
        available_rooms = await operations.get_available_rooms("HTL001", check_in, check_out)
        for room in available_rooms:
            print(f"  - {room['room_name']} ({room['room_type']}) - {room['price']} {room['currency']}")
        
        # 5. Tworzenie nowej rezerwacji
        print("\n📝 Creating new booking...")
        new_booking = {
            "booking_id": "BKG003",
            "hotel_id": "HTL001",
            "room_code": "DBL001",
            "guest_name": "Piotr Wiśniewski",
            "guest_email": "piotr.wisniewski@email.com",
            "check_in_date": date(2025, 12, 10),
            "check_out_date": date(2025, 12, 13),
            "total_price": 1350.00,
            "currency": "PLN"
        }
        
        if await operations.create_booking(new_booking):
            print("✅ New booking created")
        
        # 6. Pobieranie szczegółów rezerwacji
        print("\n📄 Getting booking details...")
        booking_details = await operations.get_booking_details("BKG003")
        if booking_details:
            print(f"  Booking: {booking_details['booking_id']}")
            print(f"  Guest: {booking_details['guest_name']}")
            print(f"  Hotel: {booking_details['hotel_name']}")
            print(f"  Room: {booking_details['room_name']}")
            print(f"  Total: {booking_details['total_price']} {booking_details['currency']}")
        
        # 7. Generowanie raportu przychodów
        print("\n💰 Generating revenue report...")
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
        revenue_report = await operations.get_revenue_report(start_date, end_date)
        for report in revenue_report:
            print(f"  {report['hotel_name']} ({report['city']}): {report['total_revenue']} {report['currency']} ({report['total_bookings']} bookings)")
        
        print("\n✅ Demo completed successfully!")
        
        # Opcjonalnie: czyszczenie danych demonstracyjnych
        # print("\n🧹 Cleaning up sample data...")
        # if await operations.cleanup_sample_data():
        #     print("✅ Sample data cleaned up")
        
    except Exception as e:
        print(f"❌ Demo failed: {str(e)}")
        logger.exception("Demo failed with exception")


if __name__ == "__main__":
    # Uruchomienie demonstracji
    asyncio.run(demo_database_operations())
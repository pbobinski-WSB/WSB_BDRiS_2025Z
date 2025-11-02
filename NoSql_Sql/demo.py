import time
import json
import mysql.connector
from pymongo import MongoClient
from redis import Redis
from neo4j import GraphDatabase

# --- 0. Demonstracja MySQL (Baza Relacyjna) ---
def demo_mysql():
    print("\n--- Rozpoczynam demonstrację MySQL ---")
    db_connection = None
    try:
        db_connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password",
            database="nosql_demo"
        )
        cursor = db_connection.cursor(dictionary=True)

        # Krok 1: Wymagane jest zdefiniowanie schematu (DDL)
        print("Wymagane zdefiniowanie schematu: tworzenie tabel `users` i `tags`...")
        cursor.execute("DROP TABLE IF EXISTS tags")
        cursor.execute("DROP TABLE IF EXISTS users")
        cursor.execute("""
            CREATE TABLE users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                age INT
            )
        """)
        cursor.execute("""
            CREATE TABLE tags (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                tag VARCHAR(50),
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        
        # Krok 2: Wstawianie danych (DML)
        print("\nWstawianie danych do tabel...")
        # Prosty użytkownik
        cursor.execute("INSERT INTO users (name, email, age) VALUES (%s, %s, %s)", ("Jan Kowalski", "jan.k@example.com", 30))
        # Użytkownik z tagami - wymaga wstawienia do DWÓCH tabel
        cursor.execute("INSERT INTO users (name, email, age) VALUES (%s, %s, %s)", ("Anna Nowak", "anna.n@example.com", 28))
        anna_id = cursor.lastrowid
        tags_for_anna = [('python',), ('data',), ('nosql',)]
        for tag in tags_for_anna:
            cursor.execute("INSERT INTO tags (user_id, tag) VALUES (%s, %s)", (anna_id, tag[0]))
        
        db_connection.commit()
        print("Dane wstawione.")

        # Krok 3: Odpytywanie danych
        print("\nOdczyt wszystkich użytkowników:")
        cursor.execute("SELECT * FROM users")
        print(list(cursor))

        print("\nOdczyt pełnego profilu Anny Nowak (wymaga operacji JOIN):")
        cursor.execute("""
            SELECT u.name, u.email, t.tag 
            FROM users u
            JOIN tags t ON u.id = t.user_id
            WHERE u.name = 'Anna Nowak'
        """)
        print(list(cursor))

    except mysql.connector.Error as e:
        print(f"Błąd MySQL: {e}")
    finally:
        if db_connection and db_connection.is_connected():
            cursor.close()
            db_connection.close()


# --- 1. Demonstracja MongoDB (Baza Dokumentowa) ---
def demo_mongodb():
    print("\n--- Rozpoczynam demonstrację MongoDB ---")
    try:
        # Połączenie z kontenerem 'mongodb'
        client = MongoClient('mongodb://root:password@localhost:27018/')
        db = client.nosql_demo
        collection = db.users
        
        # Czyszczenie kolekcji na start
        collection.delete_many({})
        print("Kolekcja 'users' wyczyszczona.")

        # Zapisujemy jeden dokument
        user_simple = {"name": "Jan Kowalski", "email": "jan.k@example.com", "age": 30}
        collection.insert_one(user_simple)
        print(f"Zapisano jeden dokument: {user_simple}")
        
        # Zapisujemy bardziej złożony dokument (zagnieżdżony obiekt i lista)
        user_complex = {
            "name": "Anna Nowak",
            "email": "anna.n@example.com",
            "age": 28,
            "address": {"city": "Warszawa", "street": "Prosta 1"},
            "tags": ["python", "data", "nosql"]
        }
        collection.insert_one(user_complex)
        print(f"Zapisano złożony dokument: {user_complex}")
        
        # Odczytujemy wszystkich użytkowników
        print("\nOdczyt wszystkich użytkowników:")
        for user in collection.find():
            print(user)
        
        # Odczytujemy użytkownika po zagnieżdżonym polu
        print("\nZnajdź użytkownika z Warszawy:")
        user_from_warsaw = collection.find_one({"address.city": "Warszawa"})
        print(user_from_warsaw)

    except Exception as e:
        print(f"Błąd MongoDB: {e}")
    finally:
        client.close()

# --- 2. Demonstracja Redis (Baza Klucz-Wartość) ---
def demo_redis():
    print("\n--- Rozpoczynam demonstrację Redis ---")
    try:
        # Połączenie z kontenerem 'redis'
        r = Redis(host='localhost', port=6379, decode_responses=True)
        
        # Proste SET/GET - np. do przechowywania konfiguracji
        r.set('system:version', '1.2.3')
        version = r.get('system:version')
        print(f"Odczytano wersję systemu: {version}")
        
        # Przechowywanie sesji użytkownika (serializowanej do JSON) z czasem wygaśnięcia
        user_session = {"userId": 123, "cart_items": 5}
        r.set('session:user:123', json.dumps(user_session), ex=60) # Wygasa po 60s
        print("Zapisano sesję użytkownika na 60 sekund.")

        # Użycie jako licznika (operacje atomowe)
        r.incr('page_views:home')
        r.incr('page_views:home')
        views = r.get('page_views:home')
        print(f"Strona główna ma {views} wyświetleń.")
        
    except Exception as e:
        print(f"Błąd Redis: {e}")

# --- 3. Demonstracja Neo4j (Baza Grafowa) ---
def demo_neo4j():
    print("\n--- Rozpoczynam demonstrację Neo4j ---")
    # Połączenie z kontenerem 'neo4j'
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

    def run_query(tx, query, **kwargs):
        result = tx.run(query, **kwargs)
        return [record.data() for record in result]

    try:
        with driver.session(database="neo4j") as session:
            # Czyszczenie bazy na start
            session.execute_write(run_query, "MATCH (n) DETACH DELETE n")
            print("Baza grafowa wyczyszczona.")
            
            # Tworzenie węzłów (Nodes)
            session.execute_write(run_query, "CREATE (:Person {name: 'Jan'})")
            session.execute_write(run_query, "CREATE (:Person {name: 'Anna'})")
            session.execute_write(run_query, "CREATE (:Person {name: 'Piotr'})")
            session.execute_write(run_query, "CREATE (:Movie {title: 'Matrix'})")
            print("Stworzono węzły: Jan, Anna, Piotr, Matrix.")

            # Tworzenie relacji (Relationships)
            session.execute_write(run_query, "MATCH (p1:Person {name: 'Jan'}), (p2:Person {name: 'Anna'}) CREATE (p1)-[:FRIENDS_WITH]->(p2)")
            session.execute_write(run_query, "MATCH (p1:Person {name: 'Anna'}), (p2:Person {name: 'Piotr'}) CREATE (p1)-[:FRIENDS_WITH]->(p2)")
            session.execute_write(run_query, "MATCH (p:Person {name: 'Jan'}), (m:Movie {title: 'Matrix'}) CREATE (p)-[:RATED {rating: 5}]->(m)")
            session.execute_write(run_query, "MATCH (p:Person {name: 'Piotr'}), (m:Movie {title: 'Matrix'}) CREATE (p)-[:RATED {rating: 4}]->(m)")
            print("Stworzono relacje: Jan->Anna, Anna->Piotr, Jan->Matrix, Piotr->Matrix.")

            # Zapytania - potęga grafu
            print("\nKto ocenił film 'Matrix'?")
            result = session.execute_read(run_query, "MATCH (p:Person)-[r:RATED]->(m:Movie {title: 'Matrix'}) RETURN p.name, r.rating")
            print(result)
            
            print("\nZnajdź znajomych znajomego Jana (ale nie samego Jana):")
            result = session.execute_read(run_query, """
                MATCH (p1:Person {name: 'Jan'})-[:FRIENDS_WITH*2]->(p3:Person)
                WHERE p1 <> p3
                RETURN p3.name
            """)
            print(result)

    except Exception as e:
        print(f"Błąd Neo4j: {e}")
    finally:
        driver.close()

if __name__ == '__main__':
    demo_mysql()
    demo_mongodb()
    demo_redis()
    demo_neo4j()

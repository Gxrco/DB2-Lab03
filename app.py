from neo4j import GraphDatabase

uri = "neo4j+s://4fbc64fe.databases.neo4j.io"         
username = "neo4j"                      
password = "LlgAEFFdMyTNy3ayx0YB6XiO3jbrqAIxe5iLPGUCmbQ"

driver = GraphDatabase.driver(uri, auth=(username, password))

def close_connection():
    driver.close()

def verify_connection():
    try:
        with driver.session() as session:
            result = session.run("RETURN 1 AS value")
            value = result.single()["value"]
            print("Connection successful. Result:", value)
    except Exception as e:
        print("Connection failed:", e)

def create_user(tx, user_id, name):
    query = """
    MERGE (u:USER {userId: $userId, name: $name})
    """
    tx.run(query, userId=user_id, name=name)

def create_movie(tx, movie_id, title, year, plot):
    query = """
    MERGE (m:MOVIE {movieId: $movieId, title: $title, year: $year, plot: $plot})
    """
    tx.run(query, movieId=movie_id, title=title, year=year, plot=plot)

def create_rating(tx, user_id, movie_id, rating, timestamp):
    query = """
    MATCH (u:USER {userId: $userId})
    MATCH (m:MOVIE {movieId: $movieId})
    MERGE (u)-[:RATED {rating: $rating, timestamp: $timestamp}]->(m)
    """
    tx.run(query, userId=user_id, movieId=movie_id, rating=rating, timestamp=timestamp)

def populate_graph():
    with driver.session() as session:
        # Crear usuarios
        users = [
            {"userId": "u1", "name": "Alice"},
            {"userId": "u2", "name": "Bob"},
            {"userId": "u3", "name": "Charlie"},
            {"userId": "u4", "name": "Diana"},
            {"userId": "u5", "name": "Eve"}
        ]
        for user in users:
            session.write_transaction(create_user, user["userId"], user["name"])
        
        # Crear películas
        movies = [
            {"movieId": 1, "title": "Inception", "year": 2010, "plot": "A mind-bending thriller"},
            {"movieId": 2, "title": "The Matrix", "year": 1999, "plot": "A hacker discovers reality is a simulation"},
            {"movieId": 3, "title": "Interstellar", "year": 2014, "plot": "A journey through space and time"},
            {"movieId": 4, "title": "The Dark Knight", "year": 2008, "plot": "A vigilante fights crime in Gotham"},
            {"movieId": 5, "title": "Forrest Gump", "year": 1994, "plot": "A man with a low IQ achieves great things"}
        ]
        for movie in movies:
            session.write_transaction(create_movie, movie["movieId"], movie["title"], movie["year"], movie["plot"])
        
        # Crear relaciones de calificación
        ratings = [
            {"userId": "u1", "movieId": 1, "rating": 5, "timestamp": 1620010000},
            {"userId": "u1", "movieId": 2, "rating": 4, "timestamp": 1620010500},
            {"userId": "u2", "movieId": 2, "rating": 5, "timestamp": 1620011000},
            {"userId": "u2", "movieId": 3, "rating": 4, "timestamp": 1620011500},
            {"userId": "u3", "movieId": 3, "rating": 5, "timestamp": 1620012000},
            {"userId": "u3", "movieId": 4, "rating": 4, "timestamp": 1620012500},
            {"userId": "u4", "movieId": 4, "rating": 5, "timestamp": 1620013000},
            {"userId": "u4", "movieId": 5, "rating": 4, "timestamp": 1620013500},
            {"userId": "u5", "movieId": 1, "rating": 3, "timestamp": 1620014000},
            {"userId": "u5", "movieId": 5, "rating": 5, "timestamp": 1620014500}
        ]
        for rating in ratings:
            session.write_transaction(create_rating, rating["userId"], rating["movieId"], rating["rating"], rating["timestamp"])


verify_connection()

populate_graph()

close_connection()

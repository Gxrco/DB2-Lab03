from neo4j import GraphDatabase

uri = "<URI>"         
username = "<USERNAME"                      
password = "<PASSWORD"

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

def run_cypher_query(query, parameters=None):
    with driver.session() as session:
        with session.begin_transaction() as tx:
            result = tx.run(query, parameters)
            try:
                return [record for record in result]
            except Exception as e:
                print("Error during query execution:", e)
                return None

def find_user(user_id):
    query = "MATCH (u:USER {userId: $userId}) RETURN u"
    return run_cypher_query(query, {'userId': user_id})

def find_movie(movie_id):
    query = "MATCH (m:MOVIE {movieId: $movieId}) RETURN m"
    return run_cypher_query(query, {'movieId': movie_id})

def find_user_rate_movie(user_id, movie_id):
    query = """
    MATCH (u:USER {userId: $userId})-[r:RATED]->(m:MOVIE {movieId: $movieId})
    RETURN u, r, m
    """
    return run_cypher_query(query, {'userId': user_id, 'movieId': movie_id})

# Funciones auxiliares para crear nodos Person y sus relaciones

def create_person(tx, tmdbId, name, born, died, bornIn, url, imdbId, bio, poster, labels):
    # Une las etiquetas (por ejemplo: Person:Actor:Director)
    labels_str = ":".join(labels)
    query = f"""
    MERGE (p:{labels_str} {{tmdbId: $tmdbId}})
    SET p.name = $name,
        p.born = $born,
        p.died = $died,
        p.bornIn = $bornIn,
        p.url = $url,
        p.imdbId = $imdbId,
        p.bio = $bio,
        p.poster = $poster
    """
    tx.run(query, tmdbId=tmdbId, name=name, born=born, died=died, bornIn=bornIn, url=url, imdbId=imdbId, bio=bio, poster=poster)

def create_acted_in(tx, person_tmdbId, movieId, role):
    query = """
    MATCH (p {tmdbId: $tmdbId})
    MATCH (m:MOVIE {movieId: $movieId})
    MERGE (p)-[r:ACTED_IN]->(m)
    SET r.role = $role
    """
    tx.run(query, tmdbId=person_tmdbId, movieId=movieId, role=role)

def create_directed(tx, person_tmdbId, movieId, role):
    query = """
    MATCH (p {tmdbId: $tmdbId})
    MATCH (m:MOVIE {movieId: $movieId})
    MERGE (p)-[r:DIRECTED]->(m)
    SET r.role = $role
    """
    tx.run(query, tmdbId=person_tmdbId, movieId=movieId, role=role)

def update_movie_full(tx, movieId, tmdbId, released, imdbRating, imdbId, runtime, countries, imdbVotes, url, revenue, poster, budget, languages):
    query = """
    MATCH (m:MOVIE {movieId: $movieId})
    SET m.tmdbId = $tmdbId,
        m.released = $released,
        m.imdbRating = $imdbRating,
        m.imdbId = $imdbId,
        m.runtime = $runtime,
        m.countries = $countries,
        m.imdbVotes = $imdbVotes,
        m.url = $url,
        m.revenue = $revenue,
        m.poster = $poster,
        m.budget = $budget,
        m.languages = $languages
    """
    tx.run(query, movieId=movieId, tmdbId=tmdbId, released=released, imdbRating=imdbRating, imdbId=imdbId, runtime=runtime, countries=countries, imdbVotes=imdbVotes, url=url, revenue=revenue, poster=poster, budget=budget, languages=languages)

def create_genre(tx, name):
    query = """
    MERGE (g:Genre {name: $name})
    """
    tx.run(query, name=name)

def create_in_genre(tx, movieId, genre_name):
    query = """
    MATCH (m:MOVIE {movieId: $movieId})
    MATCH (g:Genre {name: $genre_name})
    MERGE (m)-[:IN_GENRE]->(g)
    """
    tx.run(query, movieId=movieId, genre_name=genre_name)

# --- Bloque principal: creación del grafo desde cero ---
if __name__ == "__main__":
    verify_connection()

    with driver.session() as session:
        # 1. Crear el Nodo 5: User
        session.execute_write(create_user, "u5", "Eve")

        # 2. Crear el Nodo 4: Movie ("The Dark Knight")
        session.execute_write(create_movie, 4, "The Dark Knight", 2008, "A vigilante fights crime in Gotham")
        
        # Actualizar propiedades extendidas del Nodo 4 (Movie)
        session.execute_write(
            update_movie_full,
            4,                      # movieId
            404,                    # tmdbId
            "2008-07-18T00:00:00",    # released
            9.0,                    # imdbRating
            1004,                   # imdbId
            152,                    # runtime
            ["USA", "UK"],          # countries
            2300000,                # imdbVotes
            "http://example.com/thedarkknight",  # url
            1000000000,             # revenue
            "http://example.com/thedarkknight/poster.jpg", # poster
            185000000,              # budget
            ["English"]             # languages
        )

        # 3. Crear el Nodo 6: Genre y la relación IN_GENRE desde la película
        session.execute_write(create_genre, "Action")
        session.execute_write(create_in_genre, 4, "Action")

        # 4. Crear los nodos Person y sus relaciones con la película (Nodo 4)

        # Nodo 1: Person, Actor, Director
        persona1 = {
            "tmdbId": 101,
            "name": "John Doe",
            "born": "1960-01-01T00:00:00",
            "died": "2020-01-01T00:00:00",
            "bornIn": "USA",
            "url": "http://example.com/johndoe",
            "imdbId": 111,
            "bio": "An accomplished actor and director.",
            "poster": "http://example.com/johndoe/poster.jpg",
            "labels": ["Person", "Actor", "Director"]
        }
        session.execute_write(create_person, **persona1)
        session.execute_write(create_acted_in, persona1["tmdbId"], 4, "Protagonist")
        session.execute_write(create_directed, persona1["tmdbId"], 4, "Director")

        # Nodo 2: Person, Actor
        persona2 = {
            "tmdbId": 102,
            "name": "Jane Smith",
            "born": "1975-05-20T00:00:00",
            "died": None,
            "bornIn": "UK",
            "url": "http://example.com/janesmith",
            "imdbId": 112,
            "bio": "A talented actress known for dramatic roles.",
            "poster": "http://example.com/janesmith/poster.jpg",
            "labels": ["Person", "Actor"]
        }
        session.execute_write(create_person, **persona2)
        session.execute_write(create_acted_in, persona2["tmdbId"], 4, "Supporting Role")

        # Nodo 3: Person, Director
        persona3 = {
            "tmdbId": 103,
            "name": "Alice Johnson",
            "born": "1980-07-15T00:00:00",
            "died": None,
            "bornIn": "Canada",
            "url": "http://example.com/alicejohnson",
            "imdbId": 113,
            "bio": "An innovative director with a unique vision.",
            "poster": "http://example.com/alicejohnson/poster.jpg",
            "labels": ["Person", "Director"]
        }
        session.execute_write(create_person, **persona3)
        session.execute_write(create_directed, persona3["tmdbId"], 4, "Director")

        # 5. Crear relaciones de calificación (RATED)
        # Ejemplo: El usuario "u5" califica la película (Nodo 4)
        session.execute_write(create_rating, "u5", 4, 5, 1620015000)
        # Se puede agregar otra relación de calificación adicional (opcional)
        session.execute_write(create_rating, "u1", 4, 4, 1620016000)

    # Ejemplos de consulta para verificar la creación
    user_result = find_user('u5')
    print("\nUsuario encontrado:", user_result)

    movie_result = find_movie(4)
    print("\nPelícula encontrada:", movie_result)

    rating_result = find_user_rate_movie('u5', 4)
    print("\nRelación de calificación entre usuario y película:", rating_result, "\n")

    close_connection()

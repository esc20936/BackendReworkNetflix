import logging

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

class Neo4j:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()
    
    
    
    def get_movies_by_actors(self, actors):
        with self.driver.session(database="neo4j") as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.read_transaction(self._get_movies_by_actors, actors)
            for record in result:
                print("Movies by actors: {record}".format(record=record))
                
            return result
    
    @staticmethod
    def _get_movies_by_actors(tx, actors):
        result = tx.run(f"""MATCH (a:Person)-[:ACTED_IN]->(m:Movie)
                            WHERE a.name IN {actors}
                            WITH m
                            LIMIT 15
                            RETURN m.poster AS poster, m.title AS name
                            ORDER BY rand()
                        """, actors=actors)
        try:
            result = [{"title": record["name"], "poster":record["poster"]} for record in result]
            # remove duplicates
            result = [dict(t) for t in {tuple(d.items()) for d in result}]
            return result
        except Neo4jError as error:
            logging.error(error)
            return []
    
    
    
    def get_movie_by_name(self, movie_name):
        with self.driver.session(database="neo4j") as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.read_transaction(self._get_movie_by_name, movie_name)
            for record in result:
                print("Movie by name: {record}".format(record=record))
                
            return result
    
    @staticmethod
    def _get_movie_by_name(tx, movie_name):
        result = tx.run(f"""MATCH (m:Movie)
                            WHERE m.title = '{movie_name}'
                            MATCH (a:Person)-[:ACTED_IN]->(m)
                            MATCH (g:Genre)-[:GENRE_OF]->(m)
                            MATCH (d:Person)-[:DIRECTED]->(m)
                            RETURN m,collect(g) AS genres, collect(a) AS actors, collect(d) AS directors
                        """, movie_name=movie_name)
        try:
            # return as a list of dictionaries with all the existing properties
            # return [{key : str(record["m"][key]) if type(record["m"][key])!=list else list(record["m"][key]) for key in record["m"].keys()} for record in result]
            
            movieData ={}
            genres = set()
            actors = set()
            directors = set()
            
            # add the movie data
            for key in result.peek()["m"].keys():
                movieData[key] = str(result.peek()["m"][key]) if type(result.peek()["m"][key])!=list else list(result.peek()["m"][key])
            
            # add the genres
            for record in result:
                for genre in record["genres"]:
                    print("ENTRE")
                    genres.add(genre["name"])
                for actor in record["actors"]:
                    actors.add(actor["name"])
                for director in record["directors"]:
                    directors.add(director["name"])

            # add the actors
           
                    
            
            movieData["genres"] = list(genres)
            movieData["actors"] = list(actors)
            movieData["directors"] = list(directors)
            
            return movieData
            
        except Neo4jError as error:
            logging.error(error)
            return []
    
    
    
    def get_movies_by_genre(self, genre_name, movie_name):
        with self.driver.session(database="neo4j") as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.read_transaction(self._get_movies_by_genre, genre_name, movie_name)
            for record in result:
                print("Movies by genre: {record}".format(record=record))
                
            return result
    
    @staticmethod
    def _get_movies_by_genre(tx, genre_name, movie_name):
        result = tx.run(f"""MATCH (m:Movie)<-[:GENRE_OF]-(g:Genre)
                            WHERE g.name = '{genre_name}' AND m.title <> '{movie_name}'
                            WITH m
                            LIMIT 15
                            RETURN m.poster AS poster, m.title AS name
                            ORDER BY rand()
                        """, genre_name=genre_name)
        try:
            return [{"title": record["name"], "poster":record["poster"]} for record in result]
        except Neo4jError as error:
            logging.error(error)
            return []
    
    def get_random_movie(self):
        with self.driver.session(database="neo4j") as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.read_transaction(self._get_random_movie)
            for record in result:
                print("Random movie: {record}".format(record=record))
                
            return result

    @staticmethod
    def _get_random_movie(tx):
        result = tx.run("""MATCH (m:Movie)
                    WITH m, rand() AS random
                    ORDER BY random
                    LIMIT 1
                    MATCH (a:Person)-[:ACTED_IN]->(m)
                    MATCH (g:Genre)-[:GENRE_OF]->(m)
                    MATCH (d:Person)-[:DIRECTED]->(m)
                    RETURN m,collect(g) AS genres, collect(a) AS actors, collect(d) AS directors
                        """)
        try:
            # return as a list of dictionaries with all the existing properties
            # return [{key : str(record["m"][key]) if type(record["m"][key])!=list else list(record["m"][key]) for key in record["m"].keys()} for record in result]
            
            movieData ={}
            genres = set()
            actors = set()
            directors = set()
            
            # add the movie data
            for key in result.peek()["m"].keys():
                movieData[key] = str(result.peek()["m"][key]) if type(result.peek()["m"][key])!=list else list(result.peek()["m"][key])
            
            # add the genres
            for record in result:
                for genre in record["genres"]:
                    print("ENTRE")
                    genres.add(genre["name"])
                for actor in record["actors"]:
                    actors.add(actor["name"])
                for director in record["directors"]:
                    directors.add(director["name"])

            # add the actors
           
                    
            
            movieData["genres"] = list(genres)
            movieData["actors"] = list(actors)
            movieData["directors"] = list(directors)
            
            return movieData
            
        except Neo4jError as error:
            logging.error(error)
            return []
    
    
    def create_genre_movie_relationship(self, genre_name, movie_name):
        with self.driver.session(database="neo4j") as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.execute_write(
                self._create_and_return_relationship2, genre_name, movie_name)
            for record in result:
                print("Created relationship: {relationship}".format(
                    relationship=record['relationship']))
    
    @staticmethod
    def _create_and_return_relationship2(tx, genre_name, movie_name):
        
        result = tx.run("MATCH (g:Genre {name:$genre_name}) "
                        "MATCH (m:Movie {title:$movie_name}) "
                        "MERGE (g)-[r:GENRE_OF]->(m) "
                        "RETURN g.name, type(r), m.title", genre_name=genre_name, movie_name=movie_name)
        try:
            return [{"genre": record["g.name"], "relationship": record["type(r)"], "movie": record["m.title"]} for record in result]
        except Neo4jError as error:
            logging.error(error)
            return [] 
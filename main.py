from flask import Flask, request
from NEO4j import Neo4j

app = Flask(__name__)
uri = "neo4j+s://ffdacf65.databases.neo4j.io"
user = "neo4j"
password = "sFlE8EchqCO2xYZ1iqbXQ4y5neW9GJFtwVwQLCj-L1I"
bdConnection = Neo4j(uri, user, password)
# undefined routes
@app.route('/')
def index():
    return '404 Not Found, please be better at typing'


@app.route('/randomMovie', methods=['GET'])
def randomMovie():
    return bdConnection.get_random_movie()

# get movies by genre and movie name
@app.route('/moviesByGenre/<genre_name>/<movie_name>', methods=['GET'])
def moviesByGenre(genre_name, movie_name):
    return bdConnection.get_movies_by_genre(genre_name, movie_name)

# get movie by name
@app.route('/movieByName/<movie_name>', methods=['GET'])
def movieByName(movie_name):
    return bdConnection.get_movie_by_name(movie_name)


# get movies with at least one actor in common
# its going to receive a list of actors
@app.route('/moviesByActors', methods=['POST'])
def moviesByActors():
    data = request.get_json()
    return bdConnection.get_movies_by_actors(data['actors'])

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=105)
    
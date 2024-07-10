# Let's write your code here!
from flask import Flask, abort, request
from flask_basicauth import BasicAuth
from flask_swagger_ui import get_swaggerui_blueprint
from collections import defaultdict
import json
import pymysql
import os
import math



app = Flask(__name__)
app.config.from_file("flask_config.json", load=json.load)
auth = BasicAuth(app)

swaggerui_blueprint = get_swaggerui_blueprint(
    base_url='/docs',
    api_url='/static/openapi.yaml',
)
app.register_blueprint(swaggerui_blueprint)

MAX_PAGE_SIZE = 100

def remove_null_fields(obj):
    return {k:v for k, v in obj.items() if v is not None}

@app.route("/movies/<int:movie_id>")
@auth.required

def movie(movie_id):
    db_conn = pymysql.connect(host="localhost", 
                              user="root", 
                              password= "warp4818", 
                              database="bechdel",
                              cursorclass=pymysql.cursors.DictCursor)
    with db_conn.cursor() as cursor:

        cursor.execute("""SELECT
                        M.movieId,
                        M.originalTitle,
                        M.primaryTitle AS englishTitle,
                        B.rating AS bechdelScore,
                        M.runtimeMinutes,
                        M.startYear AS Year,
                        M.movieType,
                        M.isAdult
                    FROM Movies M
                    JOIN Bechdel B ON B.movieId = M.movieId 
                    WHERE M.movieId=%s""", 
                        (movie_id, ))        
        movie = cursor.fetchone()
        if not movie:
            abort(404, description="Movie not found")
        movie = remove_null_fields(movie)

    with db_conn.cursor() as cursor:
        cursor.execute("SELECT * FROM MoviesGenres WHERE movieId=%s", (movie_id, ))
        genres = cursor.fetchall()
    movie['genres'] = [g['genre'] for g in genres]

    with db_conn.cursor() as cursor:
        cursor.execute("""
                    SELECT
                        P.personId,
                        P.primaryName AS name,
                        P.birthYear,
                        P.deathYear,
                        MP.job,
                        MP.category AS role
                    FROM MoviesPeople MP
                    INNER JOIN People P on P.personId = MP.personId
                    WHERE MP.movieId=%s
                        """, (movie_id, ))
        people = cursor.fetchall()
        movie['people'] = [remove_null_fields(p) for p in people]
    

    db_conn.close() 
    return movie

@app.route("/movies")
@auth.required
def movies():
    #URL Parameters
    page = int(request.args.get('page', 0))
    page_size = int(request.args.get('page_size', MAX_PAGE_SIZE))
    page_size = min(page_size, MAX_PAGE_SIZE)
    include_details = bool(int(request.args.get('include_details',0)))

    filters = []
    params = []

    # Filtering
    originalTitle = request.args.get('originalTitle')
    if originalTitle:
        filters.append("originalTitle LIKE %s")
        params.append(f"%{originalTitle}%")

    year = request.args.get('year')
    if year:
        filters.append("startYear = %s")
        params.append(year)

    filter_query = " AND ".join(filters)
    if filter_query:
        filter_query = "WHERE " + filter_query
    

    db_conn = pymysql.connect(host="localhost", user="root", password= "warp4818",database="bechdel",
                              cursorclass=pymysql.cursors.DictCursor)
    with db_conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT M.movieId,
                M.originalTitle,
                M.primaryTitle AS englishTitle,
                B.rating AS bechdelScore,
                M.runtimeMinutes,
                M.startYear AS year,
                M.movieType,
                M.isAdult
            FROM Movies M
            JOIN Bechdel B ON B.movieId = M.movieId 
            {filter_query}
            ORDER BY movieId
            LIMIT %s
            OFFSET %s
        """, (*params, page_size, page * page_size))
        movies = cursor.fetchall()
        movie_ids = [mov['movieId'] for mov in movies]
    
    if include_details:
        # Get genres
        with db_conn.cursor() as cursor:
            placeholder = ','.join(['%s'] * len(movie_ids))
            cursor.execute(f"SELECT * FROM MoviesGenres WHERE movieId IN ({placeholder})",
                        movie_ids)
            genres = cursor.fetchall()
        genres_dict = defaultdict(list)
        for obj in genres:
            genres_dict[obj['movieId']].append(obj['genre'])
        
        # Get people
        with db_conn.cursor() as cursor:
            placeholder = ','.join(['%s'] * len(movie_ids))
            cursor.execute(f"""
                SELECT
                    MP.movieId,
                    P.personId,
                    P.primaryName AS name,
                    P.birthYear,
                    P.deathYear,
                    MP.category AS role
                FROM MoviesPeople MP
                JOIN People P on P.personId = MP.personId
                WHERE movieId IN ({placeholder})
            """, movie_ids)
            people = cursor.fetchall()
        people_dict = defaultdict(list)
        for obj in people:
            movieId = obj['movieId']
            del obj['movieId']
            people_dict[movieId].append(obj)

        # Merge genres and people into movies
        for movie in movies:
            movieId = movie['movieId']
            movie['genres'] = genres_dict[movieId]
            movie['people'] = people_dict[movieId]


    with db_conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS total FROM Movies")
        total = cursor.fetchone()
        last_page = math.ceil(total['total'] / page_size)

    db_conn.close()
    return {
        'movies': movies,
        'next_page': f'/movies?page={page+1}&page_size={page_size}',
        'last_page': f'/movies?page={last_page}&page_size={page_size}',
    }

@app.route("/peoples")
def peoples():
    page = int(request.args.get('page', 0))
    page_size = int(request.args.get('page_size', MAX_PAGE_SIZE))
    page_size = min(page_size, MAX_PAGE_SIZE)
    include_details = bool(int(request.args.get('include_details',0)))
    filters = []
    params = []

    # Filtering
    primaryName = request.args.get('primaryName')
    if primaryName:
        filters.append("primaryName LIKE %s")
        params.append(f"%{primaryName}%")

    birthYear = request.args.get('birthYear')
    if birthYear:
        filters.append("birthYear = %s")
        params.append(birthYear)

    filter_query = " AND ".join(filters)
    if filter_query:
        filter_query = "WHERE " + filter_query


    db_conn = pymysql.connect(host="localhost", user="root", password= "warp4818",database="bechdel",
                              cursorclass=pymysql.cursors.DictCursor)
    with db_conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT * FROM People
            {filter_query}
            ORDER BY personId
            LIMIT %s
            OFFSET %s
        """, (*params, page_size, page * page_size))
        peoples = cursor.fetchall()
        person_ids = [pers['personId'] for pers in peoples]
##
    if include_details:
        # Get role
        with db_conn.cursor() as cursor:
            placeholder = ','.join(['%s'] * len(person_ids))
            cursor.execute(f"SELECT * FROM MoviesPeople WHERE personId IN ({placeholder})",
                        person_ids)
            role = cursor.fetchall()
        role_dict = defaultdict(list)
        for obj in role:
            role_dict[obj['personId']].append(obj['category'])
        
        # Get people
        with db_conn.cursor() as cursor:
            placeholder = ','.join(['%s'] * len(person_ids))
            cursor.execute(f"""
                SELECT
                    MP.movieId,
                    P.personId,
                    P.primaryName AS name,
                    P.birthYear,
                    P.deathYear,
                    MP.category AS role
                FROM MoviesPeople MP
                JOIN People P on P.personId = MP.personId
                WHERE P.personId IN ({placeholder})
            """, person_ids)
            people2 = cursor.fetchall()
        people_dict = defaultdict(list)
        for obj in people2:
            personId = obj['personId']
            del obj['personId']
            people_dict[personId].append(obj)

        # Merge genres and people into movies
        for people in peoples:
            personId = people['personId']
            people['category'] = role_dict[personId]
            people['people'] = people_dict[personId] 
##
    with db_conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS total FROM People")
        total = cursor.fetchone()
        last_page = math.ceil(total['total'] / page_size)

    db_conn.close()
    return {
        'peoples': peoples,
        'next_page': f'/peoples?page={page+1}&page_size={page_size}',
        'last_page': f'/peoples?page={last_page}&page_size={page_size}',
    }

@app.route("/peoples/<int:personId>")
@auth.required

def people(personId):
    db_conn = pymysql.connect(host="localhost", 
                              user="root", 
                              password="warp4818", 
                              database="bechdel",
                              cursorclass=pymysql.cursors.DictCursor)
    with db_conn.cursor() as cursor:

        cursor.execute("""SELECT *                        
                    FROM people M
                    WHERE personId=%s""", 
                        (personId, ))
        
        people = cursor.fetchone()
        if not people:
            abort(404)
        people = remove_null_fields(people)
    
    with db_conn.cursor() as cursor:
        cursor.execute("""SELECT 
                        mp.category AS role,
                        mp.characters,
                        m.originalTitle 
                        FROM people p
                        INNER JOIN moviespeople mp ON p.personId = mp.personId
                        JOIN movies m on m.movieId = mp.movieId
                        WHERE p.personId=%s""", 
                        (personId, ))
        genres = cursor.fetchall()
        people['characters'] = [g['characters'] for g in genres]
        people['originalTitle '] = [g['originalTitle'] for g in genres]

        db_conn.close() 
        return people

#new route POST   
@app.route("/movies/<int:movie_id>", methods=["POST"])
def post_movie(movie_id):
    db_conn = pymysql.connect(host="localhost", 
                              user="root", 
                              password= "warp4818", 
                              database="bechdel",
                              cursorclass=pymysql.cursors.DictCursor)
    with db_conn.cursor() as cursor:

        cursor.execute("""SELECT
                        M.movieId,
                        M.originalTitle,
                        M.primaryTitle AS englishTitle,
                        B.rating AS bechdelScore,
                        M.runtimeMinutes,
                        M.startYear AS Year,
                        M.movieType,
                        M.isAdult
                    FROM Movies M
                    JOIN Bechdel B ON B.movieId = M.movieId 
                    WHERE M.movieId=%s""", 
                        (movie_id, ))        
        movie = cursor.fetchone()
        if not movie:
            abort(404, description="Movie not found")
        movie = remove_null_fields(movie)

    with db_conn.cursor() as cursor:
        cursor.execute("SELECT * FROM MoviesGenres WHERE movieId=%s", (movie_id, ))
        genres = cursor.fetchall()
    movie['genres'] = [g['genre'] for g in genres]

    with db_conn.cursor() as cursor:
        cursor.execute("""
                    SELECT
                        P.personId,
                        P.primaryName AS name,
                        P.birthYear,
                        P.deathYear,
                        MP.job,
                        MP.category AS role
                    FROM MoviesPeople MP
                    INNER JOIN People P on P.personId = MP.personId
                    WHERE MP.movieId=%s
                        """, (movie_id, ))
        people = cursor.fetchall()
        movie['people'] = [remove_null_fields(p) for p in people]
    

    db_conn.close() 
    return movie

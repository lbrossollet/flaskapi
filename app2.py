# Let's write your code here!
from flask import Flask
from flask_basicauth import BasicAuth
from flask import abort
from flask import request
from flask_swagger_ui import get_swaggerui_blueprint
import math
import json
import pymysql
import os



app = Flask(__name__)
app.config.from_file("flask_config.json", load=json.load)
auth = BasicAuth(app)

swaggerui_blueprint = get_swaggerui_blueprint(
    base_url='/docs',
    api_url='/static/openapi.yaml',)
app.register_blueprint(swaggerui_blueprint)

def remove_null_fields(obj):
    return {k:v for k, v in obj.items() if v is not None}
@app.route("/movies/<int:movie_id>")
@auth.required
def movie(movie_id):
    db_conn = pymysql.connect(host="localhost",
                              user="root",
                              password="warp4818",
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
                    WHERE M.movieId=%s"""
                       , (movie_id, ))
        movie = cursor.fetchone()
        if not movie:
            abort(404)
    
        movie = remove_null_fields(movie)


    with db_conn.cursor() as cursor:
        cursor.execute(" SELECT * FROM MoviesGenres WHERE movieId=%s", (movie_id, ))
        genres=cursor.fetchall()

    movie['genres']=[g['genre'] for g in genres]

    with db_conn.cursor() as cursor:
        cursor.execute("""SELECT
                            P.personId,
                            P.primaryName AS name,
                            P.birthYear,
                            P.deathYear,
                            MP.job,
                            MP.category AS role
                            FROM MoviesPeople MP
                            JOIN People P on P.personId = MP.personId
                            WHERE MP.movieId=%s""", (movie_id, ))
        actors=cursor.fetchall()
    
    movie['actors']=[a['name'] for a in actors]
    movie['actor'] = [remove_null_fields(p) for p in actors]


    db_conn.close() 
    return movie


MAX_PAGE_SIZE = 100

@app.route("/movies")
def movies():
    page = int(request.args.get('page', 0))
    page_size = int(request.args.get('page_size', MAX_PAGE_SIZE))
    page_size = min(page_size, MAX_PAGE_SIZE)

    db_conn = pymysql.connect(host="localhost", user="root",password='warp4818' ,database="bechdel",
                              cursorclass=pymysql.cursors.DictCursor)
    with db_conn.cursor() as cursor:
        cursor.execute("""
            SELECT * FROM Movies
            ORDER BY movieId
            LIMIT %s
            OFFSET %s
        """, (page_size, page * page_size))
        movies = cursor.fetchall()

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

@app.route("/people/<int:person_id>")
def people(person_id):
    db_conn = pymysql.connect(host="localhost",
                              user="root",
                              password="warp4818",
                              database="bechdel",
                              cursorclass=pymysql.cursors.DictCursor)
    with db_conn.cursor() as cursor:
        cursor.execute(""" select * from bechdel.People p where p.personId=%s """,(person_id,))
    people=cursor.fetchone()

    return people

@app.route("/people")
def peoples():
    page = int(request.args.get('page', 0))
    page_size = int(request.args.get('page_size', MAX_PAGE_SIZE))
    page_size = min(page_size, MAX_PAGE_SIZE)

    db_conn = pymysql.connect(host="localhost", user="root",password='warp4818' ,database="bechdel",
                              cursorclass=pymysql.cursors.DictCursor)
    with db_conn.cursor() as cursor:
        cursor.execute("""
            SELECT * FROM People p
            ORDER BY p.personId
            LIMIT %s
            OFFSET %s
        """, (page_size, page * page_size))
        movies = cursor.fetchall()

    with db_conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS total FROM People")
        total = cursor.fetchone()
        last_page = math.ceil(total['total'] / page_size)

    db_conn.close()
    return {
        'People': movies,
        'next_page': f'/People?page={page+1}&page_size={page_size}',
        'last_page': f'/People?page={last_page}&page_size={page_size}',
    }





#select *
#from bechdel.MoviesPeople  mp
#oin bechdel.people p on mp.personId=p.personId
#join bechdel.movies m on mp.movieId=m.movieId
#where mp.personId=%s
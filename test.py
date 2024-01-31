from flask import Flask, request
from flask_mysqldb import MySQL
from flask_cors import CORS
import pandas as pd
from json import loads

port = 8000
app = Flask(__name__)
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = ''
app.config['MYSQL_DB'] = 'sakila'
mysql = MySQL(app)
CORS(app)

@app.get("/top5/<type>")
def get_top_5_movies(type):
    cursor = mysql.connection.cursor()
    query = ''
    if(type == 'actors'):
        query = 'SELECT fa.actor_id, a.first_name, a.last_name\
            FROM film_actor AS fa join actor AS a ON fa.actor_id = a.actor_id\
            GROUP BY actor_id ORDER BY COUNT(film_id) DESC LIMIT 5'
    else:
        query = 'SELECT f.film_id, f.title FROM inventory AS i,\
            rental AS r, film AS f, film_actor AS fa, (SELECT actor_id, COUNT(film_id) AS num_films\
            FROM film_actor GROUP BY actor_id ORDER BY COUNT(film_id) DESC) AS top_actors WHERE\
            i.inventory_id = r.inventory_id AND i.film_id = f.film_id AND i.film_id = fa.film_id AND\
            fa.actor_id = top_actors.actor_id GROUP BY i.film_id, fa.actor_id ORDER BY top_actors.num_films\
            DESC, COUNT(i.film_id) DESC LIMIT 5'

    cursor.execute(query)
    result = cursor.fetchall()
    column_names = [i[0] for i in cursor.description]
    top_5_df = pd.DataFrame(data=result, columns=column_names)
    res = top_5_df.to_json(orient="split")
    parsed = loads(res)
    cursor.close()
    return parsed

@app.get("/movie/<movieId>")
def get_movie_info(movieId):
    cursor = mysql.connection.cursor()
    query = f'SELECT f.film_id, c.name AS category, f.rating, f.title, COUNT(i.film_id) AS rented, f.description, f.release_year,\
        f.rental_duration, f.rental_rate, f.length, f.replacement_cost, f.special_features\
        FROM inventory AS i, rental AS r, category AS c, film_category AS fc, film AS f\
        WHERE i.inventory_id = r.inventory_id AND i.film_id = f.film_id AND i.film_id = fc.film_id AND fc.category_id = c.category_id\
        GROUP BY i.film_id, fc.category_id HAVING i.film_id = {movieId}'    
    cursor.execute(query)
    result = cursor.fetchall()
    column_names = [i[0] for i in cursor.description]
    movie = pd.DataFrame(data=result, columns=column_names)
    res = movie.to_json(orient="split")
    parsed = loads(res)
    cursor.close()
    return parsed

@app.get("/actor/<actorId>")
def get_actor_info(actorId):
    cursor = mysql.connection.cursor()
    query = f'SELECT f.film_id, f.title, COUNT(i.film_id) AS rental_count\
        FROM inventory AS i, rental AS r, film AS f, film_actor AS fa, actor AS a\
        WHERE i.inventory_id = r.inventory_id AND i.film_id = f.film_id AND i.film_id = fa.film_id AND fa.actor_id = {actorId}\
        GROUP BY i.film_id, fa.actor_id\
        ORDER BY COUNT(i.film_id) DESC LIMIT 5'
    cursor.execute(query)
    result = cursor.fetchall()
    column_names = [i[0] for i in cursor.description]
    actor = pd.DataFrame(data=result, columns=column_names)
    res = actor.to_json(orient="split")
    parsed = loads(res)
    cursor.close()
    return parsed

if __name__ == '__main__':
    app.run(debug=True, port=8000)
'''
@app.get('/login')
def login_get():
    return show_the_login_form()

@app.post('/login')
def login_post():
    return do_the_login()
'''
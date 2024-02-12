from flask import Flask, request
from flask_mysqldb import MySQL
from flask_cors import CORS
from json import loads, dumps

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
    result = list(cursor.fetchall())
    res = dumps(result)
    parsed = loads(res)
    cursor.close()
    return parsed
# 
@app.get("/movie/rented/<movieId>")
def get_movie_w_rent_info(movieId):
    cursor = mysql.connection.cursor()
    query = f'SELECT f.film_id, c.name AS category, f.rating, f.title\
        , f.description, f.release_year, f.special_features, COUNT(i.film_id) AS rented\
        FROM inventory AS i, rental AS r, category AS c, film_category AS fc, film AS f\
        WHERE i.inventory_id = r.inventory_id AND i.film_id = f.film_id AND i.film_id = fc.film_id AND fc.category_id = c.category_id\
        GROUP BY i.film_id, fc.category_id HAVING i.film_id = {movieId}'    
    cursor.execute(query)
    result = list(cursor.fetchall())
    res = dumps(result)
    parsed = loads(res)
    cursor.close()
    return parsed

@app.get("/movie/<movieId>")
def get_movie_info(movieId):
    cursor = mysql.connection.cursor()
    query = f'SELECT f.film_id, c.name AS category, f.rating, f.title\
        , f.description, f.release_year, f.special_features\
        FROM category AS c, film_category AS fc, film AS f\
        WHERE f.film_id = fc.film_id AND fc.category_id = c.category_id\
        GROUP BY f.film_id, fc.category_id HAVING f.film_id = {movieId}'    
    cursor.execute(query)
    result = list(cursor.fetchall())
    res = dumps(result)
    parsed = loads(res)
    cursor.close()
    return parsed

@app.get("/actor/<actorId>")
def get_actor_info(actorId):
    cursor = mysql.connection.cursor()
    query = f'SELECT f.title, COUNT(i.film_id) AS rental_count\
        FROM inventory AS i, rental AS r, film AS f, film_actor AS fa\
        WHERE i.inventory_id = r.inventory_id AND i.film_id = f.film_id AND i.film_id = fa.film_id AND fa.actor_id = {actorId}\
        GROUP BY i.film_id\
        ORDER BY COUNT(i.film_id) DESC LIMIT 5'
    cursor.execute(query)
    result = list(cursor.fetchall())
    res = dumps(result)
    parsed = loads(res)
    cursor.close()
    return parsed

@app.get("/movie/info")
def get_movie_by_title():
    url_params = request.args
    title = url_params.get('title', '')
    actor = url_params.get('actor', '')
    genre = url_params.get('genre', '')
    cursor = mysql.connection.cursor()
    first, last ='',''
    if(' ' in actor):
        if(actor[0] == ' '):
            last = actor[1:]
        elif(actor[-1] == ' '):
            first = actor[:-1]
        else:
            fractured_name=actor.split()
            first = fractured_name[0]
            last = fractured_name[1]
    else:
        first = actor
    query = f'SELECT DISTINCT f.film_id, title, rating, description\
        FROM film AS f JOIN film_actor AS fa ON f.film_id = fa.film_id\
        JOIN actor AS a ON fa.actor_id = a.actor_id\
        JOIN film_category as fc ON f.film_id = fc.film_id\
        JOIN category AS c on fc.category_id = c.category_id\
        WHERE title LIKE \'{title}%\'\
        AND first_name LIKE \'{first}%\' AND last_name LIKE \'{last}%\'\
        AND name LIKE \'{genre}%\''
    cursor.execute(query)
    result = list(cursor.fetchall())
    res = dumps(result)
    parsed = loads(res)
    cursor.close()
    return parsed

@app.get("/customers/info")
def get_customer_id():
    cursor = mysql.connection.cursor()
    url_params = request.args
    id = int(url_params.get('id'))
    first = url_params.get('first', '')
    last = url_params.get('last', '')
    if(id == 0):
        query = f'SELECT customer_id, first_name, last_name, email, address, active\
        FROM customer AS c JOIN address AS a ON c.address_id=a.address_id\
        WHERE first_name LIKE \'{first}%\' AND last_name LIKE \'{last}%\''
    else:
        query = f'SELECT customer_id, first_name, last_name, email, address, active\
        FROM customer AS c JOIN address AS a ON c.address_id=a.address_id WHERE customer_id={int(id)}'
    print(query)
    cursor.execute(query)
    result = list(cursor.fetchall())
    res = dumps(result)
    parsed = loads(res)
    cursor.close()
    return parsed

@app.get("/rental/<id>")
def get_customer_rental_info(id):
    cursor = mysql.connection.cursor()
    query = f'SELECT COUNT(r.inventory_id) as DVDs\
        FROM customer AS c, rental AS r WHERE c.customer_id = r.customer_id AND c.customer_id = {int(id)}'
    cursor.execute(query)
    result = list(cursor.fetchall())
    # res = dumps(result, default=str)
    # parsed = loads(res)
    query = f'SELECT c.customer_id, c.first_name, c.last_name, COUNT(r.inventory_id) as DVDs\
        FROM customer AS c, rental AS r WHERE c.customer_id = r.customer_id AND c.customer_id = {int(id)} AND r.return_date IS NULL'
    result.append(list(cursor.fetchall()))
    res = dumps(result, default=str)
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
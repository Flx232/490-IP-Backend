from flask import Flask, request
from flask_mysqldb import MySQL
from flask_cors import CORS
from json import loads, dumps
from datetime import datetime
import random
import math

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

@app.get("/movie/total/<movieId>")
def get_movie_rental_total(movieId):
    cursor=mysql.connection.cursor()
    query = f'SELECT COUNT(DISTINCT r.inventory_id)\
        FROM inventory AS i, rental AS r WHERE i.inventory_id=r.inventory_id\
        AND film_id={movieId} AND r.inventory_id NOT IN (SELECT DISTINCT r.inventory_id\
        FROM inventory AS i JOIN rental AS r WHERE i.inventory_id=r.inventory_id\
        AND film_id={movieId} AND return_date IS NULL)'
    cursor.execute(query)
    result = list(cursor.fetchall())
    res=dumps(result)
    parsed=loads(res)
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
        query = f'SELECT customer_id, first_name, last_name, email, address, active, store_id\
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
    query = f'SELECT COUNT(r.inventory_id) as DVDs\
        FROM customer AS c, rental AS r WHERE c.customer_id = r.customer_id AND c.customer_id = {int(id)} AND r.return_date IS NULL'
    cursor.execute(query)
    result2 = cursor.fetchall()
    res = [[result[0][0], 0 if len(result2) == 0 else result2[0][0]]]
    res1 = dumps(res, default=str)
    parsed = loads(res1)
    cursor.close()
    return parsed

@app.post("/customer/add")
def handle_add_form():
    cursor = mysql.connection.cursor()
    seed = 0
    random.seed(seed)
    id = int(random.random()*math.pow(2,16))
    while True:
        query = f'SELECT customer_id FROM customer WHERE customer_id={id}'
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result) == 0:
            break
        seed += 1
        random.seed(seed)
        id = int(random.random()*math.pow(2,16))
    time = datetime.now()
    data = request.json
    first_name, last_name = data.get("first_name").upper(), data.get("last_name").upper()
    email = data.get("email")
    store_id = data.get("store")
    address_id = 0
    if len(data.get("address")) > 0:
        query = f'SELECT address_id FROM address WHERE address LIKE \'{data.get("address")}\''
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result) == 0:
            return "Invalid Address"
        address_id = result[0][0]
    query = f'INSERT INTO customer\
        (customer_id, store_id, first_name, last_name, email, address_id, active, create_date, last_update)\
        VALUES ({id}, {store_id}, \'{first_name}\', \'{last_name}\', \'{email}\', {address_id}, 1, \'{time}\', \'{time}\')'
    cursor.execute(query)
    cursor.execute('COMMIT')
    cursor.close()
    return data

@app.post("/movie/rent")
def handle_movie_rent():
    cursor = mysql.connection.cursor()
    seed = 0
    random.seed(seed)
    id = int(random.random()*math.pow(2,16))
    while True:
        query = f'SELECT rental_id FROM rental WHERE rental_id={id}'
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result) == 0:
            break
        seed += 1
        random.seed(seed)
        id = int(random.random()*math.pow(2,16))
    time = datetime.now()
    data = request.json
    customer_id=data.get("customer_id")
    film_id=data.get("film_id")
    staff_id=data.get("staff_id")
    query = f'SELECT DISTINCT r.inventory_id FROM inventory AS i, rental AS r\
        WHERE i.inventory_id=r.inventory_id AND film_id={film_id} AND r.inventory_id\
        NOT IN (SELECT DISTINCT r.inventory_id FROM inventory AS i JOIN rental AS r\
        WHERE i.inventory_id=r.inventory_id AND film_id={film_id} AND return_date IS NULL) LIMIT 1'
    cursor.execute(query)
    result = cursor.fetchall()
    inventory_id = result[0][0]
    query = f'INSERT INTO rental\
        (rental_id, rental_date, inventory_id, customer_id, staff_id, last_update)\
        VALUES ({id}, \'{time}\', \'{inventory_id}\', \'{customer_id}\', {staff_id}, \'{time}\')'
    cursor.execute(query)
    cursor.execute('COMMIT')
    cursor.close()
    return data

@app.patch("/customer/edit")
def handle_edit_form():
    cursor = mysql.connection.cursor()
    time = datetime.now()
    data = request.json
    address_str= ''
    store_id = f'store_id={data.get("store")}'
    activity = f'active={data.get("active")}'
    first_name = '' if data.get("first_name") == None else data.get("first_name")
    last_name = '' if data.get("last_name") == None else data.get("last_name")
    email = '' if data.get("email") == None else data.get("email")

    first_name_str = f', first_name=\'{first_name.upper()}\'' if (len(first_name) > 0) else ''
    last_name_str = f', last_name=\'{last_name.upper()}\'' if(len(last_name) > 0) else ''
    email_str = f', email=\'{email}\'' if len(email) > 0 else ''

    if len('' if data.get("address") == None else data.get("address")) > 0:
        query = f'SELECT address_id FROM address WHERE address LIKE \'{data.get("address")}\''
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result) == 0:
            return "Invalid Address"
        address_str = f', address_id={result[0][0]}'

    query = f'UPDATE customer\
        SET last_update=\'{time}\', {store_id}, {activity}{first_name_str}{last_name_str}{email_str}{address_str}\
        WHERE customer_id={data.get("id")}'
    cursor.execute(query)
    cursor.execute('COMMIT')
    cursor.close()
    return data

@app.delete("/customer/remove/<id>")
def handle_remove_customer(id):
    cursor = mysql.connection.cursor()
    query = f'DELETE FROM customer WHERE customer_id={id}'
    cursor.execute(query)
    cursor.execute('COMMIT')
    cursor.close()
    return id

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
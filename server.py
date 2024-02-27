from flask import Flask, request
from flask_mysqldb import MySQL
from flask_cors import CORS
from json import loads, dumps
from datetime import datetime
import random
import math
import re

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
        query = f'SELECT customer_id, first_name, last_name, email, address,\
        active, store_id, phone, country, city, district, postal_code\
        FROM customer AS c JOIN address AS a ON c.address_id=a.address_id JOIN\
        city AS ci ON a.city_id = ci.city_id JOIN country as co ON ci.country_id = co.country_id\
        WHERE first_name LIKE \'{first}%\' AND last_name LIKE \'{last}%\''
    else:
        query = f'SELECT customer_id, first_name, last_name, email, address,\
        active, store_id, phone, country, city, district, postal_code\
        FROM customer AS c JOIN address AS a ON c.address_id=a.address_id JOIN\
        city AS ci ON a.city_id = ci.city_id JOIN country as co ON ci.country_id = co.country_id\
        WHERE customer_id={int(id)}'
    cursor.execute(query)
    result = list(cursor.fetchall())
    res = dumps(result)
    parsed = loads(res)
    cursor.close()
    return parsed

@app.get("/rental/<id>")
def get_customer_rental_info(id):
    cursor = mysql.connection.cursor()
    query = f'SELECT COUNT(inventory_id) as DVDs\
        FROM rental WHERE customer_id = {int(id)}'
    cursor.execute(query)
    result = list(cursor.fetchall())
    # res = dumps(result, default=str)
    # parsed = loads(res)
    query = f'SELECT COUNT(inventory_id) as DVDs\
        FROM rental WHERE customer_id = {int(id)} AND return_date IS NULL'
    cursor.execute(query)
    result2 = cursor.fetchall()
    res = [[result[0][0], 0 if len(result2) == 0 else result2[0][0]]]
    res1 = dumps(res, default=str)
    parsed = loads(res1)
    cursor.close()
    return parsed

@app.get("/rentals/movie/<id>")
def get_customer_movie_rent_list(id):
    cursor = mysql.connection.cursor()
    query = f'SELECT r.customer_id, first_name, last_name, email\
        FROM rental AS r, customer AS c, inventory as i WHERE i.inventory_id = r.inventory_id\
        AND c.customer_id = r.customer_id AND i.film_id = {id} AND return_date IS NULL'
    cursor.execute(query)
    result = list(cursor.fetchall())
    res = dumps(result, default=str)
    parsed = loads(res)
    cursor.close()
    return parsed

@app.get("/customer/rent_hist/<id>")
def get_rent_hist(id):
    cursor = mysql.connection.cursor()
    query = f'SELECT DISTINCT f.film_id, title, rating, description, rental_date, return_date FROM inventory AS i,\
    rental AS r, film as f WHERE r.inventory_id = i.inventory_id AND f.film_id = i.film_id AND r.customer_id = {int(id)} AND return_date IS NOT NULL'
    cursor.execute(query)
    result1 = list(cursor.fetchall())
    query = f'SELECT DISTINCT f.film_id, title, rating, description, rental_date FROM inventory AS i,\
    rental AS r, film as f WHERE r.inventory_id = i.inventory_id AND f.film_id = i.film_id AND r.customer_id = {int(id)}\
    AND return_date IS NULL'
    cursor.execute(query)
    result2 = list(cursor.fetchall())
    result = [result1, result2]
    res = dumps(result, default=str)
    parsed = loads(res)
    cursor.close()
    return parsed

@app.post("/customer/add")
def handle_add_form():
    cursor = mysql.connection.cursor()
    data = request.json
    email = data.get("email")
    query = f'SELECT customer_id FROM customer WHERE email LIKE \'{email}\''
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) > 0:
        return "same email"
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
    first_name, last_name = data.get("first_name").upper(), data.get("last_name").upper()
    store_id = data.get("store")
    address_id = add_address(data)
    if(address_id == "invalid postal"): return "invalid postal"
    if(address_id == "invalid phone"): return "invalid phone"
    query = f'INSERT INTO customer\
        (customer_id, store_id, first_name, last_name, email, address_id, active, create_date, last_update)\
        VALUES ({id}, {store_id}, \'{first_name}\', \'{last_name}\', \'{email}\', {address_id}, 1, \'{time}\', \'{time}\')'
    cursor.execute(query)
    cursor.execute('COMMIT')
    cursor.close()
    return data

def add_city(data):
    cursor = mysql.connection.cursor()
    city_id = 0
    city = data.get("city")
    query = f'SELECT city_id FROM city WHERE city LIKE \'{city}\''
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) > 0:
        city_id = result[0][0]
    else:
        seed = 0
        city_id = int(random.random()*math.pow(2,16))
        while True:
            query = f'SELECT city_id FROM city WHERE city_id={city_id}'
            cursor.execute(query)
            result = cursor.fetchall()
            if len(result) == 0:
                break
            seed += 1
            random.seed(seed)
            city_id = int(random.random()*math.pow(2,16))

        country = data.get("country")
        country_id = 0
        query = f'SELECT country_id FROM country WHERE country LIKE \'{country}\''
        cursor.execute(query)
        result = cursor.fetchall()
        if(len(result) > 0):
            country_id = result[0][0]
        else:
            seed = 0
            country_id = int(random.random()*math.pow(2,16))
            while True:
                query = f'SELECT country_id FROM country WHERE country_id={country_id}'
                cursor.execute(query)
                result = cursor.fetchall()
                if len(result) == 0:
                    break
                seed += 1
                random.seed(seed)
                country_id = int(random.random()*math.pow(2,16))
            time = datetime.now()
            query = f'INSERT INTO country (country_id, country, last_update) VALUES ({country_id}, \'{country}\', \'{time}\')'
            cursor.execute(query)
            cursor.execute('COMMIT')
        time = datetime.now()
        query = f'INSERT INTO city (city_id, city, country_id, last_update) VALUES ({city_id}, \'{city}\', {country_id}, \'{time}\')'
        cursor.execute(query)
        cursor.execute('COMMIT')
    return city_id

def add_address(data):
    address_id = 0
    cursor = mysql.connection.cursor()
    postal = data.get("postal")
    address = data.get("address")
    if(len(postal)>0):
        query = f'SELECT address_id FROM address WHERE postal_code LIKE \'{postal}\' AND address NOT LIKE \'{address}\''
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result) > 0:
            return "invalid postal"
    
    phone = data.get("phone")
    query = f'SELECT address_id FROM address WHERE phone LIKE \'{phone}\' AND address NOT LIKE \'{address}\''
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) > 0:
        return "invalid phone"

    query = f'SELECT address_id, city_id FROM address WHERE address LIKE \'{address}\''
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) > 0:
        address_id=result[0][0]
        query = f'SELECT city_id, country_id FROM city WHERE city LIKE \'{data.get("city")}\' AND city_id = {result[0][1]}'
        cursor.execute(query)
        result = cursor.fetchall()
        if(len(result) > 0):
            query = f'SELECT country_id FROM country WHERE country LIKE \'{data.get("country")}\' AND country_id = {result[0][1]}'
            cursor.execute(query)
            result = cursor.fetchall()
            if(len(result)>0):
                return address_id
    seed = 0
    address_id = int(random.random()*math.pow(2,16))
    while True:
        query = f'SELECT address_id FROM address WHERE address_id={address_id}'
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result) == 0:
            break
        seed += 1
        random.seed(seed)
        address_id = int(random.random()*math.pow(2,16))
    city_id = add_city(data)
    query = ''
    time = datetime.now()
    location = 'POINT(0 0)'
    if(len(postal)==0):
        query = f'INSERT INTO address (address_id, address, district, city_id, phone, location, last_update)\
        VALUES ({address_id}, \'{address}\', \'{data.get("district")}\', {city_id}, {phone},\
        ST_GeomFromText(\'{location}\'), \'{time}\')'
    else:
        query = f'INSERT INTO address (address_id, address, district, city_id, postal_code, phone, location, last_update)\
        VALUES ({address_id}, \'{address}\', \'{data.get("district")}\', {city_id}, {postal}, {phone},\
        ST_GeomFromText(\'{location}\'), \'{time}\')'
    cursor.execute(query)
    cursor.execute('COMMIT')
    return address_id

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

@app.patch("/return/movie")
def handle_remove_movie_rent():
    cursor = mysql.connection.cursor()
    data = request.json
    customer_id=data.get("customer_id")
    film_id=data.get("film_id")
    time = datetime.now()
    query = f'SELECT r.inventory_id FROM inventory AS i, rental AS r WHERE film_id = {film_id}\
    AND customer_id = {customer_id} AND return_date IS NULL LIMIT 1'
    cursor.execute(query)
    result = cursor.fetchall()
    inventory_id = result[0][0]
    query = f'UPDATE rental SET last_update=\'{time}\', return_date=\'{time}\' WHERE inventory_id={inventory_id}\
    AND customer_id = {customer_id}'
    cursor.execute(query)
    cursor.execute('COMMIT')
    cursor.close()
    return data

def edit_address(data):
    address_id = 0
    cursor = mysql.connection.cursor()
    postal = data.get("postal")
    address = data.get("address")
    if(len(postal)>0):
        query = f'SELECT address_id FROM address WHERE postal_code LIKE \'{postal}\' AND address NOT LIKE \'{address}\''
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result) > 0:
            return "invalid postal"
    
    phone = data.get("phone")
    query = f'SELECT address_id FROM address WHERE phone LIKE \'{phone}\' AND address NOT LIKE \'{address}\''
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) > 0:
        return "invalid phone"

    query = f'SELECT address_id, city_id FROM address WHERE address LIKE \'{address}\''
    cursor.execute(query)
    result = cursor.fetchall()
    city_id = 0
    if len(result) > 0:
        city_id = result[0][1]
        address_id=result[0][0]
        query = f'SELECT city_id, country_id FROM city WHERE city LIKE \'{data.get("city")}\' AND city_id = {result[0][1]}'
        cursor.execute(query)
        result = cursor.fetchall()
        if(len(result) > 0):
            query = f'SELECT country_id FROM country WHERE country LIKE \'{data.get("country")}\' AND country_id = {result[0][1]}'
            cursor.execute(query)
            result = cursor.fetchall()
            if(len(result) == 0):
                city_id = add_city(data)
        else:city_id = add_city(data)
    else:city_id = add_city(data)
    district = data.get("district")
    query = f'SELECT address_id FROM address WHERE postal_code LIKE \'{postal}\'\
        AND phone LIKE \'{phone}\' AND address LIKE \'{address}\'\
        AND city_id={city_id} AND district LIKE\'{district}\'' if(len(postal)>0) else f'SELECT address_id\
        FROM address WHERE  phone LIKE \'{phone}\' AND address LIKE \'{address}\'\
        AND city_id={city_id} AND district LIKE\''
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) > 0:
        return result[0][0]
    time = datetime.now()
    if(len(postal)==0):
        query = f'UPDATE address SET address=\'{address}\', district=\'{district}\',\
        city_id={city_id}, phone=\'{phone}\', last_update=\'{time}\' WHERE address_id={address_id}'
    else:
        query = f'UPDATE address SET address=\'{address}\', district=\'{data.get("district")}\',\
        city_id={city_id}, postal_code=\'{postal}\', phone=\'{phone}\', last_update=\'{time}\' WHERE address_id={address_id}'
    cursor.execute(query)
    cursor.execute('COMMIT')
    return address_id

@app.patch("/customer/edit")
def handle_edit_form():
    cursor = mysql.connection.cursor()
    data = request.json
    id = data.get("id")
    email = data.get("email")
    query = f'SELECT customer_id FROM customer WHERE email LIKE \'{email}\''
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) > 0 and result[0][0] != id:
        return "same email"
    time = datetime.now()
    first_name, last_name, store_id = data.get("first_name").upper(), data.get("last_name").upper(), data.get("store")
    active = data.get("active")
    address = edit_address(data)
    query = f'SELECT customer_id FROM customer WHERE\
        store_id={store_id} AND active={active} AND\
        first_name LIKE \'{first_name}\' AND last_name LIKE\'{last_name}\' AND\
        email LIKE \'{email}\' AND address_id={address}'
    cursor.execute(query)
    if len(result) > 0 and result[0][0] == id:
        query = f'SELECT * FROM address where address_id={address}'
        cursor.execute(query)
        result = cursor.fetchall()
        res = dumps(result, default=str)
        parsed = loads(res)
        cursor.close()
        return parsed
    query = f'UPDATE customer\
        SET last_update=\'{time}\', store_id={store_id}, active={active},\
        first_name=\'{first_name}\', last_name=\'{last_name}\',\
        email=\'{email}\', address_id=\'{address}\' WHERE customer_id={id}'
    cursor.execute(query)
    cursor.execute('COMMIT')
    cursor.close()
    return data

@app.delete("/customer/remove/<id>")
def handle_remove_customer(id):
    cursor = mysql.connection.cursor()
    query = f'SELECT COUNT(inventory_id) as DVDs\
        FROM rental WHERE customer_id = {id} AND return_date IS NULL'
    cursor.execute(query)
    result = list(cursor.fetchall())
    if(result[0][0] == 0):
        query = f'DELETE FROM rental WHERE customer_id ={id}'
        cursor.execute(query)
        cursor.execute('COMMIT')
        query = f'DELETE FROM customer WHERE customer_id={id}'
        cursor.execute(query)
        cursor.execute('COMMIT')
        cursor.close()
        return id
    cursor.close()
    return "invalid"

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
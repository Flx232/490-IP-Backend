from flask import Flask, request
from flask_mysqldb import MySQL
from flask_cors import CORS
import pandas as pd
from json import loads

port = 8000
app = Flask(__name__)
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'root'
app.config['MYSQL_DB'] = 'sakila'
mysql = MySQL(app)
CORS(app)

@app.route("/")
def get_top_5():
    cursor = mysql.connection.cursor()
    query = 'SELECT f.film_id, f.title, COUNT(i.film_id) AS rental_count FROM inventory AS i,\
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
    return(
        parsed
    )

@app.route("/user/<username>")
def user(username):
    return f'{username} test'

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        return do_the_login()
    else:
        return show_the_login_form()
    
def do_the_login():
    return "Logged in"

def show_the_login_form():
    return "Please log in"

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
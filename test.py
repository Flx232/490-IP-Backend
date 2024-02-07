import pandas as pd
import mysql.connector
first='a'
last='b'
id=1

try:
    cnx = mysql.connector.connect(host='localhost', user='root', password='')
    cursor = cnx.cursor(buffered=True)
    cursor.execute('USE sakila')
    query = f'SELECT title, rental_date, return_date\
        FROM rental AS r JOIN inventory AS i ON r.inventory_id=i.inventory_id\
        JOIN film AS f ON i.film_id=f.film_id\
        WHERE customer_id = {int(id)};'
    cursor.execute(query)
    result = list(cursor.fetchall())
    for i in result:
        modify_list = list(i)
        modify_list[1] = modify_list[1].strftime("%Y-%m-%d %H:%M:%S")
        modify_list[2] = modify_list[2].strftime("%Y-%m-%d %H:%M:%S")
        i = tuple(modify_list)
        
    for i in result:
        print(i)
except mysql.connector.Error as err:
    print(err)
finally:
    try:
        cnx
    except NameError:
        pass
    else:
        cnx.close()

import pandas as pd
import mysql.connector
filter = 'a'

try:
    cnx = mysql.connector.connect(host='localhost', user='root', password='')
    cursor = cnx.cursor(buffered=True)
    cursor.execute('USE sakila')
    query = f'SELECT film_id, title FROM film WHERE title LIKE \'{filter.upper()}%\''
    cursor.execute(query)
    result = list(cursor.fetchall())
    column = tuple([i[0] for i in cursor.description])
    result.insert(0, column)
    print(result)
except mysql.connector.Error as err:
    print(err)
finally:
    try:
        cnx
    except NameError:
        pass
    else:
        cnx.close()

import pandas as pd
import mysql.connector
import random
import math
first='a'
last='b'
id=1

try:
    cnx = mysql.connector.connect(host='localhost', user='root', password='')
    cursor = cnx.cursor(buffered=True)
    cursor.execute('USE sakila')
    query = f'SELECT address_id FROM address WHERE postal_code LIKE \'123123\' AND address NOT LIKE \'34 Main Street\'\
        AND district NOT LIKE \'New Jersey\' AND city_id NOT IN (SELECT c.city_id FROM city AS c AND country AS co WHERE\
        c.country_id = co.country_id AND city LIKE \'Newark\' AND country LIKE \'United States\')'
    cursor.execute(query)
    result = cursor.fetchall()
    print(result)
    cursor.close()
except mysql.connector.Error as err:
    print(err)
finally:
    try:
        cnx
    except NameError:
        pass
    else:
        cnx.close()

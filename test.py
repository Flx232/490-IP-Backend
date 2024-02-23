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
    query = f'SELECT DISTINCT r.inventory_id FROM inventory AS i, rental AS r\
        WHERE i.inventory_id=r.inventory_id AND film_id=1 AND r.inventory_id\
        NOT IN (SELECT DISTINCT r.inventory_id FROM inventory AS i JOIN rental AS r\
        WHERE i.inventory_id=r.inventory_id AND film_id=1 AND return_date IS NULL) LIMIT 1'
    cursor.execute(query)
    result = cursor.fetchall()
    print(result[0][0])
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

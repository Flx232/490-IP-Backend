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
    query = f'SELECT customer_id FROM customer WHERE email LIKE \"MARY.SMITH@sakilacustomer.org\"'
    cursor.execute(query)
    result = cursor.fetchall()
    print(len(result))
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

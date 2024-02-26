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
    query = f'SELECT location FROM address WHERE address LIKE \'47 MySakila Drive\''
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) > 0:
        print(result)
    else:
        print("hi")
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

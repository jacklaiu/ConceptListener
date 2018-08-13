import pymysql.cursors

# Connect to the database 95.163.200.245
connection = pymysql.connect(host='95.163.200.245',
                             user='root',
                             password='queue11235813',
                             db='conceptlistener',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

# connection = pymysql.connect(host='localhost',
#                              user='root',
#                              password='123456',
#                              db='conceptlistener',
#                              charset='utf8mb4',
#                              cursorclass=pymysql.cursors.DictCursor)

def updatemany(sql, arr_values):
    cursor = None
    try:
        with connection.cursor() as cursor:
            # Create a new record
            cursor.executemany(sql, arr_values)
        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()
    finally:
        cursor.close()

def update(sql, values):
    cursor = None
    try:
        with connection.cursor() as cursor:
            # Create a new record
            cursor.execute(sql, values)
        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()
    finally:
        cursor.close()

def select(sql, values):
    cursor = None
    try:
        with connection.cursor() as cursor:
            # Read a single record
            cursor.execute(sql, values)
            result = cursor.fetchall()
            return result

    finally:
        cursor.close()

def selectmany(sql, arr_values):
    cursor = None
    try:
        with connection.cursor() as cursor:
            # Read a single record
            cursor.executemany(sql, arr_values)
            result = cursor.fetchall()
            return result

    finally:
        cursor.close()













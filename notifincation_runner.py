from mysql.connector import MySQLConnection, Error
import requests

import config
import notification

def sms():    
    sql = "SELECT id, message, receiver FROM sms WHERE result IS NULL"

    db_config = config.db_config
    conn = MySQLConnection(**db_config)

    cursor = conn.cursor()

    cursor.execute(sql)
    result = cursor.fetchall()

    print(result)

    for x in result:
        result = notification.sendSMS(x[0], x[1], x[2])
        print(result)

        

# sms()
# notification.insertSMS('This is an error', '02102982776')

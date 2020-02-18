import pandas as pd
import os
from mysql.connector import MySQLConnection

import functions
import config


def countCSV(file_path):
    functions.log('### SIZE : ' + str(os.path.getsize(file_path)))

    column_names = ['time', 'z_p', 'z_f', 'z_t',
                            'x_p', 'x_f', 'x_t',
                            'y_p', 'y_f', 'y_t',
                            'v_p', 'v_f', 'v_t']
    target_cols = [0, 1, 2, 3,
                   4, 5, 6,
                   7, 8, 9,
                   10, 11, 12]

    csv = pd.read_csv(file_path, skiprows=2, delimiter=',',
                      names=column_names, usecols=target_cols)

    return int(csv['time'].count())


def parseFileName(file_path):
    array = file_path.split('_')
    array[0] = array[0][array[0].find("DIN")+3:]
    array[1] = array[1][0:array[1].find(".")]
    array.append(array[0][array[0].find("DIN")+3:])

    return array


def storeDB(date, device_id, count):
    query = "INSERT INTO din(date, device_id, count) VALUES(%s, %s, %s)"
    args = (date, device_id, count, )
    din_id = 0

    db_config = config.db_config
    conn = MySQLConnection(**db_config)

    try:
        cursor = conn.cursor()
        cursor.execute(query, args)

        if cursor.lastrowid:
            print('last insert id', cursor.lastrowid)
            din_id = int(cursor.lastrowid or 0)
        else:
            raise Exception('last insert id not found')

        conn.commit()
    except Exception as exception:
        print('Exception: %s' % exception)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

    return din_id


def rollbackDB(id):
    query = "DELETE FROM din WHERE id = %s"
    args = (id,)

    try:
        db_config = config.db_config
        conn = MySQLConnection(**db_config)

        cursor = conn.cursor()
        cursor.execute(query, args)
    except Exception as exception:
        print('Exception: %s' % exception)
    finally:
        cursor.close()
        conn.close()

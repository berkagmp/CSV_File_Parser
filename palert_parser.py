import pandas as pd
import os
import csv
from mysql.connector import MySQLConnection

import config
import functions


def getDatetimeAndDeviceId(file_path):
    with open(file_path, newline='') as f:
        reader = csv.reader(f)

        for row in reader:
            if(row[0].startswith("#StationCode")):
                device_id = row[0].replace('#StationCode:', '')
            elif(row[0].startswith("#StartTime")):
                datetime = row[0].replace('#StartTime:', '')
                array = datetime.split(' ')
                # date = array[0].replace('/', '')
                date = functions.formatDate(array[0])
                time = functions.formatTime(array[1])
                break

    return [date, time, device_id]


def countCSV(file_path, skiprow):
    # functions.log('### SIZE : ' + str(os.path.getsize(file_path)))

    csv = pd.read_csv(file_path, skiprows=skiprow, delimiter=',', names=[
        'centisecond', 'acc_a', 'acc_b', 'acc_c', 'pd', 'dis'])

    return int(csv['centisecond'].count())


def parseFileName(file_path):
    array = file_path.split('_')
    array[0] = array[0][array[0].rfind("/")+1:]
    array[1] = array[1][0:array[1].find(".")]
    array.append(array[0][0:8])
    array.append(array[0][8:])

    return array


def parseFileNameForEqEvent(file_path):
    array = file_path.split('_')
    array[4] = array[4][0:array[4].find(".")]
    array.append(array[4][0:8])
    array.append(array[4][8:])

    return array


def storeDB(date, time, device_id, file_type, count, filedatetime):
    query = "INSERT INTO palert(date, time, device_id, type, count, filedatetime) VALUES(%s, %s, %s, %s, %s, %s)"
    args = (date, time, device_id, file_type, count, filedatetime, )
    palert_id = 0

    db_config = config.db_config
    conn = MySQLConnection(**db_config)

    try:
        cursor = conn.cursor()
        cursor.execute(query, args)

        if cursor.lastrowid:
            print('last insert id', cursor.lastrowid)
            palert_id = int(cursor.lastrowid or 0)
        else:
            raise Exception('last insert id not found')

        conn.commit()
    except Exception as exception:
        print('Exception: %s' % exception)
        conn.rollback()
    finally:
        cursor.close()

    return palert_id


def rollbackDB(id):
    query = "DELETE FROM palert WHERE id = %s"
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

import pandas as pd
import math
import os
import csv
from mysql.connector import MySQLConnection, Error

import functions
import config


# Get Date and Time from inside the file
# #Start Time: 2019/09/03 22:32:09
def getDatetime(file_path):
    with open(file_path, newline='') as f:
        reader = csv.reader(f)

        for row in reader:
            datetime = row
            array = datetime[0].split(' ')
            date = array[2].replace('/', '')
            time = array[3].replace(':', '')

            break

    return [date, time]

# Get total count of the RSHD data (excluding RSHD RESULT)


def countCSV(file_path):
    functions.log('### SIZE : ' + str(os.path.getsize(file_path)))

    column_names = ['time', 'acc_a', 'acc_b',
                    'acc_c', 'dis_a', 'dis_b', 'dis_c']
    target_cols = [[0, 1, 2, 3, 4, 5, 6],
                   [0, 7, 8, 9, 10, 11, 12],
                   [0, 13, 14, 15, 16, 17, 18]]

    csv = pd.read_csv(file_path, skiprows=6,
                      delimiter=',', names=column_names, usecols=target_cols[0])

    return int(csv['time'].count())

# Delete the row in RSHD table


def rollbackDB(id):
    query = "DELETE FROM rshd WHERE id = %s"
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


def rshd_alert(type):
    print(type)


def checkData(rshd_result, device_id):

    sql = ""
    sql += "select x_warning_value, y_warning_value, x_danger_value, y_danger_value from rshd_standard"
    sql += "  join reports on reports.id = rshd_standard.report_id"
    sql += "  join devices on devices.id = reports.device_id"
    sql += " where devices.id = %s"

    db_config = config.db_config
    conn = MySQLConnection(**db_config)

    cursor = conn.cursor()
    params = (device_id,)

    cursor.execute(sql, params)
    result = cursor.fetchall()

    print(result)
    # result: x_warning_value, y_warning_value, x_danger_value, y_danger_value

    for x in result:
        x_warning_value = float(x[0])
        y_warning_value = float(x[1])
        x_danger_value = float(x[2])
        y_danger_value = float(x[3])

    stiffeness_roof_ground_b = float(rshd_result[0][1])
    stiffeness_roof_ground_c = float(rshd_result[0][2])
    stiffeness_second_ground_b = float(rshd_result[1][1])
    stiffeness_second_ground_c = float(rshd_result[1][2])

    if(x_danger_value >= stiffeness_roof_ground_b
       or y_danger_value >= stiffeness_roof_ground_c
       or x_danger_value >= stiffeness_second_ground_b
       or y_danger_value >= stiffeness_second_ground_c):
        rshd_alert('danger')
    elif(x_warning_value >= stiffeness_roof_ground_b
         or y_warning_value >= stiffeness_roof_ground_c
         or x_warning_value >= stiffeness_second_ground_b
         or y_warning_value >= stiffeness_second_ground_c):
        rshd_alert('warning')


def insert_rshd_result(result, datetime, device_id, count):
    query = "INSERT INTO rshd(date, time"
    query += ",stiffeness_roof_ground_a,stiffeness_roof_ground_b,stiffeness_roof_ground_c"
    query += ",stiffeness_second_ground_a,stiffeness_second_ground_b,stiffeness_second_ground_c"
    query += ",naturalFrequency_roof_ground_a,naturalFrequency_roof_ground_b,naturalFrequency_roof_ground_c"
    query += ",naturalFrequency_second_ground_a,naturalFrequency_second_ground_b,naturalFrequency_second_ground_c"
    query += ",displacement_roof_ground_a,displacement_roof_ground_b,displacement_roof_ground_c"
    query += ",displacement_second_ground_a,displacement_second_ground_b,displacement_second_ground_c"
    query += ",rsquare_roof_ground_a,rsquare_roof_ground_b,rsquare_roof_ground_c"
    query += ",rsquare_second_ground_a,rsquare_second_ground_b,rsquare_second_ground_c"
    query += ", device_id, count) "
    query += "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    args = (datetime[0],
            datetime[1],
            result[0][0],
            result[0][1],  # stiffeness_roof_ground_b
            result[0][2],  # stiffeness_roof_ground_c
            result[1][0],
            result[1][1],  # stiffeness_second_ground_b
            result[1][2],  # stiffeness_second_ground_c
            result[0][3],
            result[0][4],
            result[0][5],
            result[1][3],
            result[1][4],
            result[1][5],
            result[0][6],
            result[0][7],
            result[0][8],
            result[1][6],
            result[1][7],
            result[1][8],
            result[0][9],
            result[0][10],
            result[0][11],
            result[1][9],
            result[1][10],
            result[1][11],
            device_id,
            count,
            )
    rshd_id = 0

    try:
        db_config = config.db_config
        conn = MySQLConnection(**db_config)

        cursor = conn.cursor()
        cursor.execute(query, args)

        if cursor.lastrowid:
            print('last insert id', cursor.lastrowid)
            rshd_id = int(cursor.lastrowid or 0)
        else:
            print('last insert id not found')

        conn.commit()
    except Exception as exception:
        print('Exception: %s' % exception)
    finally:
        cursor.close()
        conn.close()

    return rshd_id


def readFile(file_path):
    array = []

    try:
        f = open(file_path, 'r', encoding='UTF8')

        if len(f.read()) == 0:
            raise Exception('FILE IS EMPTY')
        else:
            f.seek(0)
            for line in f.readlines():
                if "#Data Format" in line:
                    break
                elif ("#Start Time" in line) | ("#Device ID" in line) | ("#RSHD result" in line):
                    continue
                else:
                    data = (line[line.find(":")+1:]).strip()
                    data = data.split(',')
                    data = list(filter(None, data))

                    array.append(data)

        for arr in array:
            for data in arr:
                if(math.isnan(float(data))):
                    raise Exception('The file is broken')

        return array
    except Exception as e:
        # print('Error: {}'.format(str(e)))
        raise e
    finally:
        f.close()


# Get Information form Filename
# e.g.) ['../loaddata/RSHD/C10129/RSHDEVENT', 'C10129', '20190903', '223209']
def parseFileName(file_path):
    array = file_path.split('_')
    array[3] = array[3][0:array[3].find(".")]

    return array


# Insert RSHD result to DB
def rshd_result(file_path, device_id, datetime, count):
    print("file_path: %s" % (file_path))
    rshd_result = readFile(file_path)
    rshd_id = insert_rshd_result(rshd_result, datetime, device_id, count)

    checkData(rshd_result, device_id)

    return rshd_id

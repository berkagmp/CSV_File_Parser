import pandas as pd
import os
from mysql.connector import MySQLConnection

import functions
import config


def countCSV(file_path, device_id):
    functions.log('### SIZE : ' + str(os.path.getsize(file_path)))

    csv = pd.read_csv(file_path, sep=',', names=[
        'time', 'h_g', 'h_d', 'v_g', 'v_d'])

    m_h_g = csv['h_g'].max()
    m_h_d = csv['h_d'].max()
    m_v_g = csv['v_g'].max()
    m_v_d = csv['v_d'].max()

    print(m_h_g)
    print(m_h_d)
    print(m_v_g)
    print(m_v_d)

    standard = functions.getStructureType(device_id, 'I')
    status = 1

    if(m_h_d >= standard[0] or m_v_d >= standard[1] or (m_h_g * 0.1) >= standard[2] or (m_v_g * 0.1) >= standard[3]):
        status = 0

    return [int(csv['time'].count()), status]


def parseFileName(file_path):
    array = file_path.split('_')
    array[4] = array[4][0:array[4].find(".")]

    return array


def storeDB(date, device_id, count, status):
    query = "INSERT INTO iso(date, device_id, count, status) VALUES(%s, %s, %s, %s)"
    args = (date, device_id, count, status)
    iso_id = 0

    try:
        db_config = config.db_config
        conn = MySQLConnection(**db_config)

        cursor = conn.cursor()
        cursor.execute(query, args)

        if cursor.lastrowid:
            print('last insert id', cursor.lastrowid)
            iso_id = int(cursor.lastrowid or 0)
        else:
            raise Exception('last insert id not found')

        conn.commit()
    except Exception as exception:
        print('Exception: %s' % exception)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

    return iso_id


def rollbackDB(id):
    query = "DELETE FROM iso WHERE id = %s"
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

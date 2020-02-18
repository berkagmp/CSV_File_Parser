from mysql.connector import MySQLConnection
from parserLogger import logger

import config

db_config = config.db_config


def log(str):
    print(str)
    logger.warning(str)


def formatDate(date):
    arr = date.split('/')

    for i in range(len(arr)):
        if(len(arr[i]) < 2):
            arr[i] = '0' + arr[i]

    return ''.join(arr)


def formatTime(time):
    if(time.find(".") > -1):
        time = time[0:time.find(".")
                    ]

    arr = time.split(':')

    for i in range(len(arr)):
        if(len(arr[i]) < 2):
            arr[i] = '0' + arr[i]

    return ''.join(arr)


def getStructureType(device_id, report_type):
    query = ""
    query += "SELECT reports.s_type FROM reports "
    query += " WHERE device_id = (SELECT devices.id FROM devices WHERE devices.broadcast_serial = %s) "
    query += "   AND reports.type = %s "

    args = (device_id, report_type, )

    conn = MySQLConnection(**db_config)

    try:
        cursor = conn.cursor()
        cursor.execute(query, args)

        result = cursor.fetchall()

        for x in result:
            report_type = x[0]

        warning = [[73.979, 80, 76.901, 86.02, 92.041],  # db H
                   [71.126, 77.025, 73.979, 82.922, 89.247],  # db V
                   [0.050, 0.100, 0.070, 0.200, 0.400],  # gal H
                   [0.036, 0.071, 0.050, 0.140, 0.290]]  # gal V
    except Exception as exception:
        print('Exception: %s' % exception)
    finally:
        cursor.close()
        conn.close()

    return [warning[0][report_type], warning[1][report_type], warning[2][report_type], warning[3][report_type]]


def getReportID(serial_no, report_type):
    query = ""
    query += "SELECT reports.id FROM reports"
    query += "  JOIN devices ON devices.id = reports.device_id"
    query += " WHERE devices.broadcast_serial = %s"
    query += "   AND reports.type = %s"

    args = (serial_no, report_type)

    conn = MySQLConnection(**db_config)

    try:
        cursor = conn.cursor()
        cursor.execute(query, args)

        result = cursor.fetchall()

        for x in result:
            report_id = x[0]
    except Exception as exception:
        print('Exception: %s' % exception)
    finally:
        cursor.close()
        conn.close()

    return report_id


def getDeviceID(serial_no):
    query = "SELECT devices.id FROM devices WHERE devices.broadcast_serial = %s "
    args = (serial_no, )

    conn = MySQLConnection(**db_config)

    try:
        cursor = conn.cursor()
        cursor.execute(query, args)

        result = cursor.fetchall()

        for x in result:
            device_id = x[0]
    except Exception as exception:
        print('Exception: %s' % exception)
    finally:
        cursor.close()
        conn.close()

    return device_id


def getAllInfo(serial_no):
    query = "SELECT report_type, p_name, s_name FROM device_report WHERE broadcast_serial = %s "
    args = (serial_no, )

    conn = MySQLConnection(**db_config)

    try:
        cursor = conn.cursor()
        cursor.execute(query, args)

        result = cursor.fetchall()
    except Exception as exception:
        print('Exception: %s' % exception)
    finally:
        cursor.close()
        conn.close()

    return result


def getPhoneNumbers(serial_no):
    query = "SELECT phone FROM phone_by_serial_no WHERE serial_no = %s GROUP BY phone"
    args = (serial_no, )

    conn = MySQLConnection(**db_config)

    try:
        cursor = conn.cursor()
        cursor.execute(query, args)

        result = cursor.fetchall()
    except Exception as exception:
        print('Exception: %s' % exception)
    finally:
        cursor.close()
        conn.close()

    return result

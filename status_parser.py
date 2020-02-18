import os
import time
from mysql.connector import MySQLConnection

import functions
import archiving
import notification

import config


db_config = config.db_config
device_dir_path = config.data_path + 'HB_RSHD/'


# Get All Contoller IDs (PX-01)
def getControllers():
    query = "SELECT id, broadcast_serial FROM devices WHERE status_check = 1 AND devices.type = 'PX'"

    conn = MySQLConnection(**db_config)

    try:
        cursor = conn.cursor()
        cursor.execute(query)

        result = cursor.fetchall()
    except Exception as exception:
        print('Exception: %s' % exception)
    finally:
        cursor.close()
        conn.close()

    return result


# Get Palerts from PX-01
def getDeviceIDsFromController(controller_serial_no):
    query = "SELECT ip_address, id, broadcast_serial FROM devices WHERE status_check = 1 AND controller_id IN (SELECT id FROM devices WHERE id = %s) "
    args = (controller_serial_no, )

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


# Parsing Palerts Data from TXT file
# e.g.) IP Address, Status
def readFile(file_path):
    array = []

    try:
        f = open(file_path, 'r')

        if len(f.read()) == 0:
            raise Exception('FILE IS EMPTY')
        else:
            f.seek(0)
            for line in f.readlines():
                if "Node with IP" in line:
                    ip = (line[line.find(":")+1:line.find("is")]).strip()
                    status = (line[line.find("is")+2:line.rfind(".")]).strip()
                    array.append((ip, status))

        return array

    except Exception as e:
        functions.log(e)
    finally:
        f.close()


# Parsing PX-01 Data from TXT file
# e.g.) Disk Space, CPU, Memory
def readFileAll(file_path):
    str = ''

    try:
        f = open(file_path, 'r')

        if len(f.read()) == 0:
            raise Exception('FILE IS EMPTY')
        else:
            f.seek(0)
            for line in f.readlines():
                str += line
                if (line.find("CPU Load") > -1):
                    return str

        return str

    except Exception as e:
        functions.log(e)
    finally:
        f.close()


# Insert to DB
def insertDeviceInfo(device_status, device_id, datetime):
    query = "INSERT INTO devices_status (status, device_id, created_at) VALUES (%s, %s, %s)"
    args = (device_status, device_id, datetime, )

    try:
        db_config = config.db_config
        conn = MySQLConnection(**db_config)

        cursor = conn.cursor()
        cursor.execute(query, args)

        conn.commit()

        return True
    except Exception as exception:
        functions.log('Exception: ' + exception)
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def main():
    # All PX-01 List
    controllers = getControllers()

    # Loop
    for controller in controllers:
        path = device_dir_path + controller[1] + '/'
        f = "filepx01.txt"
        s1 = "status.txt"
        s2 = "sysstatus.txt"

        devices = getDeviceIDsFromController(controller[0])

        # PX-01 Status Check
        if os.path.exists(path + s1) or os.path.exists(path + s2):
            if os.path.exists(path + s1):
                s = s1
            else:
                s = s2

            # Get file created time
            mtime = time.strftime('%Y-%m-%d %H:%M:%S',
                                  time.gmtime(os.path.getmtime(path + s)))

            try:
                statuses = readFileAll(path + s)

                controller_id = path[path.rfind("/")+1:]

                if(insertDeviceInfo(statuses, controller[0], mtime)):
                    archiving.move_archiving(path + "/", s)
                else:
                    archiving.backup_error_file(path + "/", s)
            except Exception as e:
                functions.log(e)
                archiving.backup_error_file(path + "/", s)

        else:
            functions.log('status.txt or sysstatus.txt does not exists')
            insertDeviceInfo('no', controller[0], None)

        if os.path.exists(path + f):
            functions.log('exists ' + f)
            mtime = time.strftime('%Y-%m-%d %H:%M:%S',
                                  time.gmtime(os.path.getmtime(path + f)))

            try:
                statuses = readFile(path + f)
                controller_id = path[path.rfind("/")+1:]
                functions.log("path: " + path)

                if(len(statuses) < 1):
                    raise Exception(
                        'There is no data: {}'.format(path + "/" + f))

                for device_status in statuses:
                    for device in devices:
                        if device[0] == device_status[0]:
                            insertDeviceInfo(
                                device_status[1], device[1], mtime)

                archiving.move_archiving(path + "/", f)
            except Exception as e:
                functions.log(e)
                archiving.backup_error_file(path + "/", f)

        else:
            functions.log('not exists ' + f)
            for device in devices:
                insertDeviceInfo('no', device[1], None)


main()

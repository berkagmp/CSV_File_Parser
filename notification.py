import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests
from mysql.connector import MySQLConnection, Error
from datetime import datetime

import functions
import config


def insertSMS(type, serial_no, date):
    msg = ''

    report_info = functions.getAllInfo(serial_no)
    print(report_info)

    phones = functions.getPhoneNumbers(serial_no)
    print(phones)

    date = datetime.strptime(date, "%Y%m%d").strftime("%d/%m/%Y")

    if(type == 'I'):
        msg = 'Palert('+serial_no+') detected ISO vibration above warning level at ' + \
            report_info[0][1] + ' ' + report_info[0][2] + ' - Date: ' + date

    query = "INSERT INTO sms(message, receiver, updated_at) VALUES(%s, %s, %s)"

    now = datetime.now()
    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')

    for phone in phones:

        args = (
            msg,
            phone[0].replace("-", ""),
            formatted_date,
        )

        try:
            db_config = config.db_config
            conn = MySQLConnection(**db_config)

            cursor = conn.cursor()
            cursor.execute(query, args)

            if cursor.lastrowid:
                print('last insert sms_id', cursor.lastrowid)
            else:
                print('last insert id not found')

            conn.commit()
        except Exception as exception:
            print('Exception: %s' % exception)
        finally:
            cursor.close()
            conn.close()


def sendEmail(message, fullpath):
    to = config.receiver_email
    cc = config.engineer_email
    subject = "System Parsing Error"
    message = "<p>The error has been occurred during parsing</p><p>" + \
        message+"</p><p>"+fullpath+"</p>"

    query = "INSERT INTO email(`to`, `cc`, `subject`, `body`) VALUES(%s, %s, %s, %s)"
    args = (to, cc, subject, message)

    try:
        db_config = config.db_config
        conn = MySQLConnection(**db_config)

        cursor = conn.cursor()
        cursor.execute(query, args)

        conn.commit()
    except Exception as exception:
        print('Exception: %s' % exception)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

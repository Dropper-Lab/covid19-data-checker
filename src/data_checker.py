"""
MIT License

Copyright (c) 2020 Dropper Lab

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import logging
from logging.handlers import RotatingFileHandler

import time
import os

import smtplib
from email.mime.text import MIMEText

import pymysql

import mail_property
import mysql_property
import database_info

logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)
fileHandler = RotatingFileHandler('./log/data_checker.log', maxBytes=1024 * 1024 * 1024 * 9, backupCount=9)
fileHandler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)s] >> %(message)s'))
logger.addHandler(fileHandler)
logger.setLevel(logging.INFO)
logger.info('every package loaded and start logging')


def send_mail(origin=mail_property.mail_address, target=mail_property.mail_address, subject='', message=''):
    smtp = smtplib.SMTP_SSL(mail_property.address, mail_property.port)
    smtp.ehlo()
    smtp.login(mail_property.username, mail_property.password)

    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = origin
    msg['To'] = target
    smtp.sendmail(origin, target, msg.as_string())

    smtp.quit()


def check_timestamp(timestamp_1, timestamp_2, error_range):
    if abs(timestamp_1 - timestamp_2) < error_range:
        return 0
    else:
        return 1


def check_tables(database_name, table_list, current_timestamp):
    logger.info('checkTables: function started')
    connection = pymysql.connect(host=mysql_property.hostname, user=mysql_property.user,
                                 password=mysql_property.password, db=database_name,
                                 charset=mysql_property.charset)
    logger.info('checkTables: database connection opened | database_name=' + database_name)
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger.info('checkTables: database cursor created')

    result_flag = 0
    result_list = []

    for table in table_list:
        cursor.execute(f"select * from  {table} order by timestamp desc limit 0, 1;")
        previous_timestamp = cursor.fetchone()['timestamp']
        if -check_timestamp(current_timestamp, previous_timestamp, 3600):
            result_flag = 1
            result_list.append(table)

    connection.close()
    logger.info('checkTables: database connection closed')

    logger.info('checkTables: function ended | result_flag=' + str(result_flag) + ' | result_list=' + str(result_list))
    return result_flag, result_list


def check_status():
    timestamp = time.time()

    status = [0]

    for database in database_info.database_list:
        flag, list = check_tables(database, database_info.table_list[database], timestamp)

        if flag == 1:
            status[0] = 1

        status.append({'name': database, 'flag': flag, 'list': list})

    return status


def assemble_message(result):
    message = '- Dropper API Data Report -\n\n\n'
    for data in result[1:]:
        message += f"[{data['name']}] {'RED' if data['flag'] else 'GREEN'} - {len(data['list'])}\n"

        if data['flag']:
            message += '---------------------------\n'
            for table in data['list']:
                message += f"{table}\n"
            message += '---------------------------\n'

        message += '\n'

    return message


def autofix():
    os.system('python3.6 status_crawler.py ; python3.6 foreign_crawler.py ; python3.6 patient_crawler.py')


if __name__ == '__main__':
    result = check_status()
    message = assemble_message(result)

    if result[0] == 0:
        send_mail(subject='[Dropper API] Data update has been finished successfully',
                  message=message)
    else:
        send_mail(subject='[Dropper API] Data update has been failed',
                  message=message)

        autofix()

        result = check_status()
        message = assemble_message(result)

        if result[0] == 0:
            send_mail(subject='[Dropper API] Autofix has been finished successfully',
                      message=message)
        else:
            send_mail(subject='[Dropper API] Autofix has been failed',
                      message=message)

import logging
from logging.handlers import RotatingFileHandler

import time

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


if __name__ == '__main__':
    timestamp = time.time()

    result = [0]

    for database in database_info.database_list:
        flag, list = check_tables(database, database_info.table_list[database], timestamp)

        if flag == 1:
            result[0] = 1

        result.append({'name': database, 'flag': flag, 'list': list})

    message = '- Dropper API Data Report -\n\n\n'
    for data in result[1:]:
        message += f"[{data['name']}] {'RED' if data['flag'] else 'GREEN'} - {data['list']}\n"

    if result[0] == 0:
        send_mail(subject='[Dropper API] Data has been updated successfully',
                  message=message)
    else:
        send_mail(subject='[Dropper API] There is a problem with the data update',
                  message=message)

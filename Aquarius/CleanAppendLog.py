# -*- coding: utf-8 -*-

__author__ = 'sdamiano'

""" This program is intended to clean out the Aq_Event_log_ table.  That table gets very big very, very quickly
with records from API appends.  As it gets too big, the whole system slows down dramatically.  This is to nightly
clean out the records from the API so that manual append records can be left in place for much longer"""



#Set up logging to an external file if desired
Log_to_file = True

# Import libraries to talk to SQL and the database parameters
import os
import datetime
import pymssql
import pymysql

from dbinfo import aqdb_host, aqdb_name, aqdb_user, aqdb_password
from dbinfo import aq_username, dbhost, dbname, dbuser, dbpswd, logdirectory


if Log_to_file:
    # Find the date/time the script was started:
    start_datetime = datetime.datetime.now()
    # Get the path and directory of this script:
    filename = os.path.realpath('__file__')
    #Open up a text file to log to
    logfile = logdirectory + "\AppendLog_" + start_datetime.strftime("%Y%m%d") + ".txt"
    text_file = open(logfile, "a+")
    text_file.write("Script: %s \n" % filename)
    text_file.write("Script started at %s \n \n" % start_datetime)

    #Open up a text file backup the table to
    backupfile = logdirectory + "\Backup_aq_event_log_" + start_datetime.strftime("%Y%m%d") + ".txt"
    batext_file = open(backupfile, "a+")
    batext_file.write("Script: %s \n" % filename)
    batext_file.write("Script started at %s \n \n" % start_datetime)


def Get_Event_Log_Length():
    '''
    :return: this returns the number of records in the aq_event_log_ table
    when this table becomes too long, the whole system bogs down.
    '''
    conn = pymssql.connect(server=aqdb_host, user=aqdb_user, password=aqdb_password, database=aqdb_name)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            COUNT(*)
        FROM
            aq_event_log_
    """)

    events = cur.fetchall()[0]

    cur.close()     # close the database cursor
    conn.close()    # close the database connection

    return events

# Check the length of the event log prior to cleaning
pre_cleaning = Get_Event_Log_Length()
print "There were %s records in aq_event_log_ prior to cleaning" % pre_cleaning
if Log_to_file:
    text_file.write("There were %s records in aq_event_log_ prior to cleaning \n" % pre_cleaning)
    batext_file.write("There were %s records in aq_event_log_ prior to cleaning \n" % pre_cleaning)


# Write out the contents of the event log to a text file.
if Log_to_file:
    conn = pymssql.connect(server=aqdb_host, user=aqdb_user, password=aqdb_password, database=aqdb_name, as_dict=True)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            *
        FROM
            aq_event_log_
    """)

    events = cur.fetchall()

    cur.close()     # close the database cursor
    conn.close()    # close the database connection
    for row in events:
        batext_file.write(row)


#Look for the aop id of dataseries that are being appended by the API from dreamhost
conn=pymysql.connect(host=dbhost,db=dbname,user=dbuser,passwd=dbpswd)
cur = conn.cursor()

cur.execute("""
    SELECT DISTINCT
        AQTimeSeriesID
    FROM
        Series_for_midStream
    WHERE
        AQTimeSeriesID != 0
    ;
""")

AqSeries = cur.fetchall()

cur.close()     # close the database cursor
conn.close()    # close the database connection


# Now delete events from the SQL table
conn = pymssql.connect(server=aqdb_host, user=aqdb_user, password=aqdb_password, database=aqdb_name)
cur = conn.cursor()

# Delete all events directly run by the API user
cur.execute("""
    DELETE
    FROM
        aq_event_log_
    WHERE
        userID_ = '%s'
    ;
""" % aq_username)

print "%s rows were deleted from aq_event_log_ that were added by the API user" % cur.rowcount
if Log_to_file:
    text_file.write("%s rows were deleted from aq_event_log_ that were added by the API user \n" % cur.rowcount)

conn.commit()

# Delete event run by the system on the time series designated to receive streaming data.
for AQTimeSeriesID in AqSeries:
    cur.execute("""
        DELETE
        FROM
            aq_event_log_
        WHERE
            eventOrigin_ = '%s' and eventType_ = 'Automated Processing' and userID_ = 'SYSTEM@AQUARIUS'
        ;
    """ % AQTimeSeriesID)

    print "%s row were deleted from aq_event_log_ that were automated processing on AOP %s" % (cur.rowcount, AQTimeSeriesID)
    if Log_to_file and cur.rowcount > 0:
        text_file.write("%s row were deleted from aq_event_log_ that were automated processing on AOP %s \n" % (cur.rowcount, AQTimeSeriesID))

    conn.commit()

cur.close()     # close the database cursor
conn.close()    # close the database connection


#Check the length of the table again after cleaning.
post_cleaning = Get_Event_Log_Length()
print "There are %s records in aq_event_log_ after cleaning" % post_cleaning
if Log_to_file:
    text_file.write("There are %s records in aq_event_log_ after cleaning \n" % post_cleaning)


#Close out the text file
if Log_to_file:
    # Find the date/time the script was started:
    end_datetime = datetime.datetime.now()
    runtime = end_datetime - start_datetime
    text_file.write("\n")
    text_file.write("Script completed at %s \n" % end_datetime)
    text_file.write("Total time for script: %s \n" % runtime)
    text_file.write("========================================================================================= \n")
    text_file.write("\n \n")
    text_file.write("========================================================================================= \n")
    text_file.close()

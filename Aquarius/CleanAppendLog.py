# -*- coding: utf-8 -*-

"""

@author: Sara Geleskie Damiano

This program is intended to clean out the Aq_Event_log_ table.  That table gets very big very, very quickly
with records from API appends.  As it gets too big, the whole system slows down dramatically.  This is to regularly
clean out the records from the API so that manual append records can be left in place for much longer.
"""

# Import libraries to talk to SQL and the database parameters
import os
import datetime
import pytz
import argparse
import sys
import pymssql
import pymysql

from dbinfo import aqdb_host, aqdb_name, aqdb_user, aqdb_password
from dbinfo import aq_username, dbhost, dbname, dbuser, dbpswd

__author__ = 'Sara Geleskie Damiano'
__contact__ = 'sdamiano@stroudcenter.org'

# Find the date/time the script was started:
start_datetime_utc = datetime.datetime.now(pytz.utc)
eastern_local_time = pytz.timezone('US/Eastern')
start_datetime_loc = start_datetime_utc.astimezone(eastern_local_time)

# Get the path and directory of this script:
script_name_with_path = os.path.realpath(__file__)
script_directory = os.path.dirname(os.path.realpath(__file__))

# Set up a parser for command line options
parser = argparse.ArgumentParser(description='This script appends data from Dreamhost to Aquarius.')
parser.add_argument('--debug', action='store_true',
                    help='Turn debugging on')
parser.add_argument('--nolog', action='store_false',
                    help='Turn logging off')
# Read the command line options, if run from the command line
if sys.stdin.isatty():
    debug = parser.parse_args().debug
    Log_to_file = parser.parse_args().nolog
else:
    debug = True
    Log_to_file = True

if debug:
    print "Now running script: %s" % script_name_with_path
    print "Script started at %s" % start_datetime_loc


if Log_to_file:
    # Open up a text file to log to
    logfile = script_directory + "\AppendLogs\CleaningLog_" + start_datetime_loc.strftime("%Y%m%d") + ".txt"
    if debug:
        print "Log being written to: %s" % logfile
    text_file = open(logfile, "a+")
    text_file.write("***********************************************************************************************\n")
    text_file.write("Script: %s \n" % script_name_with_path)
    text_file.write("***********************************************************************************************\n")
    text_file.write("\n")
    text_file.write("Script started at %s \n \n" % start_datetime_loc)
    # Open up a text file backup the table to - Not doing because this slows the script immensely
    # backupfile = script_directory + "\AppendLogs\Backup_aq_event_log_" + \
    #              start_datetime_loc.strftime("%Y%m%d") + ".txt"
    # if debug:
    #     print "Log being written to: %s" % logfile
    # batext_file = open(backupfile, "a+")
    # batext_file.write("*******************************************************************************************\n")
    # batext_file.write("Script: %s \n" % script_name_with_path)
    # batext_file.write("*******************************************************************************************\n")
    # batext_file.write("\n")
    # batext_file.write("Script started at %s \n \n" % start_datetime_loc)
else:
    text_file = ""


def get_event_log_length():
    """
    :return: this returns the number of records in the aq_event_log_ table
    when this table becomes too long, the whole system bogs down.
    """
    conn_f = pymssql.connect(server=aqdb_host, user=aqdb_user, password=aqdb_password, database=aqdb_name)
    cur_f = conn.cursor()

    cur.execute("""
        SELECT
            COUNT(*)
        FROM
            aq_event_log_
    """)

    events = cur_f.fetchall()[0]

    cur_f.close()     # close the database cursor
    conn_f.close()    # close the database connection

    return events

# Check the length of the event log prior to cleaning
pre_cleaning = get_event_log_length()
if debug:
    print "There were %s records in aq_event_log_ prior to cleaning" % pre_cleaning
if Log_to_file:
    text_file.write("There were %s records in aq_event_log_ prior to cleaning \n" % pre_cleaning)
    # batext_file.write("There were %s records in aq_event_log_ prior to cleaning \n" % pre_cleaning)


# Write out the contents of the event log to a text file.
# SRGD - Not doing this at this time, it slows down the script immensely.
# if Log_to_file:
#     conn = pymssql.connect(server=aqdb_host, user=aqdb_user, password=aqdb_password, database=aqdb_name, as_dict=True)
#     cur = conn.cursor()
#
#     cur.execute("""
#         SELECT
#             *
#         FROM
#             aq_event_log_
#     """)
#
#     events = cur.fetchall()
#
#     cur.close()     # close the database cursor
#     conn.close()    # close the database connection
#     for row in events:
#         batext_file.write(row)


# Look for the aop id of dataseries that are being appended by the API from dreamhost
conn = pymysql.connect(host=dbhost, db=dbname, user=dbuser, passwd=dbpswd)
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

if debug:
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

    if debug:
        print "%s row were deleted from aq_event_log_ that were automated processing on AOP %s" %\
              (cur.rowcount, AQTimeSeriesID)
    if Log_to_file and cur.rowcount > 0:
        text_file.write("%s row were deleted from aq_event_log_ that were automated processing on AOP %s \n" %
                        (cur.rowcount, AQTimeSeriesID))

    conn.commit()

cur.close()     # close the database cursor
conn.close()    # close the database connection


# Check the length of the table again after cleaning.
post_cleaning = get_event_log_length()
if debug:
    print "There are %s records in aq_event_log_ after cleaning" % post_cleaning
if Log_to_file:
    text_file.write("There are %s records in aq_event_log_ after cleaning \n" % post_cleaning)

# Find the date/time the script finished:
end_datetime_utc = datetime.datetime.now(pytz.utc)
end_datetime_loc = end_datetime_utc.astimezone(eastern_local_time)
runtime = end_datetime_utc - start_datetime_utc

# Close out the text file
if debug:
    print "Script completed at %s" % end_datetime_loc
    print "Total time for script: %s" % runtime
if Log_to_file:
    text_file.write("\n")
    text_file.write("Script completed at %s \n" % end_datetime_loc)
    text_file.write("Total time for script: %s \n" % runtime)
    text_file.write("***********************************************************************************************\n")
    text_file.write("\n \n")
    text_file.close()

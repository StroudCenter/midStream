# -*- coding: utf-8 -*-

"""
Created on Wed Nov 05 13:58:15 2014

@author: Sara Geleskie Damiano

This script moves all data from series tagged with an Aquarius dataset primary key
from a DreamHost database to Stroud's Aquarius server.
This version includes only data from the last hour.
"""

import suds
import pymysql
import datetime
import pytz
import base64
import os
import sys

# Bring in all of the database connection inforamation.
from dbinfo import aq_url, aq_username, aq_password, dbhost, dbname, dbuser, dbpswd, logdirectory

__author__ = 'Sara Geleskie Damiano'

# Set up logging and debugging.
Log_to_file = True  # This will save a log in an external text file.
debug = True    # This will add lots of print statements.  Turn it off in production.

# Call up the Aquarius Aquisition SOAP API
client = suds.client.Client(aq_url)

# Find the date/time the script was started:
start_datetime_utc = datetime.datetime.now(pytz.utc)

# Deal with timezones...
eastern_standard_time = pytz.timezone('Etc/GMT+5')
eastern_local_time = pytz.timezone('US/Eastern')
costa_rica_time = pytz.timezone('Etc/GMT+6')
start_datetime_est = start_datetime_utc.astimezone(eastern_standard_time)
start_datetime_loc = start_datetime_utc.astimezone(eastern_local_time)

# Set the datetime for the query to go from
query_datetime = start_datetime_loc - datetime.timedelta(hours=1)

if debug:
    print "Script started at %s" % start_datetime_loc
if Log_to_file:
    # Get the path and directory of this script:
    filename = os.path.realpath(__file__)
    #Open up a text file to log to
    logfile = logdirectory + "\AppendLog_" + start_datetime_loc.strftime("%Y%m%d") + ".txt"
    text_file = open(logfile, "a+")
    text_file.write("Script: %s \n" % filename)
    text_file.write("Script started at %s \n \n" % start_datetime_loc)


# Get an authentication token to open the path into the API
AuthToken = client.service.GetAuthToken(aq_username, aq_password)
if debug:
    print "Authentication Token: %s" % AuthToken

# Set up connection to the DreamHost MySQL database
conn = pymysql.connect(host=dbhost, db=dbname, user=dbuser, passwd=dbpswd)
cur = conn.cursor()

# Look for Dataseries that have an associated Aquarius Time Series ID
cur.execute("""
    SELECT DISTINCT
        AQTimeSeriesID,
        TableName,
        TableColumnName,
        DateTimeSeriesStart,
        DateTimeSeriesEnd
    FROM
        Series_for_midStream
    WHERE
        AQTimeSeriesID != 0
        AND (DateTimeSeriesEnd is NULL
             OR DateTimeSeriesEnd > '%s')
    ORDER BY
        TableName,
        AQTimeSeriesID
    ;"""
    % str(query_datetime.strftime("%Y-%m-%d %H:%M:%S"))
)

AqSeries = cur.fetchall()

if debug:
    print "%s series found with corresponding time series in Aquarius" % (len(AqSeries))
if Log_to_file:
    text_file.write("%s series found with corresponding time series in Aquarius \n \n" % (len(AqSeries)))
    text_file.write("Series, Table, Column, NumericIdentifier, TextIdentifier, NumPointsAppended, AppendToken  \n")


def get_data_from_table(table, column, series_start, series_end):
    """
    Returns a base64 data object with the data from a given table and column
    """
    # Set up an min and max time for when those values are NULL in dreamhost
    if series_end is None:
        series_end = start_datetime_utc + datetime.timedelta(days=1)  # Future times clearly not valid
    if series_start is None:
        series_start = datetime.datetime(2000, 1, 1, 0, 0, 0)
    # Get what time was an hour ago in the correct time zone.
    query_start_utc = start_datetime_utc - datetime.timedelta(hours=1)
    query_start_est = query_start_utc.astimezone(eastern_standard_time)
    query_start_cr = query_start_utc.astimezone(costa_rica_time)
    # Creating the query text here because the character masking works oddly
    # in the cur.execute function.
    if table == "CRDavis":
        query_text = "SELECT Date, " + column + " FROM " + table + " WHERE " \
                     + "Date < " + str(series_end.strftime("'%Y-%m-%d %H:%M:%S'")) \
                     + " AND " \
                     + "Date > " + str(series_start.strftime("'%Y-%m-%d %H:%M:%S'")) \
                     + " AND " \
                     + "Date > " + query_start_cr.strftime("'%Y-%m-%d %H:%M:%S'") \
                     + " AND " \
                     + column + " IS NOT NULL " \
                     + ";"
    elif table == "davis":
        query_text = "SELECT Date, " + column + " FROM " + table + " WHERE " \
                     + "Date < " + str(series_end.strftime("'%Y-%m-%d %H:%M:%S'")) \
                     + " AND " \
                     + "Date > " + str(series_start.strftime("'%Y-%m-%d %H:%M:%S'")) \
                     + " AND " \
                     + "Date > " + query_start_est.strftime("'%Y-%m-%d %H:%M:%S'") \
                     + " AND " \
                     + column + " IS NOT NULL " \
                     + ";"
    else:
        query_text = "SELECT Loggertime, " + column + " FROM " + table + " WHERE " \
                     + "Date < " + str(series_end.strftime("'%Y-%m-%d %H:%M:%S'")) \
                     + " AND " \
                     + "Date > " + str(series_start.strftime("'%Y-%m-%d %H:%M:%S'")) \
                     + " AND " \
                     + " AND " \
                     + "Date > " + query_start_est.strftime("'%Y-%m-%d %H:%M:%S'") \
                     + column + " IS NOT NULL " \
                     + ";"

    if debug:
        print "   Data selected using the query:"
        print "   " + query_text
        t1 = datetime.datetime.now()

    cur.execute(query_text)

    values_table = cur.fetchall()

    if debug:
        print "   which returns %s values" % (len(values_table))
        t2 = datetime.datetime.now()
        print "   SQL execution took %s" % (t2 - t1)

    # Create a comma separated string of the data in the database
    csvdata = ''
    csvdata2 = ''

    fff = ""  # fff represents a numeric flag value (optional).
    ggg = ""  # ggg represents a numeric grade value (optional).
    iii = ""  # iii represents a numeric interpolation code (optional).
    aaa = ""  # aaa represents a numeric approval code (optional).
    note = ""  # “note” represents a text note which can be attached to the point (optional).

    for timestamp, value in values_table:
        if table in ("davis" "CRDavis"):
            csvdata2 += csvdata.join("\n".join(["%s"",""%s"",""%s"",""%s"",""%s"",""%s"",""%s" %
                                                (timestamp.isoformat(' '), value, fff, ggg, iii, aaa, note)]) +
                                     "\n")
        else:
            # Need to convert arduino logger time into unix time (add 946684800)
            # and then to UTC-5 (add 18000)
            timestamp_dt = datetime.datetime.fromtimestamp(timestamp+946684800+18000, tz=pytz.timezone('EST'))
            timestamp_dt2 = timestamp_dt.replace(tzinfo=None)
            csvdata2 += csvdata.join("\n".join(["%s"",""%s"",""%s"",""%s"",""%s"",""%s"",""%s" %
                                                (timestamp_dt2.isoformat(' '), value, fff, ggg, iii, aaa, note)]) +
                                     "\n")

    # Convert the datastring into a base64 object
    csvbytes = base64.b64encode(csvdata2)

    return csvbytes


# Get data for all series that are available
loopnum = 1
for AQTimeSeriesID, table_name, table_column_name, series_start, series_end in AqSeries:
    if debug:
        print "Attempting to append series %s of %s" % (loopnum, len(AqSeries))
        print "Data being appended to Time Series # %s" % AQTimeSeriesID
    appendbytes = get_data_from_table(table_name, table_column_name, series_start, series_end)
    # Actually append to the Aquarius dataset
    if len(appendbytes) > 0:
        try:
            if debug:
                t3 = datetime.datetime.now()
            AppendResult = client.service.AppendTimeSeriesFromBytes2(
                long(AQTimeSeriesID), appendbytes, aq_username)
            if debug:
                t4 = datetime.datetime.now()
        except:
            error_in_append = sys.exc_info()[0]
            if debug:
                print "      Error: %s" % error_in_append
                print "      API execution took %s" % (t4 - t3)
            if Log_to_file:
                text_file.write("%s, %s, %s, ERROR!, 0, %s  \n"
                                % (loopnum, table_name, table_column_name, error_in_append))
        else:
            if debug:
                print AppendResult
                print "      API execution took %s" % (t4 - t3)
            if Log_to_file:
                text_file.write("%s, %s, %s, %s, %s, %s, %s \n"
                                % (loopnum, table_name, table_column_name,
                                   AQTimeSeriesID, AppendResult.TsIdentifier,
                                   AppendResult.NumPointsAppended, AppendResult.AppendToken))
    else:
        if debug:
            print "      No data appended from this query."
        if Log_to_file:
            # text_file.write("No data appended from this query. \n")
            text_file.write("%s, %s, %s, NoAppend, 0, NoAppend  \n" % (loopnum, table_name, table_column_name))
    loopnum += 1

# Close out the database connections
cur.close()  # close the database cursor
conn.close()  # close the database connection


# Find the date/time the script finished:
end_datetime_utc = datetime.datetime.now(pytz.utc)
end_datetime_loc = end_datetime_utc.astimezone(eastern_local_time)
runtime = end_datetime_utc - start_datetime_utc

# Close out the text file
if debug:
    print "Script completed at %s \n" % end_datetime_loc
    print "Total time for script: %s \n" % runtime
if Log_to_file:
    text_file.write("\n")
    text_file.write("Script completed at %s \n" % end_datetime_loc)
    text_file.write("Total time for script: %s \n" % runtime)
    text_file.write("========================================================================================= \n")
    text_file.write("\n \n")
    text_file.write("========================================================================================= \n")
    text_file.close()

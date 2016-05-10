# -*- coding: utf-8 -*-

"""
Created on Wed Nov 05 13:58:15 2014

@author: Sara Geleskie Damiano

This script moves all data from series tagged with an Aquarius dataset primary key
from a DreamHost database to Stroud's Aquarius server.
This version only includes a single manually specified series.
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

# Set up logging to an external file if desired
Log_to_file = True

# Select the time series.
# AqSeries = ((AQTimeSeriesID, TableName, TableColumnName, SeriesEnd=None),)
AqSeries = ((17691364, 'SL043', 'CTDdepth', None),)

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

if Log_to_file:
    # Get the path and directory of this script:
    filename = os.path.realpath(__file__)
    #Open up a text file to log to
    logfile = logdirectory + "\AppendLog_" + start_datetime_loc.strftime("%Y%m%d") + ".txt"
    text_file = open(logfile, "a+")
    text_file.write("Script: %s \n" % filename)
    text_file.write("Script started at %s \n \n" % start_datetime_loc)
    text_file.write("Manually Attempting Appends from table %s column %s to Aquarius series %s  \n" %
                    (AqSeries[0][1], AqSeries[0][2], AqSeries[0][0]))
    text_file.write("Series, Table, Column, TimeSeriesIdentifier, NumPointsAppended, AppendToken  \n")


# Get an authentication token to open the path into the API
AuthToken = client.service.GetAuthToken(aq_username, aq_password)
print "Authentication Token: %s" % AuthToken

# Set up connection to the DreamHost MySQL database
conn = pymysql.connect(host=dbhost, db=dbname, user=dbuser, passwd=dbpswd)
cur = conn.cursor()


def get_data_from_table(table, column, series_start, series_end):
    """
    Returns a base64 data object with the data from a given table and column
    """
    # Creating the query text here because the character masking works oddly
    # in the cur.execute function.
    if series_end is None:
        series_end = datetime.datetime.max
    if series_start is None:
        series_start = datetime.datetime(1900, 1, 1, 0, 0, 0)
    if table == "CRDavis":
        query_text = "SELECT Date, " + column + " FROM " + table + " WHERE " \
                     + "Date < " + str(series_end.strftime("'%Y-%m-%d %H:%M:%S'")) \
                     + " AND " \
                     + "Date > " + str(series_start.strftime("'%Y-%m-%d %H:%M:%S'")) \
                     + " AND " \
                     + column + " IS NOT NULL " \
                     + ";"
    elif table == "davis":
        query_text = "SELECT Date, " + column + " FROM " + table + " WHERE " \
                     + "Date < " + str(series_end.strftime("'%Y-%m-%d %H:%M:%S'")) \
                     + " AND " \
                     + "Date > " + str(series_start.strftime("'%Y-%m-%d %H:%M:%S'")) \
                     + " AND " \
                     + column + " IS NOT NULL " \
                     + ";"
    else:
        query_text = "SELECT Loggertime, " + column + " FROM " + table + " WHERE " \
                     + "Date < " + str(series_end.strftime("'%Y-%m-%d %H:%M:%S'")) \
                     + " AND " \
                     + "Date > " + str(series_start.strftime("'%Y-%m-%d %H:%M:%S'")) \
                     + " AND " \
                     + column + " IS NOT NULL " \
                     + ";"

    print "Data selected using the query:"
    print query_text

    cur.execute(query_text)

    values_table = cur.fetchall()

    cur.close()     # close the database cursor
    conn.close()    # close the database connection

    print "which returns %s values" % (len(values_table))

    # if Log_to_file:
    #     text_file.write("Data selected using the query: \n")
    #     text_file.write("%s \n" % (query_text))
    #     text_file.write("which returns %s values \n" % (len(values_table)))

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
    print "Attempting to append series %s of %s" % (loopnum, len(AqSeries))
    appendbytes = get_data_from_table(table_name, table_column_name, series_start, series_end)
    # Actually append to the Aquarius dataset
    if len(appendbytes) > 0:
        try:
            AppendResult = client.service.AppendTimeSeriesFromBytes2(
                long(AQTimeSeriesID), appendbytes, aq_username)
        except:
            error_in_append = sys.exc_info()[0]
            print "Error: %s" % error_in_append
            if Log_to_file:
                text_file.write("%s, %s, %s, ERROR!, 0, %s  \n"
                                % (loopnum, table_name, table_column_name, error_in_append))
        else:
            print AppendResult
            if Log_to_file:
                # text_file.write("%s \n" % AppendResult)
                text_file.write("%s, %s, %s, %s, %s, %s \n"
                                % (loopnum, table_name, table_column_name, AppendResult.TsIdentifier,
                                   AppendResult.NumPointsAppended, AppendResult.AppendToken))
    else:
        print "No data appended from this query."
        if Log_to_file:
            # text_file.write("No data appended from this query. \n")
            text_file.write("%s, %s, %s, NoAppend, 0, NoAppend  \n" % (loopnum, table_name, table_column_name))
    loopnum += 1
        
# Close out the text file
if Log_to_file:
    # Find the date/time the script was started:
    end_datetime_utc = datetime.datetime.now(pytz.utc)
    end_datetime_loc = end_datetime_utc.astimezone(eastern_local_time)
    runtime = end_datetime_utc - start_datetime_utc
    text_file.write("\n")
    text_file.write("Script completed at %s \n" % end_datetime_loc)
    text_file.write("Total time for script: %s \n" % runtime)
    text_file.write("========================================================================================= \n")
    text_file.write("\n \n")
    text_file.write("========================================================================================= \n")
    text_file.close()
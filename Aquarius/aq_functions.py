# -*- coding: utf-8 -*-


"""
Created by Sara Geleskie Damiano on 5/16/2016 at 6:14 PM


"""

import suds
import pymysql
import pandas as pd
import time
import datetime
import pytz
import base64
import sys
import socket

# Bring in all of the database connection information.
from dbinfo import aq_acquisition_url, aq_username, aq_password, dbhost, dbname, dbuser, dbpswd

__author__ = 'Sara Geleskie Damiano'
__contact__ = 'sdamiano@stroudcenter.org'


# Get an authentication token to open the path into the API
def get_aq_auth_token(debug=False):
    """
    Sets up an authentication token for the soap session
    """
    # Call up the Aquarius Aquisition SOAP API

    client = suds.client.Client(aq_acquisition_url)
    try:
        auth_token = client.service.GetAuthToken(aq_username, aq_password)
        cookie = client.options.transport.cookiejar
    except suds.WebFault, e:
        if debug:
            print "Error Getting Token: %s" % sys.exc_info()[0]
            print '%s' % e
            print "Stopping all program execution"
        sys.exit("No Authentication Token")
    else:
        if debug:
            print "Authentication Token: %s" % auth_token
            print "Session Cookie %s" % cookie
        return auth_token, cookie


def get_dreamhost_series(cutoff_for_recent=None, table=None, column=None, debug=False):
    """
    Gets a list of all the series to append data to
    :arguments:
    cutoff_for_recent = A datetime string to use to exclude inactive series.
        All series with and end before this time will be excluded.
        Defaults to none.
    table = A table name, if data from only one is desired.
    column = A column name, if data from only one is desired
    :return:
    Returns a list of series.
    """

    str1 = ""
    str2 = ""
    str3 = ""
    if cutoff_for_recent is not None:
        str1 = " AND (DateTimeSeriesEnd is NULL OR DateTimeSeriesEnd > '" \
            + str(cutoff_for_recent.strftime("%Y-%m-%d %H:%M:%S")) \
            + "')"
    if table is not None:
        str2 = " AND TableName = '" + table + "' "
    if column is not None:
        str3 = " AND TableColumnName = '" + column + "' "

    # Look for Dataseries that have an associated Aquarius Time Series ID
    query_text = \
        "SELECT DISTINCT AQTimeSeriesID, TableName, TableColumnName, DateTimeSeriesStart, DateTimeSeriesEnd " \
        "FROM Series_for_midStream " \
        "WHERE AQTimeSeriesID != 0" + str1 + str2 + str3 + \
        "ORDER BY TableName, AQTimeSeriesID ;"

    if debug:
        print "Timeseries selected using the query:"
        print query_text

    # Set up connection to the DreamHost MySQL database
    conn = pymysql.connect(host=dbhost, db=dbname, user=dbuser, passwd=dbpswd)
    cur = conn.cursor()

    cur.execute(query_text)

    aq_series = cur.fetchall()
    if debug:
        print "which returns %s series" % len(aq_series)

    # Close out the database connections
    cur.close()  # close the database cursor
    conn.close()  # close the database connection

    return aq_series


def get_data_from_dreamhost_table(table, column, series_start, series_end,
                                  query_start=None, query_end=None, debug=False):
    """
    Returns a pandas data frame with the timestamp and data value from a given table and column.
    :param table: A string which is the same of the SQL table of interest
    :param column: A string which is the name of the column of interest
    :param series_start: The date/time when the series begins
    :param series_end: The date/time when the series end
    :param query_start: The beginning date/time of interest
    :param query_end: The ending date/time of interest
    :param debug: A boolean for whether extra print commands apply
    :return: A pandas data frame with the timestamp and data value from a given table and column.
    """

    # Set up an min and max time for when those values are NULL in dreamhost
    if series_end is None:
        series_end = datetime.datetime.now() + datetime.timedelta(days=1)  # Future times clearly not valid
    if series_start is None:
        series_start = datetime.datetime(2000, 1, 1, 0, 0, 0)
    # Set up an min and max time for when those values are NULL in dreamhost
    if query_end is None:
        query_end = datetime.datetime.now() + datetime.timedelta(days=1)  # Future times clearly not valid
    if query_start is None:
        query_start = datetime.datetime(2000, 1, 1, 0, 0, 0)

    # Creating the query text here because the character masking works oddly
    # in the cur.execute function.
    if table in ["davis", "CRDavis"]:
        dt_col = "Date"
    else:
        dt_col = "Loggertime"

    query_text = "SELECT " + dt_col + ", " + column + " as data_value " \
                 + "FROM " + table + " WHERE " \
                 + "Date < " + str(series_end.strftime("'%Y-%m-%d %H:%M:%S'")) \
                 + " AND " \
                 + "Date > " + str(series_start.strftime("'%Y-%m-%d %H:%M:%S'")) \
                 + " AND " \
                 + "Date < " + str(query_end.strftime("'%Y-%m-%d %H:%M:%S'")) \
                 + " AND " \
                 + "Date > " + str(query_start.strftime("'%Y-%m-%d %H:%M:%S'")) \
                 + " AND " \
                 + column + " IS NOT NULL " \
                 + "ORDER BY " + dt_col \
                 + ";"

    if debug:
        print "   Data selected using the query:"
        print "   " + query_text
    t1 = datetime.datetime.now()

    # Set up connection to the DreamHost MySQL database
    conn = pymysql.connect(host=dbhost, db=dbname, user=dbuser, passwd=dbpswd)

    values_table = pd.read_sql(query_text,conn)

    # Close out the database connections
    conn.close()  # close the database connection

    if debug:
        print "   which returns %s values" % (len(values_table))
        t2 = datetime.datetime.now()
        print "   SQL execution took %s" % (t2 - t1)

    if table in ["davis", "CRDavis"]:
        values_table['timestamp'] = values_table["Date"]
        values_table.set_index(['timestamp'], inplace=True)
        values_table.drop('Date', axis=1, inplace=True)
    else:
        # Need to convert arduino logger time into unix time (add 946684800)
        values_table['unix_time'] = values_table["Loggertime"] + 946684800
        values_table['timestamp'] = pd.to_datetime(values_table['unix_time'], unit='s')
        values_table.set_index(['timestamp'], inplace=True)
        values_table.drop('Loggertime', axis=1, inplace=True)
        values_table.drop('unix_time', axis=1, inplace=True)
    # print values_table.head(5)
    # print values_table.dtypes
    # print values_table.index

    return values_table


def create_appendable_csv(data_table):
    """
    This takes a pandas data frame and converts it to a base64 string ready to read into the
    Aquarius API.
    :param data_table: A python data frame with a date-time index and a "value" column.
        It also, optionally, can have the fields "flag", "grade", "interpolation",
        "approval", and "note".
    :return: A base64 text string.
    """

    if 'flag' not in data_table:
        data_table['flag'] = ""
    if 'grade' not in data_table:
        data_table['grade'] = ""
    if 'interpolation' not in data_table:
        data_table['interpolation'] = ""
    if 'approval' not in data_table:
        data_table['approval'] = ""
    if 'note' not in data_table:
        data_table['note'] = ""

    # Output a CSV
    csvdata = data_table.to_csv(header=False, date_format='%Y-%m-%dT%H:%M:%S')

    # Convert the data string into a base64 object
    csvbytes = base64.b64encode(csvdata)

    return csvbytes


def aq_timeseries_append(ts_numeric_id, appendbytes, cookie, debug=False):
    """
    Appends data to an aquarius time series given a base64 encoded csv string with the following values:
        datetime(isoformat), value, flag, grade, interpolation, approval, note
    :param ts_numeric_id: The integer primary key of an aquarius time series
    :param appendbytes: Base64 csv string with ISO-datetime, value, flag, grade, interpolation, approval, note
    :param cookie: The cookie wiith the session ID.  Get via the get_aq_auth_token function.
    :param debug: Says whether or not to issue print statements.
    :return: The append result from the SOAP client
    """

    # Call up the Aquarius Acquisition SOAP API
    client = suds.client.Client(aq_acquisition_url, timeout=325)
    client.options.transport.cookiejar = cookie
    empty_result = client.factory.create('ns0:AppendResult')

    # Actually append to the Aquarius dataset
    t3 = datetime.datetime.now()
    if len(appendbytes) > 0:
        for attempt in range(10):
            try:
                append_result = client.service.AppendTimeSeriesFromBytes2(long(ts_numeric_id),
                                                                          appendbytes,
                                                                          aq_username)
            except suds.WebFault, e:
                if debug:
                    print "      Error: %s" % sys.exc_info()[0]
                    print '      %s' % e
                    t4 = datetime.datetime.now()
                    print "      API execution took %s" % (t4 - t3)
                empty_result.NumPointsAppended = 0
                empty_result.AppendToken = 0
                empty_result.TsIdentifier = '"Error: "' + str(e) + '"'
                append_result = empty_result
                break
            except socket.timeout, e:
                if debug:
                    print "      Error: %s" % sys.exc_info()[0]
                    print '      %s' % e
                    print '      Retrying in 30 seconds'
                time.sleep(30)
            else:
                if debug:
                    print append_result
                    t4 = datetime.datetime.now()
                    print "      API execution took %s" % (t4 - t3)
                break
        else:
            if debug:
                print "      Error: %s" % sys.exc_info()[0]
                print '      10 retries attempted'
                t4 = datetime.datetime.now()
                print "      API execution took %s" % (t4 - t3)
            empty_result.NumPointsAppended = 0
            empty_result.AppendToken = 0
            empty_result.TsIdentifier = '"Error: "' + sys.exc_info()[0] + '"'
            append_result = empty_result
    else:
        if debug:
            print "      No data appended from this query."
        empty_result.NumPointsAppended = 0
        empty_result.AppendToken = 0
        empty_result.TsIdentifier = ""
        append_result = empty_result

    return append_result

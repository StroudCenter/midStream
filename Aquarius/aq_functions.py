# -*- coding: utf-8 -*-


"""
Created by Sara Geleskie Damiano on 5/16/2016 at 6:14 PM


"""

import suds
import pymysql
import datetime
import pytz
import base64
import sys

# Bring in all of the database connection inforamation.
from dbinfo import aq_aquisition_url, aq_username, aq_password, dbhost, dbname, dbuser, dbpswd

__author__ = 'Sara Geleskie Damiano'
__contact__ = 'sdamiano@stroudcenter.org'


# Get an authentication token to open the path into the API
def get_aq_auth_token():
    '''
    Sets up an authentication token for the soap session
    '''
    # Call up the Aquarius Aquisition SOAP API

    client = suds.client.Client(aq_aquisition_url)
    AuthToken = client.service.GetAuthToken(aq_username, aq_password)

    if __debug__:
        print "Authentication Token: %s" % AuthToken

    return AuthToken


def get_dreamhost_series(cutoff_for_recent=None, logger=None, column=None):
    '''
    Gets a list of all the series to append data to
    :arguments:
    cutoff_for_recent = A datetime string to use to exclude inactive series.
        All series with and end before this time will be excluded.
        Defaults to none.
    logger = A logger name, if data from only one is desired.
    column = A column name, if data from only one is desired
    :return:
    Returns a list of series.
    '''

    str1 = ""
    str2 = ""
    str3 = ""
    if cutoff_for_recent != None:
        str1 = " AND (DateTimeSeriesEnd is NULL OR DateTimeSeriesEnd > " \
            + str(cutoff_for_recent.strftime("%Y-%m-%d %H:%M:%S")) \
            + ")"
    if logger != None:
        str2 = " AND TableName = " + logger
    if column != None:
        str3 = " AND TableColumnName = " + column

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
            '%s'
            '%s'
            '%s'
        ORDER BY
            TableName,
            AQTimeSeriesID
        ;"""
        % (str1, str2, str3)
    )

    AqSeries = cur.fetchall()
    if __debug__:
        print "%s series found with corresponding time series in Aquarius" % (len(AqSeries))

    # Close out the database connections
    cur.close()  # close the database cursor
    conn.close()  # close the database connection

    return AqSeries


def get_data_from_dreamhost_table(table, column, series_start, series_end, query_start=None, query_end=None):
    """
    Returns a base64 data object with the data from a given table and column
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
    if table in ("davis" "CRDavis"):
        dt_col = "Date"
    else:
        dt_col = "Loggertime"

    query_text = "SELECT " + dt_col + ", " + column + " FROM " + table + " WHERE " \
                 + "Date < " + str(series_end.strftime("'%Y-%m-%d %H:%M:%S'")) \
                 + " AND " \
                 + "Date > " + str(series_start.strftime("'%Y-%m-%d %H:%M:%S'")) \
                 + " AND " \
                 + "Date < " + str(query_end.strftime("'%Y-%m-%d %H:%M:%S'")) \
                 + " AND " \
                 + "Date > " + str(query_start.strftime("'%Y-%m-%d %H:%M:%S'")) \
                 + " AND " \
                 + column + " IS NOT NULL " \
                     + ";"

    if __debug__:
        print "   Data selected using the query:"
        print "   " + query_text
        t1 = datetime.datetime.now()

    # Set up connection to the DreamHost MySQL database
    conn = pymysql.connect(host=dbhost, db=dbname, user=dbuser, passwd=dbpswd)
    cur = conn.cursor()

    cur.execute(query_text)

    values_table = cur.fetchall()

    # Close out the database connections
    cur.close()  # close the database cursor
    conn.close()  # close the database connection

    if __debug__:
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


def aq_timeseries_append(ts_numeric_id, appendbytes):
    """
    Appends data to an aquarius time series given a base64 encoded csv string with the following values:
        datetime(isoformat), value, flag, grade, interpolation, approval, note
    :param ts_numeric_id: The integer primary key of an aquarius time series
    :param appendbytes: Base64 csv string as above
    :return: The append result from the SOAP client
    """

    # Call up the Aquarius Aquisition SOAP API
    client = suds.client.Client(aq_aquisition_url)
    empty_result = client.factory.create('ns0:AppendResult')

    # Actually append to the Aquarius dataset
    if len(appendbytes) > 0:
        try:
            if __debug__:
                t3 = datetime.datetime.now()
            AppendResult = client.service.AppendTimeSeriesFromBytes2(
                long(ts_numeric_id), appendbytes, aq_username)
            if __debug__:
                t4 = datetime.datetime.now()

        except:
            error_in_append = sys.exc_info()[0]
            if __debug__:
                print "      Error: %s" % error_in_append
                print "      API execution took %s" % (t4 - t3)
            empty_result.NumPointsAppended = 0
            empty_result.AppendToken = 0
            empty_result.TsIdentifier = error_in_append
            return empty_result

        else:
            if __debug__:
                print AppendResult
                print "      API execution took %s" % (t4 - t3)
            return AppendResult
    else:
        if __debug__:
            print "      No data appended from this query."
        empty_result.NumPointsAppended = 0
        empty_result.AppendToken = 0
        empty_result.TsIdentifier = ""
        return empty_result
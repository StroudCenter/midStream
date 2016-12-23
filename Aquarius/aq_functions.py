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
import numpy as np

# Bring in all of the database connection information.
from dbinfo import aq_acquisition_url, aq_username, aq_password, dbhost, dbname, dbuser, dbpswd

__author__ = 'Sara Geleskie Damiano'
__contact__ = 'sdamiano@stroudcenter.org'

# Turn off chained assignment warning.
pd.options.mode.chained_assignment = None  # default='warn'


# Get an authentication token to open the path into the API
def get_aq_auth_token(username, password, debug=False):
    """
    Sets up an authentication token for the soap session
    """
    # Call up the Aquarius Aquisition SOAP API

    try:
        client = suds.client.Client(aq_acquisition_url, timeout=15)
    except Exception, e:
        if debug:
            print "Error Getting Token: %s" % sys.exc_info()[0]
            print '%s' % e
            print "Stopping all program execution"
        sys.exit("Unable to connect to server")
    else:
        try:
            auth_token = client.service.GetAuthToken(username, password)
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
load_auth_token, load_cookie = get_aq_auth_token(aq_username, aq_password)


def check_aq_connection(cookie=load_cookie):
    # Call up the Aquarius Acquisition SOAP API
    try:
        client = suds.client.Client(aq_acquisition_url, timeout=15)
        client.options.transport.cookiejar = cookie
    except Exception, e:
        is_valid = False
    else:
        try:
            is_valid = client.service.IsConnectionValid()
        except Exception, e:
            is_valid = False
        else:
            e = ""
    return is_valid, e


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
        "SELECT DISTINCT AQTimeSeriesID, TableName, TableColumnName, SeriesTimeZone," \
        " DateTimeSeriesStart, DateTimeSeriesEnd " \
        " FROM Series_for_midStream " \
        " WHERE AQTimeSeriesID != 0 " + str1 + str2 + str3 + \
        " ORDER BY TableName, AQTimeSeriesID ;"

    if debug:
        print "Timeseries selected using the query:"
        print query_text

    # Set up connection to the DreamHost MySQL database
    conn = pymysql.connect(host=dbhost, db=dbname, user=dbuser, passwd=dbpswd)
    cur = conn.cursor()

    cur.execute(query_text)

    aq_series = list(cur.fetchall())
    aq_series_list = [list(series) for series in aq_series]

    for series in aq_series_list:
        # Turn the time zone for the series into a pytz timezone
        if series[3] is not None:
            utc_offset_string = '{:+3.0f}'.format(series[3]*-1).strip()
            timezone = pytz.timezone('Etc/GMT'+utc_offset_string)
            series[3] = timezone
        else:
            series[3] = pytz.timezone('Etc/GMT+5')
        # Make the series start and end times "timezone-aware"
        # Per the database instructions, these times should always be in EST, regardless of the timezone of the logger.
        if series[4] is not None:
            series[4] = pytz.timezone('Etc/GMT+5').localize(series[4])
        if series[5] is not None:
            series[5] = pytz.timezone('Etc/GMT+5').localize(series[5])
    if debug:
        # print aq_series_list
        # print type(aq_series)
        print "which returns %s series" % len(aq_series)

    # Close out the database connections
    cur.close()  # close the database cursor
    conn.close()  # close the database connection

    return aq_series_list


def convert_rtc_time_to_python(logger_time, timezone):
    """
    This function converts an arduino logger time into a time-zone aware python date-time object.
    Arduino's internal clocks (Real Time Clock (RTC) modules like the DS3231 chip)
    are converted to unix time by adding 946684800 - This moves the epoch time from January 1, 2000 as used by
    the RTC module to January 1, 1960 as used in Unix and other systems.
    :param logger_time: An timestamp in seconds since January 1, 2000
    :param timezone: a pytz timezone object
    :return: returns a time-zone aware python date time object
    """
    unix_time = logger_time + 946684800
    datetime_unaware = datetime.datetime.utcfromtimestamp(unix_time)
    datetime_aware = timezone.localize(datetime_unaware)
    return datetime_aware


def convert_python_time_to_rtc(pydatetime, timezone):
    """
    This is the reverse of convert_rtc_time_to_python
    :param pydatetime: A python time-zone aware datetime object
    :param timezone: the timezone of the arduino/RTC
    :return: an interger of seconds since January 1, 2000 in the RTC's timezone
    """
    datetime_aware = pydatetime.astimezone(timezone)
    unix_time = (datetime_aware - timezone.localize(datetime.datetime(1970, 1, 1))).total_seconds()
    sec_from_rtc_epoch = unix_time - 946684800
    return sec_from_rtc_epoch

# TODO: Reduce the number of API pings this takes.  Maybe add the locationId to the SQL?
def get_aquarius_timezone(ts_numeric_id, cookie=load_cookie):
    # Call up the Aquarius Acquisition SOAP API
    client = suds.client.Client(aq_acquisition_url, timeout=325)
    client.options.transport.cookiejar = cookie

    all_locations = client.service.GetAllLocations().LocationDTO
    for location in all_locations:
        utc_offset_float = location.UtcOffset
        utc_offset_string = '{:+3.0f}'.format(utc_offset_float*-1).strip()
        timezone = pytz.timezone('Etc/GMT'+utc_offset_string)
        all_descriptions_array = client.service.GetTimeSeriesListForLocation(location.LocationId)
        try:
            all_descriptions = all_descriptions_array.TimeSeriesDescription
        except AttributeError:
            pass
        else:
            for description in all_descriptions:
                ts_id = description.AqDataID
                if ts_id == ts_numeric_id:
                    return timezone
    return None


def get_data_from_dreamhost_table(table, column, series_start=None, series_end=None,
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
    if series_start is None:
        series_start = pytz.utc.localize(datetime.datetime(2000, 1, 1, 0, 0, 0))
    if series_end is None:
        series_end = datetime.datetime.now(pytz.utc) + datetime.timedelta(days=1)  # Future times clearly not valid
    # Set up an min and max time for when those values are NULL in dreamhost
    if query_start is None:
        query_start = pytz.utc.localize(datetime.datetime(2000, 1, 1, 0, 0, 0))
    if query_end is None:
        query_end = datetime.datetime.now(pytz.utc) + datetime.timedelta(days=1)  # Future times clearly not valid

    # Creating the query text here because the character masking works oddly
    # in the cur.execute function.

    # TODO: Take into account time zones from the dreamhost table.
    if table in ["davis", "CRDavis"]:
        # The meteobridges streaming this data stream a column of time in UTC
        dt_col = "mbutcdatetime"
        sql_start = max(series_start, query_start).astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
        sql_end = min(series_end, query_end).astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
    else:
        dt_col = "Loggertime"
        sql_start_py = max(series_start, query_start).astimezone(pytz.timezone('Etc/GMT+5'))
        sql_start = convert_python_time_to_rtc(sql_start_py, pytz.timezone('Etc/GMT+5'))
        sql_end_py = min(series_end, query_end).astimezone(pytz.timezone('Etc/GMT+5'))
        sql_end = convert_python_time_to_rtc(sql_end_py, pytz.timezone('Etc/GMT+5'))

    query_text = "SELECT DISTINCT " + dt_col + ", " + column + " as data_value " \
                 + "FROM " + table \
                 + " WHERE " + column + " IS NOT NULL " \
                 + " AND " + dt_col + " >= '" + str(sql_start) + "'" \
                 + " AND " + dt_col + " <= '" + str(sql_end) + "'" \
                 + " ORDER BY " + dt_col \
                 + " ;"

    if debug:
        print "   Data selected using the query:"
        print "   " + query_text
    t1 = datetime.datetime.now()

    # Set up connection to the DreamHost MySQL database
    conn = pymysql.connect(host=dbhost, db=dbname, user=dbuser, passwd=dbpswd)

    values_table = pd.read_sql(query_text, conn)

    # Close out the database connections
    conn.close()  # close the database connection

    if debug:
        print "   which returns %s values" % (len(values_table))
        t2 = datetime.datetime.now()
        print "   SQL execution took %s" % (t2 - t1)

    if len(values_table) > 0:
        if table in ["davis", "CRDavis"]:
            values_table['timestamp'] = values_table[dt_col]
            values_table.set_index(['timestamp'], inplace=True)
            values_table.drop(dt_col, axis=1, inplace=True)
            # NOTE:  The data going into Aquarius MUST already be in the same timezone as that series is in Aquarius
            # TODO: Fix timezones
            values_table.index = values_table.index.tz_localize('UTC')
            if table == "davis":
                values_table.index = values_table.index.tz_convert(pytz.timezone('Etc/GMT+5'))
            if table == "CRDavis":
                values_table.index = values_table.index.tz_convert(pytz.timezone('Etc/GMT+6'))
        else:
            # Need to convert arduino logger time into unix time (add 946684800)
            values_table['timestamp'] = np.vectorize(convert_rtc_time_to_python)(values_table[dt_col],
                                                                                 pytz.timezone('Etc/GMT+5'))
            values_table.set_index(['timestamp'], inplace=True)
            values_table.drop(dt_col, axis=1, inplace=True)
            values_table.index = values_table.index.tz_convert(pytz.timezone('Etc/GMT+5'))

        if debug:
            print "The first and last rows to append:"
            print values_table.head(1)
            print values_table.tail(1)

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

    if len(data_table) > 0:
        if 'flag' not in data_table:
            data_table.loc[:, 'flag'] = ""
        if 'grade' not in data_table:
            data_table.loc[:, 'grade'] = ""
        if 'interpolation' not in data_table:
            data_table.loc[:, 'interpolation'] = ""
        if 'approval' not in data_table:
            data_table.loc[:, 'approval'] = ""
        if 'note' not in data_table:
            data_table.loc[:, 'note'] = ""

        # Output a CSV
        csvdata = data_table.to_csv(header=False, date_format='%Y-%m-%d %H:%M:%S')

        # Convert the data string into a base64 object
        csvbytes = base64.b64encode(csvdata)

    else:
        csvbytes = ""

    return csvbytes


def aq_timeseries_append(ts_numeric_id, appendbytes, cookie=load_cookie, debug=False):
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

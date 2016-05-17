# -*- coding: utf-8 -*-

"""
Created on Wed Nov 05 13:58:15 2014

@author: Sara Geleskie Damiano

This script moves all data from series tagged with an Aquarius dataset primary key
from a DreamHost database to Stroud's Aquarius server.
This version includes only data from the last hour.
"""

import datetime
import pytz
import os
import aq_functions as aq

# Bring in all of the database connection inforamation.
from dbinfo import logdirectory

__author__ = 'Sara Geleskie Damiano'

# Set up logging initial parameters.
Log_to_file = True  # This will save a log in an external text file.
past_hours_to_append = 1  # Number of hours in the past to append, use None to append entire record
Logger = None  # If you want to append data from only one logger, use None for all loggers
Column = None  # If you want to append data from a specific data column, use None for all columns


# Find the date/time the script was started:
start_datetime_utc = datetime.datetime.now(pytz.utc)
# Deal with timezones...
eastern_standard_time = pytz.timezone('Etc/GMT+5')
eastern_local_time = pytz.timezone('US/Eastern')
costa_rica_time = pytz.timezone('Etc/GMT+6')
start_datetime_est = start_datetime_utc.astimezone(eastern_standard_time)
start_datetime_loc = start_datetime_utc.astimezone(eastern_local_time)

if __debug__:
    print "Script started at %s" % start_datetime_loc

# Open file for logging
if Log_to_file:
    # Get the path and directory of this script:
    filename = os.path.realpath(__file__)
    # Open up a text file to log to
    logfile = logdirectory + "\AppendLog_" + start_datetime_loc.strftime("%Y%m%d") + ".txt"
    text_file = open(logfile, "a+")
    text_file.write("Script: %s \n" % filename)
    text_file.write("Script started at %s \n \n" % start_datetime_loc)

# Get an authentication token for the SOAP API
AuthToken = aq.get_aq_auth_token()

# Set the time cutoff for recent series.
if past_hours_to_append is None:
    current_timeseries_cutoff_dt_est = None
else:
    current_timeseries_cutoff_utc = start_datetime_utc - datetime.timedelta(hours=past_hours_to_append*2)
    current_timeseries_cutoff_est = current_timeseries_cutoff_utc.astimezone(eastern_standard_time)
    query_start_utc = start_datetime_utc - datetime.timedelta(hours=past_hours_to_append)


# Get data for all series that are available
AqSeries = aq.get_dreamhost_series(cutoff_for_recent=current_timeseries_cutoff_dt_est,
                                   logger=Logger, column=Column)
if Log_to_file:
    text_file.write("%s series found with corresponding time series in Aquarius \n \n" % (len(AqSeries)))
    text_file.write("Series, Table, Column, NumericIdentifier, TextIdentifier, NumPointsAppended, AppendToken  \n")


loopnum = 1
for ts_numeric_id, table_name, table_column_name, series_start, series_end in AqSeries:
    if __debug__:
        print "Attempting to append series %s of %s" % (loopnum, len(AqSeries))
        print "Data being appended to Time Series # %s" % ts_numeric_id

    if past_hours_to_append is None:
        query_start = None
    else:
        if table_name == "CRDavis":
            query_start = query_start_utc.astimezone(costa_rica_time)
        else:
            query_start = query_start_utc.astimezone(eastern_standard_time)

    appendbytes = aq.get_data_from_dreamhost_table(table_name, table_column_name,
                                                   series_start, series_end,
                                                   query_start=query_start, query_end=None)

    AppendResult = aq.aq_timeseries_append(ts_numeric_id, appendbytes)

    if Log_to_file:
        text_file.write("%s, %s, %s, %s, %s, %s, %s \n"
                        % (loopnum, table_name, table_column_name,
                           ts_numeric_id, AppendResult.TsIdentifier,
                           AppendResult.NumPointsAppended, AppendResult.AppendToken))

    loopnum += 1


# Find the date/time the script finished:
end_datetime_utc = datetime.datetime.now(pytz.utc)
end_datetime_loc = end_datetime_utc.astimezone(eastern_local_time)
runtime = end_datetime_utc - start_datetime_utc

# Close out the text file
if __debug__:
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

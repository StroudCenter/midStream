# -*- coding: utf-8 -*-

"""
Created on Wed Nov 05 13:58:15 2014

@author: Sara Geleskie Damiano

This script moves all data from series tagged with an Aquarius dataset primary key
from a DreamHost database to Stroud's Aquarius server.
This uses command line arguments to decide what to append
"""

import datetime
import pytz
import os
import sys
import argparse
import aq_functions as aq

# Set up initial parameters - these are rewritten when run from the command prompt.
past_hours_to_append = None  # Sets number of hours in the past to append, use None for all time
table = "SL031"  # Selects a single table to append from, often a logger number, use None for all loggers
column = "CTDcond"  # Selects a single column to append from, often a variable code, use None for all columns


# Set up a parser for command line options
parser = argparse.ArgumentParser(description='This script appends data from Dreamhost to Aquarius.')
parser.add_argument('--debug', action='store_true',
                    help='Turn debugging on')
parser.add_argument('--nolog', action='store_false',
                    help='Turn logging off')
parser.add_argument('--hours', action='store', type=int, default=None,
                    help='Sets number of hours in the past to append')
parser.add_argument('--table', action='store', default=None,
                    help='Selects a single table to append from, often a logger number')
parser.add_argument('--col', action='store', default=None,
                    help='Selects a single column to append from, often a variable code')

# Read the command line options, if run from the command line
if sys.stdin.isatty():
    debug = parser.parse_args().debug
    Log_to_file = parser.parse_args().nolog
    past_hours_to_append = parser.parse_args().hours
    table = parser.parse_args().table
    column = parser.parse_args().col
else:
    debug = True
    Log_to_file = True


# Find the date/time the script was started:
start_datetime_utc = datetime.datetime.now(pytz.utc)
# Deal with timezones...
eastern_standard_time = pytz.timezone('Etc/GMT+5')
eastern_local_time = pytz.timezone('US/Eastern')
costa_rica_time = pytz.timezone('Etc/GMT+6')
start_datetime_est = start_datetime_utc.astimezone(eastern_standard_time)
start_datetime_loc = start_datetime_utc.astimezone(eastern_local_time)

# Get the path and directory of this script:
script_name_with_path = os.path.realpath(__file__)
script_directory = os.path.dirname(os.path.realpath(__file__))

if debug:
    print "Now running script: %s" % script_name_with_path
    print "Script started at %s" % start_datetime_loc

# Open file for logging
if Log_to_file:
    logfile = script_directory + "\AppendLogs\AppendLog_" + start_datetime_loc.strftime("%Y%m%d") + ".txt"
    if debug:
        print "Log being written to: %s" % logfile
    text_file = open(logfile, "a+")
    text_file.write("Script: %s \n" % script_name_with_path)
    text_file.write("Script started at %s \n \n" % start_datetime_loc)
else:
    text_file = ""


# Set the time cutoff for recent series.
if past_hours_to_append is None:
    current_timeseries_cutoff_est = None
else:
    current_timeseries_cutoff_utc = start_datetime_utc - datetime.timedelta(hours=past_hours_to_append*2)
    current_timeseries_cutoff_est = current_timeseries_cutoff_utc.astimezone(eastern_standard_time)
    query_start_utc = start_datetime_utc - datetime.timedelta(hours=past_hours_to_append)


# Get an authentication token and sessionID for Aquarius
# Doing this once here instead of in the aq.aq_timeseries_append function
# to avoid opening a new session for each timeseries appended.
token, cookie = aq.get_aq_auth_token(debug=debug)


# Get data for all series that are available
AqSeries = aq.get_dreamhost_series(cutoff_for_recent=current_timeseries_cutoff_est,
                                   table=table, column=column, debug=debug)
if Log_to_file:
    text_file.write("%s series found with corresponding time series in Aquarius \n \n" % (len(AqSeries)))
    text_file.write("Series, Table, Column, NumericIdentifier, TextIdentifier, NumPointsAppended, AppendToken  \n")


loopnum = 1
for ts_numeric_id, table_name, table_column_name, series_start, series_end in AqSeries:
    if debug:
        print "Attempting to append series %s of %s" % (loopnum, len(AqSeries))
        print "Data being appended to Time Series # %s" % ts_numeric_id

    if past_hours_to_append is None:
        query_start = None
    else:
        if table_name == "CRDavis":
            query_start = query_start_utc.astimezone(costa_rica_time)
        else:
            query_start = query_start_utc.astimezone(eastern_standard_time)

    data_table = aq.get_data_from_dreamhost_table(table_name, table_column_name,
                                                  series_start, series_end,
                                                  query_start=query_start, query_end=None,
                                                  debug=debug)
    append_bytes = aq.create_appendable_csv(data_table)

    AppendResult = aq.aq_timeseries_append(ts_numeric_id, append_bytes, debug=debug, cookie=cookie)

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

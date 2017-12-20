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
import time
import numpy as np

__author__ = 'Sara Geleskie Damiano'
__contact__ = 'sdamiano@stroudcenter.org'

# Set up initial parameters - these are rewritten when run from the command prompt.
past_hours_to_append = None  # Sets number of hours in the past to append.  Use None for all time
append_start = None  # Sets start time for the append **in EST**, use None for all time
append_end = None  # Sets end time for the append **in EST**, use None for all time
# append_start = "2017-04-01 00:00:00"  # Sets start time for the append **in EST**, use None for all time
# append_end = "2017-05-01 00:00:00"  # Sets end time for the append **in EST**, use None for all time
table = "SL042"  # Selects a single table to append from, often a logger number, use None for all loggers
column = None  # Selects a single column to append from, often a variable code, use None for all columns


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
    append_start = None
    append_end = None
    table = parser.parse_args().table
    column = parser.parse_args().col
else:
    debug = True
    Log_to_file = True

# Deal with timezones...
eastern_standard_time = pytz.timezone('Etc/GMT+5')
eastern_local_time = pytz.timezone('US/Eastern')


def start_log():
    # Find the date/time the script was started:
    start_log_dt_utc = datetime.datetime.now(pytz.utc)
    start_log_dt_loc = start_log_dt_utc.astimezone(eastern_local_time)

    # Get the path and directory of this script:
    script_name_with_path = os.path.realpath(__file__)
    script_directory = os.path.dirname(os.path.realpath(__file__))

    if debug:
        print "Now running script: %s" % script_name_with_path
        print "Script started at %s" % start_log_dt_loc

    # Open file for logging
    if Log_to_file:
        logfile = script_directory + "\AppendLogs\AppendLog_" + start_log_dt_loc.strftime("%Y%m%d") + ".txt"
        if debug:
            print "Log being written to: %s" % logfile
        open_log_file = open(logfile, "a+")

        open_log_file.write(
            "*******************************************************************************************************\n")
        open_log_file.write("Script: %s \n" % script_name_with_path)
        open_log_file.write(
            "*******************************************************************************************************\n")
        open_log_file.write("\n")
        open_log_file.write("Script started at %s \n \n" % start_log_dt_loc)
    else:
        open_log_file = ""

    return open_log_file, start_log_dt_utc


def end_log(open_log_file, start_log_dt_utc):
    # Find the date/time the script finished:
    end_datetime_utc = datetime.datetime.now(pytz.utc)
    end_datetime_loc = end_datetime_utc.astimezone(eastern_local_time)
    runtime = end_datetime_utc - start_log_dt_utc

    # Close out the text file
    if debug:
        print "Script completed at %s" % end_datetime_loc
        print "Total time for script: %s" % runtime
    if Log_to_file:
        open_log_file.write("\n")
        open_log_file.write("Script completed at %s \n" % end_datetime_loc)
        open_log_file.write("Total time for script: %s \n" % runtime)
        open_log_file.write(
            "*******************************************************************************************************\n")
        open_log_file.write("\n \n")
        open_log_file.close()


def check_valid_connection():
    """
    Check that there is a valid connection open to the Aquarius server.  If not, abort script.
    NOTE:  If no connection to the server at all is established or an authentication token cannot be returned, the
    program will quit immediately upon loading the aq_functions module.  This will catch if the connection has died.
    """
    start_check = datetime.datetime.now()
    if debug:
        print "Checking for valid connection"
    is_valid, error = aq.check_aq_connection()
    if not is_valid:
        # If the connection has died, print out a note and write to the log, then kill script.
        if debug:
            print "Aborting script because no valid connection has been established with the Aquarius server."
            print "Server returned error: %s" % error
        if Log_to_file:
            text_file.write("Script aborted because no valid connection was established with the Aquarius server.\n")
            text_file.write("Server returned error: %s\n" % error)
        end_log(text_file, start_datetime_utc)
        sys.exit("Unable to connect to server")
    else:
        end_check = datetime.datetime.now()
        if debug:
            print "Valid connection returned after %s seconds" % (end_check - start_check)
    return


# Open the log
text_file, start_datetime_utc = start_log()

# Check for a valid connection
check_valid_connection()


# Set the time cutoff for recent series.
if append_start is None:
    append_start_dt = None
else:
    append_start_dt_naive = datetime.datetime.strptime(append_start,"%Y-%m-%d %H:%M:%S")
    append_start_dt = append_start_dt_naive.replace(tzinfo=eastern_standard_time)

if append_end is None:
    append_end_dt = None
else:
    append_end_dt_naive = datetime.datetime.strptime(append_end,"%Y-%m-%d %H:%M:%S")
    append_end_dt = append_end_dt_naive.replace(tzinfo=eastern_standard_time)

if append_start is None and append_end is None and past_hours_to_append is not None:
    append_end_dt = None
    append_start_utc = start_datetime_utc - datetime.timedelta(hours=past_hours_to_append+1)
    append_start_dt = append_start_utc.astimezone(eastern_standard_time)


# Get data for all series that are available
AqSeries = aq.get_dreamhost_series(query_start=append_start_dt, query_end=append_end_dt,
                                   table=table, column=column, debug=debug)
if Log_to_file:
    text_file.write("%s series found with corresponding time series in Aquarius \n \n" % (len(AqSeries)))
    text_file.write("Series, Table, Column, NumericIdentifier, TextIdentifier, NumPointsAppended, AppendToken  \n")


# Looping through each time series and appending the data
i = 1
for ts_numeric_id, loc_numeric_id, table_name, table_column_name, series_tz, series_start, series_end in AqSeries:
    if debug:  # Some blank lines to differentiate the new dataset
        print ""
        print ""
    check_valid_connection()
    if debug:
        print "Attempting to append series %s of %s" % (i, len(AqSeries))
        print "Data being appended to Time Series # %s" % ts_numeric_id

    aq_series_timezone = aq.get_aquarius_timezone(ts_numeric_id, loc_numeric_id)
    if debug:
        print "Time Zone of Series on Dreamhost is %s" % series_tz
        print "Time Zone of Series in Aquarius is %s" % aq_series_timezone

    if append_start_dt is None:
        query_start = None
    elif table_name in ["davis", "CRDavis"]:
        query_start = append_start_dt.astimezone(pytz.utc)
    else:
        query_start = append_start_dt.astimezone(series_tz)

    if append_end_dt is None:
        query_end = None
    elif table_name in ["davis", "CRDavis"]:
        query_end = append_end_dt.astimezone(pytz.utc)
    else:
        query_end = append_end_dt.astimezone(series_tz)

    data_table = aq.get_data_from_dreamhost_table(table_name, table_column_name,
                                                  series_start, series_end,
                                                  query_start=query_start, query_end=query_end,
                                                  debug=debug)

    append_bytes = aq.create_appendable_csv(data_table)
    AppendResult = aq.aq_timeseries_append(ts_numeric_id, append_bytes, debug=debug)
    # TODO: stop execution of further requests after an error.
    if Log_to_file:
        text_file.write("%s, %s, %s, %s, %s, %s, %s \n"
                        % (i, table_name, table_column_name,
                           ts_numeric_id, AppendResult.TsIdentifier,
                           AppendResult.NumPointsAppended, AppendResult.AppendToken))
    time.sleep(1)

    i += 1


# Close out the text file
end_log(text_file, start_datetime_utc)

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
past_hours_to_append = 366  # Sets number of hours in the past to append, use None for all time
table = None  # Selects a single table to append from, often a logger number, use None for all loggers
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
    table = parser.parse_args().table
    column = parser.parse_args().col
else:
    debug = True
    Log_to_file = True

# Deal with timezones...
eastern_standard_time = pytz.timezone('Etc/GMT+5')
eastern_local_time = pytz.timezone('US/Eastern')
costa_rica_time = pytz.timezone('Etc/GMT+6')


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
if past_hours_to_append is None:
    current_timeseries_cutoff_est = None
else:
    current_timeseries_cutoff_utc = start_datetime_utc - datetime.timedelta(hours=past_hours_to_append*2)
    current_timeseries_cutoff_est = current_timeseries_cutoff_utc.astimezone(eastern_standard_time)
    query_start_utc = start_datetime_utc - datetime.timedelta(hours=past_hours_to_append)


# Get data for all series that are available
AqSeries = aq.get_dreamhost_series(cutoff_for_recent=current_timeseries_cutoff_est,
                                   table=table, column=column, debug=debug)
if Log_to_file:
    text_file.write("%s series found with corresponding time series in Aquarius \n \n" % (len(AqSeries)))
    text_file.write("Series, Table, Column, NumericIdentifier, TextIdentifier, NumPointsAppended, AppendToken  \n")


# Looping through each time series and appending the data
i = 1
for ts_numeric_id, table_name, table_column_name, series_start, series_end in AqSeries:
    check_valid_connection()
    if debug:
        print "Attempting to append series %s of %s" % (i, len(AqSeries))
        print "Data being appended to Time Series # %s" % ts_numeric_id

    if past_hours_to_append is None:
        query_start = None
    else:
        if table_name in ["davis", "CRDavis"]:
            query_start = query_start_utc.astimezone(pytz.utc)
        else:
            query_start = query_start_utc.astimezone(eastern_standard_time)

    data_table = aq.get_data_from_dreamhost_table(table_name, table_column_name,
                                                  series_start, series_end,
                                                  query_start=query_start, query_end=None,
                                                  debug=debug)

    # if len(data_table) > 10000:
    #     grouped = data_table.groupby(np.arange(len(data_table))//7500)
    #
    #     j = 1
    #     for name, group in grouped:
    #         if debug:
    #             print "Appending chunk # %s of %s with %s values beginning on %s" % \
    #                   (j, len(grouped), len(group), group.index.get_value(group.index, 0))
    #         k = i + j/100
    #         append_bytes = aq.create_appendable_csv(group)
    #         AppendResult = aq.aq_timeseries_append(ts_numeric_id, append_bytes, debug=debug)
    #         if Log_to_file:
    #             text_file.write("%s, %s, %s, %s, %s, %s, %s \n"
    #                             % (k, table_name, table_column_name,
    #                                ts_numeric_id, AppendResult.TsIdentifier,
    #                                AppendResult.NumPointsAppended, AppendResult.AppendToken))
    #         time.sleep(325)
    #         j += 1
    #
    # else:
    #     if debug:
    #         print "Appending in a single call to the API"

    append_bytes = aq.create_appendable_csv(data_table)
    AppendResult = aq.aq_timeseries_append(ts_numeric_id, append_bytes, debug=debug)
    # TODO: stop execution of further requests after an error.
    if Log_to_file:
        text_file.write("%s, %s, %s, %s, %s, %s, %s \n"
                        % (i, table_name, table_column_name,
                           ts_numeric_id, AppendResult.TsIdentifier,
                           AppendResult.NumPointsAppended, AppendResult.AppendToken))
    # time.sleep(1)

    i += 1


# Close out the text file
end_log(text_file, start_datetime_utc)

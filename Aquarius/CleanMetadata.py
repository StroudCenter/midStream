# -*- coding: utf-8 -*-


"""
Created by Sara Geleskie Damiano on 6/29/2016 at 2:30 PM

__author__ = 'Sara Geleskie Damiano'
__contact__ = 'sdamiano@stroudcenter.org'

"""

import pymssql
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
import os
import datetime
import pytz
import argparse
import sys

from dbinfo import aqdb_host, aqdb_name, aqdb_user, aqdb_password

__author__ = 'Sara Geleskie Damiano'
__contact__ = 'sdamiano@stroudcenter.org'

pd.set_option('expand_frame_repr', False)
pd.set_option('max_colwidth', 500)
pd.set_option('display.max_columns', 0)


# Find the date/time the script was started:
start_datetime_utc = datetime.datetime.now(pytz.utc)
eastern_local_time = pytz.timezone('US/Eastern')
start_datetime_loc = start_datetime_utc.astimezone(eastern_local_time)

# Get the path and directory of this script:
script_name_with_path = os.path.realpath(__file__)
script_directory = os.path.dirname(os.path.realpath(__file__))

# Set up a parser for command line options
parser = argparse.ArgumentParser(description='This script cleans excess metadata from the Aquarius database.')
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
else:
    text_file = ""

# Set up the connection directly to the Aquarius SQL Database
conn = pymssql.connect(server=aqdb_host, user=aqdb_user, password=aqdb_password, database=aqdb_name)
cur = conn.cursor()

# Set a where clause for the SQL query
# where_clause = """
# WHERE
# TimeSeriesID in (SELECT
# AQDataID_
# FROM AQAtom_TimeSeries_
# WHERE AQParentID_ = (SELECT
# LocationID
# FROM Location
# WHERE Identifier = 'LGTUpstream'))
# """
# # where_clause = """
# WHERE
# TimeSeriesID = 9129758
# """
where_clause = ""

# Read TimeSeries information into pandas
ts_df_query = """
SELECT ts.AQDataID_  as TimeSeriesID, 
       ts.label_ as TS_label,
       Location.Identifier as LocationCode,
       ts.parameterType_ as TS_Parm_Code,
       parameter.displayid as Parameter
FROM AQAtom_TimeSeries_ as ts
LEFT JOIN Location
ON ts.AQParentID_ = Location.LocationID
LEFT JOIN parameter 
ON ts.parameterType_ = parameter.parameterid
"""
ts_df = pd.read_sql(ts_df_query, conn)
ts_df['TS_Text_ID'] = ts_df['Parameter'] + '.' + ts_df['TS_label'] + '@' + ts_df['LocationCode']
# Read the metadata table into pandas
meta_df = pd.read_sql("SELECT * FROM TimeSeriesMeta" + where_clause, conn)

# Expand the XML "Blob" of sub-meta-data into columns of its own.
meta_df['blob_dict'] = meta_df['XmlBlob'].apply(lambda x: ET.fromstring(x).attrib)
meta_expand = pd.concat([meta_df, pd.DataFrame((d for idx, d in meta_df['blob_dict'].iteritems()))], axis=1)
full_df = meta_expand.merge(ts_df, on='TimeSeriesID')
oneblob = ET.fromstring(meta_df.loc[1, 'XmlBlob'])

# Clean up Data Types
if full_df['StartTime'].dtype == np.dtype('datetime64[ns]'):
    full_df['startTime_dt'] = pd.to_datetime(full_df['startTime'], errors='ignore', format='%Y-%m-%d %H:%M:%S.%f')
else:
    full_df['startTime'] = full_df['startTime'].fillna(value='1899-12-30 00:00:00.000')
    full_df['startTime_dt'] = full_df['startTime'].apply(lambda x: pd.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S.%f'))
if full_df['EndTime'].dtype == np.dtype('datetime64[ns]'):
    full_df['endTime_dt'] = pd.to_datetime(full_df['endTime'], errors='ignore', format='%Y-%m-%d %H:%M:%S.%f')
else:
    full_df['endTime'] = full_df['endTime'].fillna(value='4637-11-26 00:00:00.000')
    full_df['endTime_dt'] = full_df['endTime'].apply(lambda x: pd.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S.%f'))
full_df['dateApplied'] = pd.to_numeric(full_df['dateApplied'])
full_df['dateAppliedTZBias'] = pd.to_numeric(full_df['dateAppliedTZBias'])
if 'modifiedPoints' in full_df.columns:
    full_df['modifiedPoints'] = pd.to_numeric(full_df['modifiedPoints'])

# Sort the list
full_df.sort_values(by=['TimeSeriesID', 'TypeName', 'DateApplied', 'DateModified'], inplace=True)
full_df.fillna(value=-9999, inplace=True)
if debug:
    print "There are %s Total Metadata Records" % len(full_df)
if Log_to_file:
    text_file.write("There are %s Total Metadata Records\n" % len(full_df))

# These are columns that are unique for every meta-data row
unique_cols = ['MetaID', 'XmlBlob', 'blob_dict', 'AQMetadataID', 'StartTime', 'startTime', 'startTime_dt',
               'EndTime', 'endTime', 'endTime_dt', 'LastModifiedTime', 'DateModified']
non_unique_cols = full_df.columns.values.tolist()
for item in unique_cols:
    non_unique_cols.remove(item)


# Update metadata for ranges that changed
dedupped = full_df.drop_duplicates(subset=non_unique_cols, keep='last')

full_df_grouped = full_df.groupby(non_unique_cols)

aggregates = pd.DataFrame({'count': full_df_grouped.size(),
                           'min_start': full_df_grouped['StartTime'].min(),
                           'max_end': full_df_grouped['EndTime'].max()
                           }).reset_index()

dedupped_aggr = pd.merge(dedupped, aggregates, on=non_unique_cols)
values_to_update = dedupped_aggr[(dedupped_aggr['StartTime'] != dedupped_aggr['min_start']) |
                                 (dedupped_aggr['EndTime'] != dedupped_aggr['max_end'])].reset_index(drop=True)

if len(values_to_update) > 0:

    if values_to_update['min_start'].dtype == np.dtype('datetime64[ns]'):
        values_to_update['min_start_str'] = values_to_update['min_start'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
    else:
        values_to_update['min_start_str'] = values_to_update['min_start'].apply(
            lambda x: "%02d-%02d-%02d %02d:%02d:%02d.%03d" %
            (x.year, x.month, x.day, x.hour, x.minute, x.second, x.microsecond))
    values_to_update['new_blob'] = values_to_update['XmlBlob'].replace(
        to_replace='(startTime="[0-9-]{10}\s[0-:."]{13})',
        value='startTime="'+values_to_update['min_start_str']+'"',
        regex=True)
    
    if values_to_update['max_end'].dtype == np.dtype('datetime64[ns]'):
        values_to_update['max_end_str'] = values_to_update['max_end'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
    else:
        values_to_update['max_end_str'] = values_to_update['max_end'].apply(
            lambda x: "%02d-%02d-%02d %02d:%02d:%02d.%03d" %
            (x.year, x.month, x.day, x.hour, x.minute, x.second, x.microsecond))
    values_to_update['new_blob2'] = values_to_update['new_blob'].replace(
        to_replace='(endTime="[0-9-]{10}\s[0-:."]{13})',
        value='endTime="'+values_to_update['max_end_str']+'"',
        regex=True)
    
    if debug:
        print "Updating %s Records" % len(values_to_update)
    if Log_to_file:
        text_file.write("Updating %s Records\n" % len(values_to_update))
    update_group = values_to_update.groupby(['TimeSeriesID'])
    for name, group in update_group:
        if len(group) > 0:
            if debug:
                print "    Updating %s Records From TimeSeries # %s (%s)"\
                      % (len(group), name, group['TS_Text_ID'].iloc[0])
            if Log_to_file:
                text_file.write("    Updating %s Records From TimeSeries # %s (%s)\n"
                                % (len(group), name, group['TS_Text_ID'].iloc[0]))
            for index, row in group.iterrows():
                sql_update = """UPDATE TimeSeriesMeta
                                SET StartTime='%s',
                                EndTime='%s',
                                XmlBlob=CONVERT(VARBINARY(max), '%s')
                                WHERE MetaID='%s';
                                """ % \
                      (row['min_start_str'], row['max_end_str'], row['new_blob2'], str(row['MetaID']))
                cur.execute(sql_update)
            conn.commit()
else:    
    if debug:
        print "No Records to Update"
    if Log_to_file:
        text_file.write("No Records to Update\n")


# Remove Duplicates from the SQL
dups = full_df[full_df.duplicated(subset=non_unique_cols, keep='last')]
if len(dups) > 0:
    if debug:
        print "Deleting %s Duplicated Records" % len(dups)
    if Log_to_file:
        text_file.write("Deleting %s Duplicated Records\n" % len(dups))
    # Doing by Group to help avoid overly massive delete statements
    dups_group = dups.groupby(['TimeSeriesID', 'TypeName'])
    for name, group in dups_group:
        if len(group) > 0:
            try:
                meta_to_delete_str = str(list(group['MetaID'].values)).replace('UUID(', '').replace(')', '').strip('[]')
                sql_delete = 'DELETE FROM TimeSeriesMeta WHERE MetaID IN ({0})'.format(meta_to_delete_str)
                if debug:
                    print "    Deleting %s Duplicate Records From TimeSeries # %s (%s)"\
                          % (len(group), name, group['TS_Text_ID'].iloc[0])
                if Log_to_file:
                    text_file.write("    Deleting %s Duplicate Records From TimeSeries # %s (%s)\n"
                                    % (len(group), name, group['TS_Text_ID'].iloc[0]))
                cur.execute(sql_delete)
                conn.commit()
            except:
                if debug:
                    print "    Delete Failed For TimeSeries # %s (%s)" % (name, group['TS_Text_ID'].iloc[0])
                if Log_to_file:
                    text_file.write("    Delete Failed For TimeSeries # %s (%s)\n"
                                    % (name, group['TS_Text_ID'].iloc[0]))
else:
    if debug:
        print "No Duplicate Records to Delete"
    if Log_to_file:
        text_file.write("No Duplicate Records to Delete\n")
    
    
# Remove junk notes
junk_notes = ['Correction was edited', 'Correction was marked as undone', 'Correction was marked as done']
junk_note_df = full_df[(full_df['TypeName'] == 'NOTE') & (full_df['comment'].isin(junk_notes))]
if len(junk_note_df) > 0:
    if debug:
        print "Deleting %s Junk Notes" % len(junk_note_df)
    if Log_to_file:
        text_file.write("Deleting %s Junk Notes\n" % len(junk_note_df))
    # Doing by Group to help avoid overly massive delete statements
    junk_note_group = junk_note_df.groupby(['TimeSeriesID'])
    for name, group in junk_note_group:
        if len(group) > 0:
            meta_to_delete_str = str(list(group['MetaID'].values)).replace('UUID(', '').replace(')', '').strip('[]')
            sql_delete = 'DELETE FROM TimeSeriesMeta WHERE MetaID IN ({0})'.format(meta_to_delete_str)
            if debug:
                print "    Deleting %s Junk Notes From TimeSeries # %s (%s)"\
                      % (len(group), name, group['TS_Text_ID'].iloc[0])

            if Log_to_file:
                text_file.write("    Deleting %s Junk Notes From TimeSeries # %s (%s)\n"
                                % (len(group), name, group['TS_Text_ID'].iloc[0]))
            cur.execute(sql_delete)
            conn.commit()
else:
    if debug:
        print "No Junk Notes to Delete"
    if Log_to_file:
        text_file.write("No Junk Notes to Delete\n")
        

# Close out the database connections
cur.close()  # close the database cursor
conn.close()  # close the database connection


# Print out what happened
by_ts_full = pd.DataFrame({'total_meta': full_df.groupby(['TimeSeriesID']).size()})
by_ts_unique = pd.DataFrame({'unique_meta': dedupped_aggr.groupby(['TimeSeriesID']).size()})
by_ts_dups = pd.DataFrame({'deleted_dups': dups.groupby(['TimeSeriesID']).size()})
by_ts_junk = pd.DataFrame({'deleted_junk': junk_note_df.groupby(['TimeSeriesID']).size()})
by_ts_updated = pd.DataFrame({'updated_meta': values_to_update.groupby(['TimeSeriesID']).size()})
by_ts_counts = pd.concat([by_ts_full, by_ts_unique, by_ts_dups, by_ts_junk, by_ts_updated], axis=1).merge(
    ts_df[['TimeSeriesID', 'TS_Text_ID']], left_index=True, right_on='TimeSeriesID').fillna(0)
if Log_to_file:
    text_file.write("NumericIdentifier, TextIdentifier, TotalMetadata, UniqueMetadata,"
                    " DeletedDeplicates, DeletedJunk, UpdatedRecords  \n")
for index, row in by_ts_counts.iterrows():
    if Log_to_file:
        text_file.write("%s, %s, %s, %s, %s, %s, %s  \n" %
                        (row['TimeSeriesID'], row['TS_Text_ID'], row['total_meta'], row['unique_meta'],
                         row['deleted_dups'], row['deleted_junk'], row['updated_meta']))


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

# -*- coding: utf-8 -*-

__author__ = 'Sara Geleskie Damiano'

"""
Created on Wed Nov 05 13:58:15 2014

@author: Sara Geleskie Damiano

This script moves all data from the dreamhost database to Stround's Aquarius server for a single manually selected
time series
"""

#Set up logging to an external file if desired
Log_to_file = True

import suds

#==============================================================================
# #This would log any issues with the SUDS connection.
# import logging
# logging.basicConfig(level=logging.INFO)
# logging.getLogger('suds.client').setLevel(logging.DEBUG)
# logging.getLogger('suds.transport').setLevel(logging.DEBUG)
# logging.getLogger('suds.xsd.schema').setLevel(logging.DEBUG)
# logging.getLogger('suds.wsdl').setLevel(logging.DEBUG)
#==============================================================================

import pymysql
import datetime
import pytz
import base64
import os
import sys

#Bring in all of the database connection inforamation.
from dbinfo import aq_url, aq_username, aq_password, dbhost, dbname, dbuser, dbpswd, logdirectory


#Call up the Aquarius Aquisition SOAP API
client = suds.client.Client(aq_url)


# AqSeries = ((AQTimeSeriesID, TableName, TableColumnName),)
# AqSeries = ((3257206, 'SL034', 'CTDdepth'),)
# AqSeries = ((3257207, 'SL034', 'CTDtemp'),)
# AqSeries = ((3257205, 'SL034', 'CTDcond'),)
# AqSeries = ((3257212, 'SL034', 'TurbLow'),)
# AqSeries = ((3257210, 'SL034', 'TurbHigh'),)
# AqSeries = ((3257208, 'SL034', 'Battery'),)

# AqSeries = ((6256245, 'SL034', 'CTDdepth'),)
# AqSeries = ((6256246, 'SL034', 'CTDtemp'),)
# AqSeries = ((6256247, 'SL034', 'CTDcond'),)
# AqSeries = ((6256248, 'SL034', 'TurbLow'),)
# AqSeries = ((6256249, 'SL034', 'TurbHigh'),)
AqSeries = ((6256250, 'SL034', 'Battery'),)
# AqSeries = ((161540, 'davis', 'temperature'),)

# AqSeries = ((3786998, 'SL054', 'CTDcond'),)

if Log_to_file:
    # Find the date/time the script was started:
    start_datetime = datetime.datetime.now()
    # Get the path and directory of this script:
    filename = os.path.realpath(__file__)
    #Open up a text file to log to
    logfile = logdirectory + "\AppendLog_" + start_datetime.strftime("%Y%m%d") + ".txt"
    text_file = open(logfile, "a+")
    text_file.write("Script: %s \n" % filename)
    text_file.write("Script started at %s \n \n" % start_datetime)
    text_file.write("Manually Attempting Appends from table %s column %s to Aquarius series %s" %
                    (AqSeries[0][1], AqSeries[0][2], AqSeries[0][0]))
    text_file.write("Series, Table, Column, TimeSeriesIdentifier, NumPointsAppended, AppendToken  \n")


#Get an authentication token to open the path into the API
AuthToken = client.service.GetAuthToken(aq_username,aq_password)
print "Authentication Token: %s" % (AuthToken)



#Call data from the MySQL database
conn=pymysql.connect(host=dbhost,db=dbname,user=dbuser,passwd=dbpswd)
def get_data_from_table(Table,Column):
    """Returns a base64 data object with the data from a given table and column

        Required Arguments:
            table name, column name
    """
    #Creating the query text here because the character masking works oddly
    #in the cur.execute function.
    if Table in ("davis" "CRDavis"):
        query_text = "SELECT Date, " + Column + " FROM " + Table + " WHERE " \
                     + Column + " IS NOT NULL " + ";"
    else:
        query_text = "SELECT Loggertime, " + Column + " FROM " + Table + " WHERE " \
                     + Column + " IS NOT NULL " + ";"

    conn=pymysql.connect(host=dbhost,db=dbname,user=dbuser,passwd=dbpswd)
    cur = conn.cursor()

    cur.execute(query_text)
        
    values_table = cur.fetchall()
    
    cur.close()     # close the database cursor
    conn.close()    # close the database connection
    
    print "Data selected using the query:"
    print query_text    
    print "which returns %s values" % (len(values_table))
    
    # if Log_to_file:
    #     text_file.write("Data selected using the query: \n")
    #     text_file.write("%s \n" % (query_text))
    #     text_file.write("which returns %s values \n" % (len(values_table)))
    
    #Create a comma separated string of the data in the database
    csvdata = ''
    csvdata2 = ''
    for timestamp, value in values_table:
        if Table in ("davis" "CRDavis"):
            csvdata2 += csvdata.join("\n".join(["%s"",""%s" % (timestamp.isoformat(' '), value)]) + "\n")
        else:
            # Need to convert arduino logger time into unix time (add 946684800)
            # and then to UTC-5 (add 18000)
            timestamp_dt = datetime.datetime.fromtimestamp(timestamp+946684800+18000,tz=pytz.timezone('EST'))
            timestamp_dt2 = timestamp_dt.replace(tzinfo=None)
            csvdata2 += csvdata.join("\n".join(["%s"",""%s" % (timestamp_dt2.isoformat(' '), value)]) + "\n")
        
    #Convert the datastring into a base64 object
    csvbytes = base64.b64encode(csvdata2)
    
    return csvbytes


#Get data for all series that are available
loopnum = 1
for AQTimeSeriesID, TableName, TableColumnName in AqSeries:
    print "Attempting to append series %s of %s" % (loopnum, len(AqSeries))
    appendbytes = get_data_from_table(TableName,TableColumnName)
    #Actually append to the Aquarius dataset
    if len(appendbytes) > 0 :
        try:
            AppendResult = client.service.AppendTimeSeriesFromBytes2(
                long(AQTimeSeriesID),appendbytes,aq_username)
        except:
            error_in_append = sys.exc_info()[0]
            print "Error: %s" % error_in_append
            if Log_to_file:
                text_file.write("%s, %s, %s, ERROR!, 0, %s  \n" % (loopnum, TableName, TableColumnName, error_in_append))
        else:
            print AppendResult
            if Log_to_file:
                #text_file.write("%s \n" % AppendResult)
                text_file.write("%s, %s, %s, %s, %s, %s \n" % (loopnum, TableName, TableColumnName, AppendResult.TsIdentifier, AppendResult.NumPointsAppended, AppendResult.AppendToken))
    else:
        print "No data appended from this query."
        if Log_to_file:
            #text_file.write("No data appended from this query. \n")
            text_file.write("%s, %s, %s, NoAppend, 0, NoAppend  \n" % (loopnum, TableName, TableColumnName))
    loopnum += 1
        
#Close out the text file
if Log_to_file:
    # Find the date/time the script was started:
    end_datetime = datetime.datetime.now()
    runtime = end_datetime - start_datetime
    text_file.write("\n")
    text_file.write("Script completed at %s \n" % end_datetime)
    text_file.write("Total time for script: %s \n" % runtime)
    text_file.write("========================================================================================= \n")
    text_file.write("\n \n")
    text_file.write("========================================================================================= \n")
    text_file.close()

# -*- coding: utf-8 -*-
"""
Created on Mon Dec 10 10:46:42 2012

@author: Justin Olexy

This script will take all incoming data on a serial port at 'addr'
and append it to a file, 'fname'.
"""

#Import necessary modules
import serial #This is the pySerial module:  http://pyserial.sourceforge.net/
import sys
from time import sleep
from collections import deque

#Set up the network addresses for pySerial
#NOTE:  The folders MUST already exist!  This program will not create them, just add files!
addr  = 'COM4'
baud  = 57600
remote_file_path = 'H:\\midStream\\data_longformat_test\\'
local_file_path = 'C:\\Users\\sdamiano.STROUDWATER\\Desktop\\Local_midStream\\data\\'
invalid_filename = 'invalid_strings.csv'
fmode = 'ab'

spinning_chars = '|/-\\'

x = ""

remote_pass_que = deque()
remote_fail_que = deque()

def check_checksum(in_string):
    '''Calculates a check-sum value and compares it to the checksum generated
    on-board by the Arduino logger.'''
    
    if not in_string:
        return False
    
    string_list = in_string.split(',')

    checksum = 0
    commas_to_checksum = 0
    comma_count = 0

    for char in in_string:
        #Find the number of commas before the last field (assumed to be the checksum)
        if char == ',':
            commas_to_checksum += 1

    for char in in_string:
        #Calculate the checksum by adding together all characters but the last field.
        if char == ',':
            comma_count += 1
        if comma_count == commas_to_checksum:
            break
        checksum += ord(char)

    #looking for obvious errors.
    if len(string_list) < 6:
        #The string must have at least 6 fields:  Date, time, UTC Offset, LoggerID, Value, CheckSum
        #Any string missing one of these 6 pieces is obviously wrong.
        print 'string_list too short'
        return False  
    string_list[commas_to_checksum] = string_list[commas_to_checksum].strip()
    if not string_list[commas_to_checksum].isdigit():
        #Makse sure that the last field is numeric.
        print 'checksum value is not numeric'
        return False
        
    checksum_in = int(string_list[commas_to_checksum])

    if checksum_in == checksum:
        print 'CHECKSUM PASS'
        return True
    else:
        print 'CHECKSUM FAIL'
        return False

# Writes a line to a file
# returns true if success
def write_line_to_file(line, filename):
    try:
        with open(filename,fmode) as outf:
            try:            
                outf.write(line)
                outf.flush()
            except:
                print 'ERROR: File Write Failed: ' + filename
                print '  Check server connection'
                return False
    except IOError: # catch an io error
        for c in spinning_chars:
            sys.stdout.write(c)
            sys.stdout.write('\b')
            sys.stdout.flush()
            sleep(0.1)
        return False

    return True
 
def write_que_to_file():
    if len(remote_pass_que):
        x = remote_pass_que[0]
        year = x[0:4]
        month = x[5:7].replace('/','')
        
        if year.isdigit() and month.isdigit():
            filename = remote_file_path + year + '-' + month + '.csv'               
        else:
            filename = remote_file_path + invalid_filename
            print 'Invalid Time, Storing in: ' + filename

        if (write_line_to_file(x,filename)):
            remote_pass_que.popleft()

    if len(remote_fail_que):
        x = remote_fail_que[0]
        filename = remote_file_path + invalid_filename
        if (write_line_to_file(x,filename)):
            remote_fail_que.popleft()
       
# Display initialization settings
print 'Writing ZBee data to file'
print addr
print 'Baud: ' + repr(baud)
print 'Local File Path: ' + local_file_path
print 'Remote File Path: ' + remote_file_path
if fmode == 'ab':
    print 'File Mode: Append, Binary'
                
with serial.Serial(addr,baud,timeout=5) as port:
    while True:
        x = port.readline()
        
        if not x: # if nothing was read, 
            # this will attempt to write the server que to the remote files
            write_que_to_file()
            continue # go back to the top of the while loop
        
        print x[0:-1]
        if check_checksum(x):
            remote_pass_que.append(x) # append to pass que
            year = x[0:4]            
            month = x[5:7].replace('/','')
            
            if year.isdigit() and month.isdigit():
                filename = local_file_path + year + '-' + month + '.csv'               
            else:
                filename = local_file_path + invalid_filename
                print 'Invalid Time, Storing in: ' + filename
            write_line_to_file(x,filename) # store to local file

        else: # failed checksum
            # store line in a junk file
            remote_fail_que.append(x) # append to fail que
            filename = local_file_path + invalid_filename
            write_line_to_file(x,filename) # store to local file

        # this will attempt to write the server que to the remote files
        write_que_to_file()

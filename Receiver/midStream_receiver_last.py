# -*- coding: utf-8 -*-
"""
Created on Mon Dec 10 10:46:42 2012

@author: Justin Olexy

This script will take all incoming data on a serial port at 'addr'
and append it to a file, 'fname'.
"""

import serial
import sys
from time import sleep

addr  = 'COM8'
baud  = 57600
file_path = 'm:\\midStream\\data\\'
fmode = 'ab'

spinning_chars = '|/-\\'

file_success = True
x = ""

def check_checksum(in_string):
    if not in_string:
        return False
    
    string_list = in_string.split(',')

    if len(string_list) < 6:
        print 'string_list too short'
        return False
    if not string_list[5]:
        print 'no checksum value'
        return False
    string_list[5] = string_list[5].strip()
    if not string_list[5].isdigit():
        print 'checksum value is not numeric'
        return False
        
    checksum_in = int(string_list[5])

    checksum = 0
    comma_count = 0

    for char in in_string:
        if char == ',':
            comma_count += 1
        if comma_count == 5:
            break
        checksum += ord(char)

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
                print 'STATUS: Attempting to reconnect ',
                return False
    except IOError: # catch an io error
        for c in spinning_chars:
            sys.stdout.write(c)
            sys.stdout.write('\b')
            sys.stdout.flush()
            sleep(0.1)
        return False

    return True
        
# Display initialization settings
print 'Writing ZBee data to file'
print addr
print 'Baud: ' + repr(baud)
print 'File Path: ' + file_path
if fmode == 'ab':
    print 'File Mode: Append, Binary'
                
with serial.Serial(addr,baud,timeout=5) as port:
    while True:
        x = port.readline()
        if x:
            print x[0:-1]
        if x and check_checksum(x):
            year = x[0:4]
            month = x[5:7].replace('/','')

            filename = file_path + year + '-' + month + '.csv'               
            
            try:
                int(year)
                int(month)
            except:
                print 'Invalid Time stamp'
                filename = file_path + 'invalid_time_stamps.csv'
                print 'Storing in: ' + filename

            # if writing the file does not succeed then keep trying
            while not write_line_to_file(x,filename):
                write_line_to_file(x,filename)
                
# -*- coding: utf-8 -*-
"""
loggerNetSim

A python program to simulate a live data logger network.

DateTime,UTCOffset,LoggerID,Value1,Value2,...ValueN

The logger values will be appended to a csv file

Created on Wed Nov 28 09:38:43 2012

@author: Justin Olexy
"""

import time
import random

LOGFILE_NAME = 'm:\midStream\CZO_disp.csv'   # output filename

LOGGERS = 3                     # Number of loggers to simulate.  Not used
LOGGER_INTERVAL = [2, 5, 30]    # A list of logger reporting intervals in seconds
MAX_LINES = 100                  # Max number of logger lines to produce
TIME_FORMAT = '%m/%d/%y %H:%M:%S' #used by strftime to generate a time string

with open(LOGFILE_NAME, 'w') as f:
    line = 1
    while (line < MAX_LINES):           # this wile statement will run about once each second
        currentSec = time.gmtime().tm_sec
        if  (currentSec % LOGGER_INTERVAL[0]) == 0:
            s = time.strftime(TIME_FORMAT) + ',-5,0001,' + str(random.randrange(1,1000)/100.00) + '\n'
            f.write(s)
            f.flush()   # Forces the new line to be written to the file
            print(str(line)+ ': '+ s[:-1])
            line = line + 1
        if (currentSec % LOGGER_INTERVAL[1]) == 0:
            s = time.strftime(TIME_FORMAT) + ',-5,0002,' + str(random.randrange(1,10)) + ',' + str(random.randrange(300,400)) + '\n'
            f.write(s)
            f.flush()   # Forces the new line to be written to the file
            print(str(line) + ': ' + s[:-1])
            line = line + 1
        if (currentSec % LOGGER_INTERVAL[2]) == 0:
            s = time.strftime(TIME_FORMAT) + ',-5,0003,' + str(random.randrange(1,1000)/100.00) + ',' + \
            str(random.randrange(1,1000)/100.00) + ',' + str(random.randrange(1,1000)/100.00) + '\n'
            f.write(s)
            f.flush()   # Forces the new line to be written to the file
            print(str(line) + ': ' + s[:-1])
            line = line + 1
        time.sleep(1)

f.close()
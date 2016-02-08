#import soaplib
import logging

import wof

import os
os.chdir('D:/StroudData/midStream')

#from czo_dao import czoDao # if metadata is contained in CZO display files
#from czo_pgdao import czoDao # if metadata is contained in cropped database
from czo_pgdao_fulldb import czoDao # if metadata is contained in full database

CSV_CONFIG_FILE = 'data/CZO_RT_config.cfg'
VALUES_FILE = 'data/loggers.csv'
#SITES_FILE = 'data/CZO_RT_sites.csv' # if metadata is contained in CZO display files
#METHODS_FILE = 'data/CZO_RT_methods.csv' # if metadata is contained in CZO display files
#HEADER_FILE = 'data/CZO_RT_header.csv' # if metadata is contained in CZO display files

logging.basicConfig(level=logging.DEBUG)

#dao = czoDao(SITES_FILE, METHODS_FILE, VALUES_FILE, HEADER_FILE) # if metadata is contained in CZO display files
dao = czoDao(VALUES_FILE) # if metadata is in database

app = wof.create_wof_app(dao, CSV_CONFIG_FILE)
app.config['DEBUG'] = True

if __name__ == '__main__':
    # This must be an available port on your computer.  
    # For example, if 8080 is already being used, try another port such as
    # 5000 or 8081.
    openPort = 8080 

    url = "http://127.0.0.1:" + str(openPort) + "/"

    print "----------------------------------------------------------------"
    print "Access 'REST' endpoints at " + url
    print "Access SOAP WSDL at " + url + "soap/wateroneflow.wsdl"
    print "Access SOAP endpoints at " + url + "soap/wateroneflow"
    print "----------------------------------------------------------------"

    app.run(host='0.0.0.0', port=openPort, threaded=True)

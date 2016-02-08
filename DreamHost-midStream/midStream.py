#import soaplib
import logging

import wof

import os
os.chdir('D:/StroudData/midStream')

from DAO_DreamHost_simple import czoDao # if metadata is contained in database

CSV_CONFIG_FILE = 'CZO_RT_config.cfg'

logging.basicConfig(level=logging.DEBUG)

dao = czoDao()

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

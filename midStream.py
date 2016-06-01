# import soaplib
import logging
import wof.flask
import os
os.chdir('D:/StroudData/midStream')

from DAO_DreamHost_simple import czoDao   # if metadata is contained in database

dao = czoDao()
config = 'CZO_RT_config.cfg'

logging.basicConfig(level=logging.DEBUG)

app = wof.flask.create_wof_flask_app(dao, config)
app.config['DEBUG'] = True

if __name__ == '__main__':
    # This must be an available port on your computer.  
    # For example, if 8080 is already being used, try another port such as
    # 5000 or 8081.
    openPort = 8080 

    url = "http://127.0.0.1:" + str(openPort)
    print "----------------------------------------------------------------"
    print "Service endpoints"
    for path in wof.flask.site_map_flask_wsgi_mount(app):
        print "%s%s" % (url, path)

    print "----------------------------------------------------------------"
    print "----------------------------------------------------------------"
    print "HTML Acess Service endpoints at "
    for path in wof.site_map(app):
        print "%s%s" % (url, path)

    print "----------------------------------------------------------------"

    app.run(host='0.0.0.0', port=openPort, threaded=True)

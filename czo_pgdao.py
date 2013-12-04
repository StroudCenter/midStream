# This is the data access object to be used with a PostgreSQL implementation
# of the logger database

from datetime import timedelta
import csv

from dateutil.parser import parse
from dateutil.tz import tzoffset as tz

from wof.dao import BaseDao
import wof.models as wof_base

import copy
import czo_model
import time

# for PostgreSQL access
# should create a  user for midStream with read-only access
import psycopg2 as pg
DB_CONNECT_STR = "host='192.168.8.100' user='postgres' dbname='midStream'"

# if DEBUG_PRINT is True, lots of information will be printed to the console
# which slows retrieval of data series significantly
DEBUG_PRINT = False

class czoDao(BaseDao):
    def __init__(self, values_file_path):
        if DEBUG_PRINT:
            print '*CsvDao __init__'     
        self.values_file_path = values_file_path

    def __del__(self):
        pass # Could end database session here for more sophisticated DAOs

    def create_site_from_row(self, row):
        """Returns a Site() object

        Required Arguments:
            A list of site information in this order
            SiteID, SiteCode, SiteName, Latitude, Longitude, Elevation, SRSID
        """
        if DEBUG_PRINT:
            print '*create_site_from_row: ' + repr(row) 

        if not row:
            return

        if not (len(row) > 6):
            return

        site = czo_model.Site() # create a new instance of site
        site.SiteID = row[0]    # and populate with site data
        site.SiteCode = row[1]
        site.SiteName = row[2]
        site.Latitude = row[3]
        site.Longitude = row[4]
        site.Elevation_m = row[5]

        # spatial reference 
        site_spatialref = wof_base.BaseSpatialReference()
        site_spatialref.SRSID = row[6]
        site.LatLongDatum = site_spatialref

        return copy.deepcopy(site)
 
    def get_all_sites(self):
        """Returns all sites as a list of Site() objects

        """
        
        if DEBUG_PRINT:
            print '*get_all_sites'        

        # Open a connection to the database
        conn = pg.connect(DB_CONNECT_STR)
        cur = conn.cursor()
        # read entire midStreamSensor table from database

        # TODO: get sites using a DB query like this:
        # this will find all the sites that have real-time deployments
        # returns SiteID, SiteCode, SiteName, Latitude, Longitude, Elevation_m, spatialref SRSID
        #cur.execute("""
        #SELECT 
        #  sites.siteid,
        #  sites.sitenamecode,
        #  sites.sitefullname,
        #  sites.latitude,
        #  sites.longitude,
        #  sites.elevation,
        #  sites.latlongdatumid
        #FROM 
        #  public.deployments,
        #  public.sites,
        #  public.dataseries
        #WHERE 
        #  deployments.deploymentid = dataseries.deploymentid AND
        #  dataseries.siteid = sites.siteid AND
        #  deployments.isrealtime = True
        #GROUP BY
        #  sites.siteid;""")

        cur.execute("""
        SELECT 
            pk_sensor_id,
            site_code,
            site_name,
            latitude,
            longitude,
            elevation_m, 
            site_spatialref
        FROM
            midstream_sensor;""")
        table = cur.fetchall()

        cur.close()     # close the database cursor
        conn.close()    # close the database connection

        if not table:
            return []

        sites = []

        # iterate over the rows in the database
        for row in table:
            site = self.create_site_from_row(row)
            sites.append(site)
            if DEBUG_PRINT:
                print site

        return sites

    def get_site_by_code(self, site_code):
        """Returns a Site() object

        Required Arguments:
            A site_code ascii string
        """
        if DEBUG_PRINT:
            print '*get_site_by_code: ' + site_code

        if not site_code:
            return

        # Open a connection to the database
        conn = pg.connect(DB_CONNECT_STR)
        cur = conn.cursor()

        # TODO: get site using a DB query like this:
        # this will find the site by site_code
        # returns SiteID, SiteCode, SiteName, Latitude, Longitude, Elevation_m, spatialref SRSID
        #cur.execute("""
        #SELECT 
        #  sites.siteid,
        #  sites.sitenamecode,
        #  sites.sitefullname,
        #  sites.latitude,
        #  sites.longitude,
        #  sites.elevation,
        #  sites.latlongdatumid
        #FROM 
        #  public.sites
        #WHERE 
        #  sites.sitenamecode = %s;""", (site_code,))

        # read row from the midStreamSensor table where the site_code matches
        cur.execute("""
        SELECT
            pk_sensor_id,
            site_code,
            site_name,
            latitude,
            longitude,
            elevation_m, 
            site_spatialref
        FROM
            midstream_sensor
        WHERE
            site_code = %s;""", (site_code,))
        row = cur.fetchone()
        cur.close()     # close the database cursor
        conn.close()    # close the database connection

        site = self.create_site_from_row(row)
        return site

    def get_sites_by_codes(self, site_codes_arr):
        """Returns a list of Site() objects

        Required Arguments:
            A list of site codes
        """
        if DEBUG_PRINT:
            print '*get_sites_by_codes'        

        if not site_codes_arr:
            return []

        # Open a connection to the database
        conn = pg.connect(DB_CONNECT_STR)
        cur = conn.cursor()

        sites = []

        for site_code in site_codes_arr:
            # TODO: get site using a DB query like this:
            # this will find the site by site_code
            # returns SiteID, SiteCode, SiteName, Latitude, Longitude, Elevation_m, spatialref SRSID
            #cur.execute("""
            #SELECT 
            #  sites.siteid,
            #  sites.sitenamecode,
            #  sites.sitefullname,
            #  sites.latitude,
            #  sites.longitude,
            #  sites.elevation,
            #  sites.latlongdatumid
            #FROM 
            #  public.sites
            #WHERE 
            #  sites.sitenamecode = %s;""", (site_code,))

            cur.execute("""
            SELECT 
                pk_sensor_id, 
                site_code, 
                site_name, 
                latitude, 
                longitude, 
                elevation_m, 
                site_spatialref 
            FROM
                midstream_sensor 
            WHERE
                site_code = %s;""", (site_code,))
            row = cur.fetchone()
            if row:
                site = self.create_site_from_row(row)
                sites.append(site)

        cur.close()     # close the database cursor
        conn.close()    # close the database connection
        
        return sites

    def get_all_variables(self):
        """Returns a list of all Variable() objects

        """
        if DEBUG_PRINT:
            print '*get_all_variables'

        variables = []
        
        # Open a connection to the database
        conn = pg.connect(DB_CONNECT_STR)
        cur = conn.cursor()
        
        # TODO: get variables using a DB query like this:
        # this will return all the variables that 
        # are associated with real-time deployments
        #cur.execute("""
        #SELECT 
        #  variables.variableid, 
        #  variables.variablename
        #FROM 
        #  public.dataseries, 
        #  public.variables, 
        #  public.units, 
        #  public.deployments
        #WHERE 
        #  dataseries.variableid = variables.variableid AND
        #  dataseries.variableunitsid = units.unitid AND
        #  dataseries.deploymentid = deployments.deploymentid AND
        #  deployments.isrealtime = True
        #GROUP BY
        #  variables.variableid;""")        
        
        # read row from the midStreamVariables table
        cur.execute("""
        SELECT
            pk_variable_id,
            variable_code, 
            variable_name,
            fk_units_id,
            fk_method_id,
            time_interval
        FROM 
            midstream_variables;""")
        table = cur.fetchall()

        if not table:
            cur.close()     # close the database cursor
            conn.close()    # close the database connection        
            return []

        for row in table:
            var1 = czo_model.Variable()

            var1.VariableID = row[0]
            var1.VariableCode = row[1]
            var1.VariableName = row[2]
            var1.VariableUnitsID = row[3]
            #var1.SampleMedium = None
            #var1.ValueType = None
            #var1.IsRegular = None
            var1.TimeSupport = row[5]
            #var1.TimeUnitsID = None
            #var1.DataType = None
            #var1.GeneralCategory = None
            #var1.NoDataValue = None
            #var1.VariableDescription = None
            var1.MethodID = row[4] # MethodID wasadded to Variable model

            var1_units = wof_base.BaseUnits() # create a sigle units instance
            var1_units.UnitsID = row[3]
            cur.execute("""
            SELECT
                unitname,
                unittype,
                unitabbreviation
            FROM
                units
            WHERE
                unitid = %s;""", (var1_units.UnitsID,))        
            db_unit = cur.fetchone()
            var1_units.UnitsName = db_unit[0]
            var1_units.UnitsType = db_unit[1]
            var1_units.UnitsAbbreviation = db_unit[2]
            var1.VariableUnits = var1_units # assign the variable units to the units instance

            variables.append(copy.deepcopy(var1))

        cur.close()     # close the database cursor
        conn.close()    # close the database connection        

        return variables

    def get_variable_by_code(self, var_code):
        """Returns a Variable() object

        Required Arguments:
            A varable code ascii string
        """        
        
        if DEBUG_PRINT:
            print '*get_variable_by_code: ' + var_code

        # Open a connection to the database
        conn = pg.connect(DB_CONNECT_STR)
        cur = conn.cursor()

        # TODO: get variable using a DB query like this:
        # this will return the variable info
        # that matches var_code
        #cur.execute("""
        #SELECT 
        #  variables.variableid, 
        #  variables.variablename
        #FROM 
        #  public.dataseries, 
        #  public.variables, 
        #  public.units, 
        #  public.deployments
        #WHERE 
        #  dataseries.variableid = variables.variableid AND
        #  dataseries.variableunitsid = units.unitid AND
        #  dataseries.deploymentid = deployments.deploymentid AND
        #  deployments.isrealtime = True AND
        #  variables.variablecode = %s
        #GROUP BY
        #  variables.variableid;""", (var_code,))    

        # read row from the midStreamVariables table
        cur.execute("""
        SELECT
            pk_variable_id,
            variable_code,
            variable_name,
            fk_units_id,
            fk_method_id,
            time_interval
        FROM
            midstream_variables
        WHERE
            variable_code = %s;""", (var_code,))
        row = cur.fetchone()

        if not row:
            cur.close()     # close the database cursor
            conn.close()    # close the database connection
            return
            
        var1 = czo_model.Variable()

        var1.VariableID = row[0]
        var1.VariableCode = row[1]
        var1.VariableName = row[2]
        var1.VariableUnitsID = row[3]

        # These commented properties have defaults in czo_model.py
        # Using defaults until they can be extracted from the logger database
        # TODO: Replace None with values read from database table
        #var1.SampleMedium = None
        #var1.ValueType = None
        #var1.IsRegular = None
        var1.TimeSupport = row[5]
        #var1.TimeUnitsID = None
        #var1.DataType = None
        #var1.GeneralCategory = None
        #var1.NoDataValue = None
        #var1.VariableDescription = None
        var1.MethodID = row[4] # MethodID wasadded to Variable model

        var1_units = wof_base.BaseUnits() # createa sigle units instance
        var1_units.UnitsID = row[3]
        cur.execute("""
        SELECT 
            unitname,
            unittype,
            unitabbreviation
        FROM
            units
        WHERE 
            unitid = %s;""", (var1_units.UnitsID,))        
        db_unit = cur.fetchone()
        var1_units.UnitsName = db_unit[0]
        var1_units.UnitsType = db_unit[1]
        var1_units.UnitsAbbreviation = db_unit[2]
        var1.VariableUnits = var1_units # assign the variable units to the units instance

        cur.close()     # close the database cursor
        conn.close()    # close the database connection

        return var1            

    def get_variables_by_codes(self, var_codes_arr):
        """Returns a list of Variable() objects

        Required Arguments:
            A list of ascii variable codes
        """

        if DEBUG_PRINT:
            print '*get_variables_by_code: ' + var_codes_arr[0]        

        if not var_codes_arr:
            return []
        
        vars = []
        for var_code in var_codes_arr:
            var1 = self.get_variable_by_code(var_code)
            vars.append(copy.deepcopy(var1))
        return vars

    def get_series_by_sitecode(self, site_code):
        """Returns a list of Series() objects

        Required Arguments:
            A list of ascii site codes
        """
        if DEBUG_PRINT:
            print '*get_series_by_sitecode: ' + site_code

        if not site_code:
            return []
        
        series_list = []
        site = self.get_site_by_code(site_code)
        
        if not site:
            return []

        # open database, read variablecode for this site
        conn = pg.connect(DB_CONNECT_STR)
        cur = conn.cursor()    
        cur.execute("""
        SELECT 
          midstream_variables.variable_code
        FROM 
          public.midstream_variables, 
          public.midstream_sensor
        WHERE 
          midstream_sensor.fk_variable_id = midstream_variables.pk_variable_id AND
          midstream_sensor.site_code = %s;""", (site_code,))

        row = cur.fetchone()
        cur.close()     # close the database cursor
        conn.close()    # close the database connection
        
        if not row:
            return []
        
        var_code = row[0]
        series = self.get_series_by_sitecode_and_varcode(site_code, var_code)
        series_list.extend(series)

        return series_list

    def get_series_by_sitecode_and_varcode(self, site_code, var_code):
        """Returns a list of Series() objects

        Required Arguments:
            An ascii site code and ascii variable code
        """
        if DEBUG_PRINT:
            print '*get_series_by_sitecode_and_varcode: ' + site_code + ' - ' + var_code
        
        if not (site_code and var_code):
            return []
        
        series_list = []
        site = self.get_site_by_code(site_code)
        if site:
            var = self.get_variable_by_code(var_code)
            if var:
                series = czo_model.Series()
                method_list = self.get_methods_by_ids([var.MethodID])
                
                # Data is updated in real-time, so the end time is read as current time
                series.BeginDateTime = '2013-01-01T00:00:00' #TODO: Perhaps get begin time from the database (had -5)
                series.EndDateTime = time.strftime('%Y-%m-%dT%H:%M:%S')# + '-05:00'
                series.BeginDateTimeUTC = '2013-01-01T05:00:00Z' #TODO: Perhaps get begin time from the database
                series.EndDateTimeUTC = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime()) + 'Z'

                series.Site = site
                series.Variable = var
                if method_list:
                    series.Method = method_list[0]
                series_list.append(series)
        return series_list

    def parse_date_strings(self, begin_date_time_string, end_date_time_string):
        """Returns a list with parsed datetimes in the local time zone.

        Required Arguments:
            begin_date_time (begin datetime as text)
            end_date_time (end datetime as text)
        Remarks:
            The returned list has two items:
                begin datetime as datetime.datetime object
                end datetime as datetime.datetime object
        """

        if DEBUG_PRINT:
            print '*parse_date_strings: '# + begin_date_time_string + ' ' + end_date_time_string
        
        # Convert input strings to datetime objects
        try:
            if begin_date_time_string:
                b = parse(begin_date_time_string)
            else:  # if there is no start date specified
                # Provide default start date at beginning of period of record
                b = parse('1900-01-01T00:00-05')
        except:
            raise ValueError('invalid start date: ' + str(begin_date_time_string))
        try:
            if end_date_time_string:
                e = parse(end_date_time_string)
            else:
                # Provide default end date at end of period of record
                e = parse(time.strftime('%Y%m%d %H:%M:%S') + '-05') # use current tme if none specified
        except:
            raise ValueError('invalid end date: ' + str(end_date_time_string))

        # If we know time zone, convert to local time.  Otherwise, assume local time.
        # Remove tzinfo in the end since datetimes from data file do not have
        # tzinfo either.  This enables date comparisons.
        local_time_zone = tz(None, -18000) # Five hours behind UTC, in seconds
        if b.tzinfo:
            b = b.astimezone(local_time_zone)
            b = b.replace(tzinfo=None)
        if e.tzinfo:
            e = e.astimezone(local_time_zone)
            e = e.replace(tzinfo=None)
        
        if DEBUG_PRINT:
            print b
            print e

        return [b, e]

    def create_datavalue_from_row(self, row, value_index):
        """Returns a Datavalue() object

        Required Arguments:
            A row of the logger file as a list
            A column number for the data value
        """
        if DEBUG_PRINT:
            print '*create_datavalue_from_row: ' + repr(row) + ' - ' + repr(value_index)        

        # Catch possible index out of range error
        if not (len(row) > value_index):        
            return
        
        datavalue = czo_model.DataValue()

        datavalue.DataValue = row[value_index].strip() # the actual data value is stored

        try:        # catch a mal-formed time stamp.. this shouldn't happen, but just in case
            value_date = parse(row[0])
        except:
            return None
        # add the UTC offset to the time stamp
        datavalue.LocalDateTime = value_date.isoformat() + row[1].strip()

        # store the UTC time by adding the UTC offset.  This assumes site is west of GMT
        value_date = value_date + timedelta(abs(int(row[1].strip()))) 
        datavalue.DateTimeUTC = value_date.isoformat() + 'Z'

        return datavalue

    def get_datavalues(self, site_code, var_code, begin_date_time=None,
                       end_date_time=None):
        """Returns a list of DataValue() objects
            The data values are extracted directly from the combined
            logger file

        Required Arguments:
            Site Code
            Variable Code
        Optional Arguments:
            start date_time
            end date_time
        """
        if DEBUG_PRINT:
            print '*get_datavalues: ' + site_code + ' - ' + var_code

        # check with the database whether this site actually is tracking
        # this variable
        conn = pg.connect(DB_CONNECT_STR)
        cur = conn.cursor()        
        cur.execute("""         
            SELECT 
              midstream_variables.variable_code
            FROM 
              public.midstream_sensor, 
              public.midstream_variables
            WHERE 
              midstream_sensor.fk_variable_id = midstream_variables.pk_variable_id AND
              midstream_sensor.site_code = %s;""", (site_code,))
        table = cur.fetchall()
        cur.close()
        conn.close()        

        site_var_match = False
        for row in table:
            if row[0] == var_code:
                site_var_match = True
        
        if site_var_match == False:
            return []
        
        # Find the site and variable
        siteResult = self.get_site_by_code(site_code)
        varResult = self.get_variable_by_code(var_code)
        valueResultArr = []

        # if failed to find site or variable
        if not (siteResult and varResult):
            return []

        if DEBUG_PRINT:
            print '- ' + varResult.VariableCode
            print '- ' + varResult.VariableName
            
        # TODO: open the database and find out, given site_code and var_code,
        # which column has the data value
        value_index = 4  # TODO: Get csv index from database

        # Parse input dates
        parse_result = self.parse_date_strings(begin_date_time, end_date_time)
        b = parse_result[0] # begin datetim
        e = parse_result[1] # end datetime

        #t1 = time.time()
        
        # grab the current time so we know which logger file to look in
        current_time = time.localtime()
        current_year = current_time[0]
        current_month = current_time[1]        
        
        data_file_list = [] # this list keeps track of all the data files we shoud scan        
        
        # add the current month to the data_file_list
        current_month_file = 'data/' + str(current_year) + '-' + str(current_month) + '.csv'

        current_month = current_month - 1        
        if current_month == 0:
            current_month = 12
            current_year = current_year - 1
        
        previous_month_file = 'data/' + str(current_year) + '-' + str(current_month) + '.csv'

        # create a list of possible filenames to look for data
        # if any of the files do not exist, that's not a problem, they will be skipped
        data_file_list.append('data/historic.csv') # first a historic data file
        data_file_list.append(previous_month_file) # next, the previous month
        data_file_list.append(current_month_file) # finally, the current month

        if DEBUG_PRINT:
            print 'scanning datafiles: '
            print data_file_list        
        
        # Read values from values file           
        for data_file in data_file_list:        
            try:
                with open(data_file, 'rb') as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if len(row) > 5: # error check to be sure row has more than 5 elements
                            if row[2].strip() + row[3].strip() == site_code: # first check if this row is for the requested site
                                try: # catch a bad time string
                                    value_date = parse(row[0]) # read the data value timedate stamp
                                except: # if a bad time string, continue on to the next row
                                    continue 
                                if DEBUG_PRINT:
                                    print value_date
                                # Check that datetime stamp is within input date range
                                if (value_date >= b and value_date <= e):
                                    # Add data value to result list
                                    datavalue = self.create_datavalue_from_row(row, value_index)
        
                                    datavalue.MethodID = varResult.MethodID # MethodID was not originally part of datavalue object
                                    if datavalue.DataValue != varResult.NoDataValue:       # only return real data                     
                                        valueResultArr.append(datavalue)
            except:
                pass
                if DEBUG_PRINT:                
                    print 'filename not found: ' + data_file
        
        #t2 = time.time()
        #print 'get_datavalues Exec Time: %0.3f s' % (t2-t1)
            
        if DEBUG_PRINT:
            print valueResultArr
            
        return valueResultArr

    def get_methods_by_ids(self, method_id_arr):
        """Returns a list of Method() objects

        Required Arguments:
            A list of method codes (integers)
        """
        if DEBUG_PRINT:
            print '****get_methods_by_ids: '
            print method_id_arr        

        if not method_id_arr:
            return []

        methods = [] 

        # Open a connection to the database
        conn = pg.connect(DB_CONNECT_STR)
        cur = conn.cursor()    

        for method_id in method_id_arr:
            cur.execute("""
            SELECT
                methodid,
                term,
                definition
            FROM
                methods
            WHERE
                methodid = %s;""", (method_id,))
            row = cur.fetchone()
            if row:
                method = czo_model.Method()
                method.MethodID = method_id
                method.MethodDescription = row[2]
                methods.append(method) # TODO: Does this need to be copy.deepcopy(method)
    
        cur.close()     # close the database cursor
        conn.close()    # close the database connection

        return methods

    def get_sources_by_ids(self, source_id_arr):
        """Returns a list of Source() objects

        Required Arguments:
            A list of integer source IDs
        """
        if DEBUG_PRINT:
            print '*get_sources_by_ids: ' + repr(source_id_arr)        

        if not source_id_arr:
            return []

        sources = []

        # Open a connection to the database
        conn = pg.connect(DB_CONNECT_STR)
        cur = conn.cursor()    

        # iterate through the source_ids and find them in the database
        for source_id in source_id_arr:
            cur.execute("""
            SELECT 
                contactname,
                organization,
                address,
                city,
                state,
                zipcode,
                phone,
                email 
            FROM
                people 
            WHERE
                personid = %s;""", (source_id,))
            row = cur.fetchone()
            if row:
                source = czo_model.Source()
                source.SourceID = source_id
                source.ContactName = row[0]
                source.Phone = row[6]
                source.Email = row[7]
                source.Organization = row[1]
                source.SourceLink = 'http://www.stroudcenter.org/'
                source.SourceDescription = 'Data collected by Stroud Water Research Center'
                source.Address = row[2]
                source.City = row[3]
                source.State = row[4]
                source.ZipCode = row[5]
                sources.append(source) # append the source to list of sources

        cur.close()     # close the database cursor
        conn.close()    # close the database connection

        return sources

    def get_qualifiers_by_ids(self, qualifier_id_arr):
        if DEBUG_PRINT:
            print '*get_qualifiers_by_ids'        
        return []

    def get_offsettypes_by_ids(self, offset_type_id_arr):
        if DEBUG_PRINT:
            print '*get_offsettypes_by_ids'
        return []
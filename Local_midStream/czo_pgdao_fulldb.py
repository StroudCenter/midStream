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
        """Returns all sites with a current real-time deployment as a
        list of Site() objects

        """
        
        if DEBUG_PRINT:
            print '*get_all_sites'        

        # Open a connection to the database
        conn = pg.connect(DB_CONNECT_STR)
        cur = conn.cursor()
        
        # this will find all the sites that have CURRENT real-time deployments
        #TODO:  Include real-time deployments that have ended within 30 days.
        # returns SiteID, SiteCode, SiteName, Latitude, Longitude, Elevation_m, spatialref SRSID
        cur.execute("""
        SELECT DISTINCT
        	Sites.SiteID,
        	Sites.SiteNameCode,
        	Sites.SiteFullName,
        	Sites.Latitude,
        	Sites.Longitude,
        	Sites.Elevation,
        	SpatialReferences.SRSID
        FROM SpatialReferences
        	RIGHT JOIN (Deployments
        		RIGHT JOIN (Sites
        			RIGHT JOIN DataSeries
        			ON Sites.SiteID = DataSeries.SiteID)
        		ON Deployments.DeploymentID = DataSeries.DeploymentID)
        	ON SpatialReferences.SpatialReferenceID = Sites.SpatialReferenceID
        WHERE (((Deployments.IsRealTime)=True) AND 
            (Deployments.DeploymentEndDateTime is NULL));
        """)
            
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

        # this will find the site by site_code
        # returns SiteID, SiteCode, SiteName, Latitude, Longitude, Elevation_m, spatialref SRSID
        cur.execute("""
        SELECT DISTINCT
        	Sites.SiteID,
        	Sites.SiteNameCode,
        	Sites.SiteFullName,
        	Sites.Latitude,
        	Sites.Longitude,
        	Sites.Elevation,
        	SpatialReferences.SRSID
        FROM SpatialReferences
        	RIGHT JOIN (Deployments
        		RIGHT JOIN (Sites
        			RIGHT JOIN DataSeries
        			ON Sites.SiteID = DataSeries.SiteID)
        		ON Deployments.DeploymentID = DataSeries.DeploymentID)
        	ON SpatialReferences.SpatialReferenceID = Sites.SpatialReferenceID
        WHERE 
          sites.sitenamecode = %s;""", (site_code,))

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
            # this will find the site by site_code
            # returns SiteID, SiteCode, SiteName, Latitude, Longitude, Elevation_m, spatialref SRSID
            cur.execute("""
            SELECT DISTINCT
            	Sites.SiteID,
            	Sites.SiteNameCode,
            	Sites.SiteFullName,
            	Sites.Latitude,
            	Sites.Longitude,
            	Sites.Elevation,
            	SpatialReferences.SRSID
            FROM SpatialReferences
            	RIGHT JOIN (Deployments
            		RIGHT JOIN (Sites
            			RIGHT JOIN DataSeries
            			ON Sites.SiteID = DataSeries.SiteID)
            		ON Deployments.DeploymentID = DataSeries.DeploymentID)
            	ON SpatialReferences.SpatialReferenceID = Sites.SpatialReferenceID
            WHERE 
              sites.sitenamecode = %s;""", (site_code,))

            row = cur.fetchone()
            if row:
                site = self.create_site_from_row(row)
                sites.append(site)

        cur.close()     # close the database cursor
        conn.close()    # close the database connection
        
        return sites

    def get_all_variables(self):
        """Returns a list of all Variable() objects that are associated with
        a current real-time deployment.

        """
        if DEBUG_PRINT:
            print '*get_all_variables'

        variables = []
        
        # Open a connection to the database
        conn = pg.connect(DB_CONNECT_STR)
        cur = conn.cursor()
        
        # this will return all the variables that 
        # are associated with real-time deployments that have not ended
        #TODO:  Include real-time deployments that have ended within 30 days.
        cur.execute("""
        SELECT DISTINCT
        	concat(DataSeries.sensorid,dataseries.variableid,dataseries.mediumtypeid,dataseries.datatypeid,dataseries.variableunitsid) as variablecode,
        	Variables.VariableName,
        	DataSeries.VariableUnitsID,
        	DataSeries.MethodID,
        	MediumTypes.Term as mediumtype,
        	DataSeries.TimeSupportValue,
        	DataSeries.TimeSupportUnitsID,
        	DataTypes.Term as datatype,
        	GeneralCategories.Term as generalcategory
        FROM Methods
        	RIGHT JOIN (GeneralCategories
        		RIGHT JOIN (MediumTypes	
        			RIGHT JOIN (Variables
        				RIGHT JOIN (DataTypes
        					RIGHT JOIN (Deployments
        						RIGHT JOIN DataSeries
        						ON Deployments.DeploymentID = DataSeries.DeploymentID)
        					ON DataTypes.DataTypeID = DataSeries.DataTypeID)
        				ON Variables.VariableID = DataSeries.VariableID)
        			ON MediumTypes.MediumTypeID = DataSeries.MediumTypeID)
        		ON GeneralCategories.GeneralCategoryID = DataSeries.GeneralCategoryID)
        	ON Methods.methodid = DataSeries.MethodID
        WHERE (((Deployments.IsRealTime)=True) AND 
            (Deployments.DeploymentEndDateTime is NULL));""")        
        
        table = cur.fetchall()

        if not table:
            cur.close()     # close the database cursor
            conn.close()    # close the database connection        
            return []

        for row in table:
            var1 = czo_model.Variable()
            
            # These commented properties have defaults in czo_model.py
            var1.VariableCode = row[0]
            var1.VariableName = row[1]
            var1.VariableUnitsID = row[2]
            var1.MethodID = row[3]
            var1.SampleMedium = row[4]
            var1.TimeSupport = row[5]
            var1.TimeUnitsID = row[6]
            var1.DataType = row[7]
            var1.GeneralCategory = row[8]
            #var1.ValueType = None
                #Leaving empty assumes that the value type is "Feild Observation" from czo_model
            #var1.IsRegular = None
                #Leaving empty assumes that the IsRegular=TRUE from czo_model
            #var1.NoDataValue = None
                #Leaving empty assumes that the NoDataValue=-9999 from czo_model
            #var1.VariableDescription = None
                #At this time, we do not have a variable discription.

            #Search for the meta-data on the variable parameter units
            var1_units = wof_base.BaseUnits() # create a sigle units instance
            var1_units.UnitsID = row[2]
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
            
            #Search for the meta-data on the variable time-support units
            var1_TimeUnits = wof_base.BaseUnits() # create a sigle units instance
            var1_TimeUnits.UnitsID = row[6]
            cur.execute("""
            SELECT
                unitname,
                unittype,
                unitabbreviation
            FROM
                units
            WHERE
                unitid = %s;""", (var1_TimeUnits.UnitsID,))        
            db_unit = cur.fetchone()
            var1_TimeUnits.UnitsName = db_unit[0]
            var1_TimeUnits.UnitsType = db_unit[1]
            var1_TimeUnits.UnitsAbbreviation = db_unit[2]
            var1.TimeUnits = var1_TimeUnits # assign the variable units to the units instance

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

        # this will return the variable info
        # that matches var_code
        cur.execute("""
        SELECT DISTINCT
        	concat(DataSeries.sensorid,dataseries.variableid,dataseries.mediumtypeid,dataseries.datatypeid,dataseries.variableunitsid)
              as variablecode,
        	Variables.VariableName,
        	DataSeries.VariableUnitsID,
        	DataSeries.MethodID,
        	MediumTypes.Term as mediumtype,
        	DataSeries.TimeSupportValue,
        	DataSeries.TimeSupportUnitsID,
        	DataTypes.Term as datatype,
        	GeneralCategories.Term as generalcategory,
            DataSeries.ValueOrderNumber,
            Loggers.LoggerCode,
            Deployments.DeploymentDatetime,
            Deployments.DeployedUTCOffset
        FROM Loggers
            RIGHT JOIN (Methods
            	RIGHT JOIN (GeneralCategories
            		RIGHT JOIN (MediumTypes	
            			RIGHT JOIN (Variables
            				RIGHT JOIN (DataTypes
            					RIGHT JOIN (Deployments
            						RIGHT JOIN DataSeries
            						ON Deployments.DeploymentID = DataSeries.DeploymentID)
            					ON DataTypes.DataTypeID = DataSeries.DataTypeID)
            				ON Variables.VariableID = DataSeries.VariableID)
            			ON MediumTypes.MediumTypeID = DataSeries.MediumTypeID)
            		ON GeneralCategories.GeneralCategoryID = DataSeries.GeneralCategoryID)
            	ON Methods.methodid = DataSeries.MethodID)
             ON Loggers."LoggerID" = Deployments.Loggerid
        WHERE 
          concat(DataSeries.sensorid,dataseries.variableid,dataseries.mediumtypeid,dataseries.datatypeid,dataseries.variableunitsid)
            = %s;""", (var_code,))    

        row = cur.fetchone()

        if not row:
            cur.close()     # close the database cursor
            conn.close()    # close the database connection
            return
            
        var1 = czo_model.Variable()
            
        # These commented properties have defaults in czo_model.py
        var1.VariableCode = row[0]
        var1.VariableName = row[1]
        var1.VariableUnitsID = row[2]
        var1.MethodID = row[3]
        var1.SampleMedium = row[4]
        var1.TimeSupport = row[5]
        var1.TimeUnitsID = row[6]
        var1.DataType = row[7]
        var1.GeneralCategory = row[8]
        var1.csvColumn = row[9]
        var1.LoggerCode = row[10]
        var1.DeploymentDateTime = row[11]
        var1.DeploymentUTCOffset = row[12]
        #var1.ValueType = None
            #Leaving empty assumes that the value type is "Feild Observation" from czo_model
        #var1.IsRegular = None
            #Leaving empty assumes that the IsRegular=TRUE from czo_model
        #var1.NoDataValue = None
            #Leaving empty assumes that the NoDataValue=-9999 from czo_model
        #var1.VariableDescription = None
            #At this time, we do not have a variable discription.

        #Search for the meta-data on the variable parameter units
        var1_units = wof_base.BaseUnits() # create a sigle units instance
        var1_units.UnitsID = row[2]
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
            
        #Search for the meta-data on the variable time-support units
        var1_TimeUnits = wof_base.BaseUnits() # create a sigle units instance
        var1_TimeUnits.UnitsID = row[6]
        cur.execute("""
        SELECT
            unitname,
            unittype,
            unitabbreviation
        FROM
            units
        WHERE
            unitid = %s;""", (var1_TimeUnits.UnitsID,))        
        db_unit = cur.fetchone()
        var1_TimeUnits.UnitsName = db_unit[0]
        var1_TimeUnits.UnitsType = db_unit[1]
        var1_TimeUnits.UnitsAbbreviation = db_unit[2]
        var1.TimeUnits = var1_TimeUnits # assign the variable units to the units instance

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
        """Returns a list of Series() objects - one for each current real-time
        deployment at the specified site.

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

        # open database, read all of the variable codes avaukabke for this site
        conn = pg.connect(DB_CONNECT_STR)
        cur = conn.cursor()    
        cur.execute("""
        SELECT DISTINCT
        	concat(DataSeries.sensorid,dataseries.variableid,dataseries.mediumtypeid,dataseries.datatypeid,dataseries.variableunitsid) AS variablecode,
        	Sites.SiteNameCode
        FROM Sites
        	RIGHT JOIN DataSeries
             RIGHT JOIN Deployments
             ON DataSeries.DeploymentID = Deployments.DeploymentID
        	ON Sites.SiteID = DataSeries.SiteID
        WHERE (((Deployments.IsRealTime)=True) AND 
            (Deployments.DeploymentEndDateTime is NULL) AND
          Sites.SiteNameCode = %s);""", (site_code,))

        row = cur.fetchone()
        cur.close()     # close the database cursor
        conn.close()    # close the database connection
        
        if not row:
            return []
        
        # given all of the variable codes, create series for each variable
        # code in the given site code
        var_code = row[0]
        series = self.get_series_by_sitecode_and_varcode(site_code, var_code)
        series_list.extend(series)

        return series_list

    def get_series_by_sitecode_and_varcode(self, site_code, var_code):
        """Returns a single Series() object for a current real-time
        deployment at the specified site for the specified variable.

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
                # Presently, no way to get real start time, now arbitrarily Jan 1, 2013 0:00.
                # TODO:  Once the actual data is in the database, get the start times from the database.
                series.BeginDateTime = '2013-01-01T00:00:00'
                series.EndDateTime = time.strftime('%Y-%m-%dT%H:%M:%S')
                series.BeginDateTimeUTC = '2013-01-01T05:00:00Z'
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
        # this variable currently in real-time.
        conn = pg.connect(DB_CONNECT_STR)
        cur = conn.cursor()   
          
        cur.execute("""
        SELECT DISTINCT
        	concat(DataSeries.sensorid,dataseries.variableid,dataseries.mediumtypeid,dataseries.datatypeid,dataseries.variableunitsid) AS variablecode,
        	Sites.SiteNameCode
        FROM Sites
        	RIGHT JOIN DataSeries
             RIGHT JOIN Deployments
             ON DataSeries.DeploymentID = Deployments.DeploymentID
        	ON Sites.SiteID = DataSeries.SiteID
        WHERE (((Deployments.IsRealTime)=True) AND 
            (Deployments.DeploymentEndDateTime is NULL) AND
          Sites.SiteNameCode = %s;""", (site_code,))
          
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
            
        # which column has the data value
        value_index = varResult.csvColumn - 1

        # Parse input dates
        parse_result = self.parse_date_strings(begin_date_time, end_date_time)
        b = parse_result[0] # begin datetime
        e = parse_result[1] # end datetime

        #t1 = time.time()
        
        # grab the current time so we know which logger file to look in
        current_time = time.localtime()
        current_year = current_time[0]
        current_month = current_time[1]        
        
        data_file_list = [] # this list keeps track of all the data files we shoud scan        
        
        # add the current month to the data_file_list
        #current_month_file = 'data/' + str(current_year) + '-' + str(current_month) + '.csv'
        current_month_file = 'data_longformat_test/' + str(current_year) + '-' + str(current_month) + '.csv'

        current_month = current_month - 1        
        if current_month == 0:
            current_month = 12
            current_year = current_year - 1
        
        #previous_month_file = 'data/' + str(current_year) + '-' + str(current_month) + '.csv'
        previous_month_file = 'data_longformat_test/' + str(current_year) + '-' + str(current_month) + '.csv'

        # create a list of possible filenames to look for data
        # if any of the files do not exist, that's not a problem, they will be skipped
        # TODO:  Currently, is only looking for the last two files, so if you 
        # specifiy older data in the function call, it will fail to find it.
        # should look for files based on the actual requested times.
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
                        if len(row) >= 5: # error check to be sure row at least 5 elements
                            if row[2].strip() == varResult.LoggerCode: # first check if this row is for the requested logger
                                try: # catch a bad time string - though these should already be eliminated in the receiver
                                    value_date = parse(row[0]) # read the data value timedate stamp - see if valid
                                except: # if a bad time string, continue on to the next row
                                    continue 
                                if DEBUG_PRINT:
                                    print value_date
                                # Check that datetime stamp is within input date range and is after logger deployment
                                # TODO:  Fix fudging with time-zone here.  This just assumes that the time=zone of
                                # the deployment date-time is the same as that of the raw value in the file.
                                if (value_date >= b and value_date >= varResult.DeploymentDateTime and value_date <= e):
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
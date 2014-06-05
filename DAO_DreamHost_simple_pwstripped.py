# This is the data access object to be used with a PostgreSQL implementation
# of the logger database

from dateutil.parser import parse
from dateutil.tz import tzoffset as tz

from wof.dao import BaseDao
import wof.models as wof_base

import copy
import czo_model
import time
import pymysql

#For MySQL Access
DB_CONNECT_STR = "host='mysql.swrcsensors.dreamhosters.com',port=3306,db='XXXXX',user='XXXXX',passwd='XXXXX'"


# if DEBUG_PRINT is True, lots of information will be printed to the console
# which slows retrieval of data series significantly
DEBUG_PRINT = False

class czoDao(BaseDao):
    #def __init__(self, values_file_path):
    #    if DEBUG_PRINT:
    #        print '*CsvDao __init__'     
    #    self.values_file_path = values_file_path

    def __init__(self):
        if DEBUG_PRINT:
            print '*CsvDao __init__'     

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
        conn=pymysql.connect(host="mysql.swrcsensors.dreamhosters.com",port=3306,db="XXXXX",user="XXXXX",passwd="XXXXX")
        cur = conn.cursor()
        
        # this will find all the sites that have CURRENT real-time deployments
        #TODO:  Include real-time deployments that have ended within 30 days.
        # returns SiteID, SiteCode, SiteName, Latitude, Longitude, Elevation_m, spatialref SRSID
        cur.execute("""
        SELECT DISTINCT
            SiteID,
            SiteCode,
            SiteName,
            Latitude,
            Longitude,
            Elevation_m,
            SpatialReference
        FROM Sites_for_midStream;
        """)
            
        table = cur.fetchall()

        cur.close()     # close the database cursor
        conn.close()    # close the database connection

        if not table:
            return []

        sites = []

        # iterate over the rows in the database
        for row in table:
            if DEBUG_PRINT:
                print 'Subquery of get_all_sites:'
                print '('
            site = self.create_site_from_row(row)
            if DEBUG_PRINT:
                print ')'
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
        conn=pymysql.connect(host="mysql.swrcsensors.dreamhosters.com",port=3306,db="XXXXX",user="XXXXX",passwd="XXXXX")
        cur = conn.cursor()

        # this will find the site by site_code
        # returns SiteID, SiteCode, SiteName, Latitude, Longitude, Elevation_m, spatialref SRSID
        cur.execute("""
        SELECT DISTINCT
            SiteID,
            SiteCode,
            SiteName,
            Latitude,
            Longitude,
            Elevation_m,
            SpatialReference
        FROM Sites_for_midStream
        WHERE 
          Sites_for_midStream.sitecode = %s;""", (site_code,))

        row = cur.fetchone()
        cur.close()     # close the database cursor
        conn.close()    # close the database connection

        if DEBUG_PRINT:
            print 'Subquery of get_site_by_code:'
            print '('
        site = self.create_site_from_row(row)
        if DEBUG_PRINT:
            print ')'
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
        conn=pymysql.connect(host="mysql.swrcsensors.dreamhosters.com",port=3306,db="XXXXX",user="XXXXX",passwd="XXXXX")
        cur = conn.cursor()

        sites = []

        for site_code in site_codes_arr:
            # this will find the site by site_code
            # returns SiteID, SiteCode, SiteName, Latitude, Longitude, Elevation_m, spatialref SRSID
            cur.execute("""
        SELECT DISTINCT
            SiteID,
            SiteCode,
            SiteName,
            Latitude,
            Longitude,
            Elevation_m,
            SpatialReference
        FROM Sites_for_midStream
            WHERE 
              Sites_for_midStream.sitecode = %s;""", (site_code,))

            row = cur.fetchone()
            if row:
                if DEBUG_PRINT:
                    print 'Subquery of get_sites_by_codes:'
                    print '('
                site = self.create_site_from_row(row)
                if DEBUG_PRINT:
                    print ')'
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
        conn=pymysql.connect(host="mysql.swrcsensors.dreamhosters.com",port=3306,db="XXXXX",user="XXXXX",passwd="XXXXX")
        cur = conn.cursor()
        
        # this will return all the variables that 
        # are associated with real-time deployments that have not ended
        #TODO:  Include real-time deployments that have ended within 30 days.
        cur.execute("""
        SELECT DISTINCT
            VariableCode,
            VariableName,
            VariableUnitsID,
            MethodID,
            MediumType,
            TimeSupportValue,
            TimeSupportUnitsID,
            DataType,
            GeneralCategory
        FROM Variables_for_midStream;
        """)        
        
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
                UnitName,
                UnitType,
                UnitAbbreviation
            FROM
                Units_for_midStream
            WHERE
                UnitID = %s;""", (var1_units.UnitsID,))        
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
                UnitName,
                UnitType,
                UnitAbbreviation
            FROM
                Units_for_midStream
            WHERE
                UnitID = %s;""", (var1_TimeUnits.UnitsID,))        
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
        conn=pymysql.connect(host="mysql.swrcsensors.dreamhosters.com",port=3306,db="XXXXX",user="XXXXX",passwd="XXXXX")
        cur = conn.cursor()

        # this will return the variable info
        # that matches var_code
        cur.execute("""
        SELECT DISTINCT
            VariableCode,
            VariableName,
            VariableUnitsID,
            MethodID,
            MediumType,
            TimeSupportValue,
            TimeSupportUnitsID,
            DataType,
            GeneralCategory
        FROM Variables_for_midStream
        WHERE 
          VariableCode = %s;""", (var_code,))    

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
           UnitName,
           UnitType,
           UnitAbbreviation
        FROM
           Units_for_midStream
        WHERE
           UnitID = %s;""", (var1_units.UnitsID,))        
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
            UnitName,
            UnitType,
            UnitAbbreviation
        FROM
            Units_for_midStream
        WHERE
            UnitID = %s;""", (var1_TimeUnits.UnitsID,))        
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
            if DEBUG_PRINT:
                print 'Subquery of get_variables_by_codes:'
                print '['
            var1 = self.get_variable_by_code(var_code)
            if DEBUG_PRINT:
                print ']'
            vars.append(copy.deepcopy(var1))
        return vars

    def get_series_by_sitecode(self, site_code):
        """Returns a list of Series() objects - one for each current
        deployment at the specified site.

        Required Arguments:
            A list of ascii site codes
        """
        if DEBUG_PRINT:
            print '*get_series_by_sitecode: ' + site_code

        if not site_code:
            return []
        
        series_list = []
        
        if DEBUG_PRINT:
            print 'Subquery of get_series_by_sitecode:'
            print '{'
        site = self.get_site_by_code(site_code)
        if DEBUG_PRINT:
            print '}'
        
        if not site:
            return []

        # open database, find all of the variable codes available for this site
        conn=pymysql.connect(host="mysql.swrcsensors.dreamhosters.com",port=3306,db="XXXXX",user="XXXXX",passwd="XXXXX")
        cur = conn.cursor()    
        cur.execute("""
        SELECT DISTINCT
            Variables_for_midStream.VariableCode,
            Sites_for_midStream.SiteCode
        FROM Sites_for_midStream
            RIGHT JOIN Series_for_midStream
             RIGHT JOIN Variables_for_midStream
             ON Variables_for_midStream.VariableID = Series_for_midStream.VariableID
            ON Sites_for_midStream.SiteID = Series_for_midStream.SiteID
        WHERE (Sites_for_midStream.SiteCode = %s);""", (site_code,))

        table = cur.fetchall()
        cur.close()     # close the database cursor
        conn.close()    # close the database connection
        
        if not table:
            return []
        
        # given all of the variable codes, create series for each variable
        # code in the given site code
        for row in table:
            var_code = row[0]
            
            if DEBUG_PRINT:
                print 'Subquery of get_series_by_sitecode:'
                print '{'
            series = self.get_series_by_sitecode_and_varcode(site_code, var_code)
            if DEBUG_PRINT:
                print '}'
                
            series_list.extend(series)
        
        if DEBUG_PRINT:
            number_data_series = len(series_list)
            print 'Number Data Series Found for this site: %d' % (number_data_series)

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
        if DEBUG_PRINT:
            print 'Subquery of get_series_by_sitecode_and_varcode:'
            print '<'
        site = self.get_site_by_code(site_code)
        if DEBUG_PRINT:
            print '>'
        if site:
            if DEBUG_PRINT:
                print 'Subquery of get_series_by_sitecode_and_varcode:'
                print '<'
            var = self.get_variable_by_code(var_code)
            if DEBUG_PRINT:
                print '>'
            if var:
                
                series = czo_model.Series()
                if DEBUG_PRINT:
                    print 'Subquery of get_series_by_sitecode_and_varcode:'
                    print '<'
                method_list = self.get_methods_by_ids([var.MethodID])
                if DEBUG_PRINT:
                    print '>'
                
                # open database, the right table name, and column names for the data values
                conn=pymysql.connect(host="mysql.swrcsensors.dreamhosters.com",port=3306,db="XXXXX",user="XXXXX",passwd="XXXXX")
                cur = conn.cursor()    
                cur.execute("""
                SELECT DISTINCT
                    Series_for_midStream.TableName,
                    Series_for_midStream.TableColumnName
                FROM Sites_for_midStream
                    RIGHT JOIN Series_for_midStream
                        RIGHT JOIN Variables_for_midStream
                        ON Variables_for_midStream.VariableID = Series_for_midStream.VariableID
                    ON Sites_for_midStream.SiteID = Series_for_midStream.SiteID
                WHERE (Sites_for_midStream.SiteCode = %s AND Variables_for_midStream.VariableCode = %s);
                """,(site_code,var_code))
        
                row = cur.fetchone()
                cur.close()     # close the database cursor
                conn.close()    # close the database connection
                
                table_name = row[0]
                column_name = row[1]  
                
                # Looking through tables to get start and end times
                if DEBUG_PRINT:
                    print 'Subquery of get_series_by_sitecode_and_varcode:'
                    print '<'
                series.BeginDateTime = self.get_begin_datetime(table_name,column_name)
                if DEBUG_PRINT:
                    print '>'
                if DEBUG_PRINT:
                    print 'Subquery of get_series_by_sitecode_and_varcode:'
                    print '<'
                series.EndDateTime = self.get_end_datetime(table_name,column_name)
                if DEBUG_PRINT:
                    print '>'
                series.BeginDateTimeUTC = '2013-01-01T05:00:00Z'
                series.EndDateTimeUTC = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime()) + 'Z'

                series.Site = site
                series.Variable = var
                if method_list:
                    series.Method = method_list[0]
                series_list.append(series)
                                
                if DEBUG_PRINT:
                    number_data_series = len(series_list)
                    print 'Number Data Series Found for this site AND variable: %d' % (number_data_series)
                    
        return series_list
                
    def get_begin_datetime(self, table_name, column_name):
        """ Returns the first date/time in a datatable, given the table name.

        Required Arguments:
            An ascii table name.
        """
        if DEBUG_PRINT:
            print '*get_begin_datetime: ' + table_name + ' - ' + column_name
        
        if not table_name:
            return []
        
        begin_datetime = []

        # open database, read the earlist time for this table/data column
        conn=pymysql.connect(host="mysql.swrcsensors.dreamhosters.com",port=3306,db="XXXXX",user="XXXXX",passwd="XXXXX")
        cur = conn.cursor()  
        cur.execute("""
        SELECT MIN(Date)
        FROM %s
        WHERE %s != '';
        """ % (table_name,column_name))
        begin_datetime = cur.fetchone()
        cur.close()     # close the database cursor
        conn.close()    # close the database connection
        
        if not begin_datetime:
            return []
        
        return begin_datetime
        
    def get_end_datetime(self, table_name, column_name):
        """ Returns the last date/time in a datatable, given the table name.

        Required Arguments:
            An ascii table name.
        """
        if DEBUG_PRINT:
            print '*get_end_datetime: ' + table_name + ' - ' + column_name
        
        if not table_name:
            return []
        
        end_datetime = []

         # open database, read the latest time for this table/data column
        conn=pymysql.connect(host="mysql.swrcsensors.dreamhosters.com",port=3306,db="XXXXX",user="XXXXX",passwd="XXXXX")
        cur = conn.cursor()  
        cur.execute("""
        SELECT MAX(Date)
        FROM %s
        WHERE %s != '';
        """ % (table_name,column_name))
        end_datetime = cur.fetchone()
        cur.close()     # close the database cursor
        conn.close()    # close the database connection
        
        if not end_datetime:
            return []
        
        return end_datetime
  
    

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

        # check with the database whether this site actually is tracking this variable.
        conn=pymysql.connect(host="mysql.swrcsensors.dreamhosters.com",port=3306,db="XXXXX",user="XXXXX",passwd="XXXXX")
        cur = conn.cursor() 
          
        cur.execute("""
        SELECT DISTINCT
            Series_for_midStream.TableName,
            Series_for_midStream.TableColumnName
        FROM Sites_for_midStream
            RIGHT JOIN Series_for_midStream
                RIGHT JOIN Variables_for_midStream
                ON Variables_for_midStream.VariableID = Series_for_midStream.VariableID
            ON Sites_for_midStream.SiteID = Series_for_midStream.SiteID
        WHERE (Sites_for_midStream.SiteCode = %s AND Variables_for_midStream.VariableCode = %s);
        """,(site_code,var_code))
     
        row = cur.fetchone()
        cur.close()
        conn.close()  
        
        #if the query returns empty handed, return an empty list.
        if not row:
            return []
        
        #else the query result is the table and column name of the desired variable
        table_name = row[0]
        column_name = row[1]              
        
        # Find the site and variable information using those functions
        if DEBUG_PRINT:
            print 'Subquery of get_datavalues:'
            print '\\'
        siteResult = self.get_site_by_code(site_code)
        if DEBUG_PRINT:
            print '//'
        if DEBUG_PRINT:
            print 'Subquery of get_datavalues:'
            print '\\'
        varResult = self.get_variable_by_code(var_code)
        if DEBUG_PRINT:
            print '//'
        valueResultArr = []

        # if failed to find site or variable
        if not (siteResult and varResult):
            return []

        if DEBUG_PRINT:
            print '- ' + varResult.VariableCode
            print '- ' + varResult.VariableName

        # Parse input dates
        if DEBUG_PRINT:
            print 'Subquery of get_datavalues:'
            print '\\'
        parse_result = self.parse_date_strings(begin_date_time, end_date_time)
        if DEBUG_PRINT:
            print '//'
        b = parse_result[0] # begin datetime
        e = parse_result[1] # end datetime

        if DEBUG_PRINT:  #To see how long this takes
            t1 = time.time()
            print "Executing this SQL Query to find data values:"
            print """
                  SELECT Date, %s
                  FROM %s
                  WHERE %s != '' AND date >= %s and date <= %s;
                  """ % (column_name,table_name,column_name,b,e)        
   
       # open database, read the values for this table/data column
        conn=pymysql.connect(host="mysql.swrcsensors.dreamhosters.com",port=3306,db="XXXXX",user="XXXXX",passwd="XXXXX")
        cur = conn.cursor()  
        cur.execute("""
        SELECT Date, %s
        FROM %s
        WHERE %s != '' AND date >= '%s' and date <= '%s';
        """ % (column_name,table_name,column_name,b,e))
        table = cur.fetchall()
        cur.close()     # close the database cursor
        conn.close()    # close the database connection
        
        for row in table:
            datavalue = czo_model.DataValue()
            datavalue.DataValue = row[1]
            LocalDateTime_noUTC = row[0]
            local_time_zone = tz(None, -18000) # Five hours behind UTC, in seconds
            datavalue.LocalDateTime = LocalDateTime_noUTC.replace(tzinfo=local_time_zone)
            datavalue.MethodID = varResult.MethodID # MethodID was not originally part of datavalue object
            valueResultArr.append(datavalue)           
                    
        if DEBUG_PRINT:        
            t2 = time.time()
            print 'get_datavalues Exec Time: %0.3f s' % (t2-t1)
            print 'Number of Values Returned: %d' % (len(valueResultArr))
            
        return valueResultArr

    def get_methods_by_ids(self, method_id_arr):
        """Returns a list of Method() objects

        Required Arguments:
            A list of method codes (integers)
        """
        if DEBUG_PRINT:
            print 'get_methods_by_ids: '

        if not method_id_arr:
            return []

        methods = [] 

        # Open a connection to the database
        conn=pymysql.connect(host="mysql.swrcsensors.dreamhosters.com",port=3306,db="XXXXX",user="XXXXX",passwd="XXXXX")
        cur = conn.cursor()    

        for method_id in method_id_arr:
            cur.execute("""
            SELECT
                MethodID,
                Term,
                Definition
            FROM
                Methods_for_midStream
            WHERE
                MethodID = %s;""", (method_id,))
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
            
        At this time, the source is HARDCODED to be Steve Hicks.
        """
        if DEBUG_PRINT:
            print '*get_sources_by_ids: '
            
        sources = []
        source = czo_model.Source()
        sources.append(source) # append the source to list of sources
        return sources

    def get_qualifiers_by_ids(self, qualifier_id_arr):
        if DEBUG_PRINT:
            print '*get_qualifiers_by_ids'        
        return []

    def get_offsettypes_by_ids(self, offset_type_id_arr):
        if DEBUG_PRINT:
            print '*get_offsettypes_by_ids'
        return []
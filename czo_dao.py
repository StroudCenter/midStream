from datetime import timedelta
import csv

from dateutil.parser import parse
from dateutil.tz import tzoffset as tz

from wof.dao import BaseDao
import wof.models as wof_base

import copy
import czo_model
import time

DEBUG_PRINT = False

class czoDao(BaseDao):
    def __init__(self, sites_file_path, methods_file_path, values_file_path, header_file_path):
        if DEBUG_PRINT:
            print '*CsvDao __init__'     
        self.sites_file_path = sites_file_path
        self.methods_file_path = methods_file_path
        self.values_file_path = values_file_path
        self.header_file_path = header_file_path

        # Build a dictionary of variables indexed by code
        variable_dict = {}
        variable_list = []
        unit_list = []
        method_list = []
        column_list = []
        method_code_to_id_dict = {}
        method_id_to_description_dict = {}
        col_to_method_id_dict = {}

        # Create a dictionary of method description from the METHODS csv file
        with open (self.methods_file_path, 'rb') as f:
            reader = csv.reader(f)
            row_num = 0
            for row in reader:
                method_code_to_id_dict[row[0].strip()] = row_num    # the row number becomes the ID
                method_id_to_description_dict[row_num] = row[1].strip()
                row_num = row_num + 1

        self.method_code_to_id_dict = method_code_to_id_dict
        self.method_id_to_description_dict = method_id_to_description_dict

        # read the variable lines from the CZO HEADER and create new
        # variable instances for each element in the header.
        with open(self.header_file_path, 'rb') as f:
            reader = csv.reader(f)

            is_doc = True       # assume starting in doc portion of header file
            for row in reader:
                if is_doc == True:  # skip over the doc part of he header
                    if row[0].strip() == '\header':
                        is_doc = False
                    continue
                elif row[0] == 'COL1':  # COL1 is not a variable
                    continue
                elif row[0] == 'COL2':  # COL2 is not a variable
                    continue
                
                # store just the column number for each variable
                # the variables start at COL3
                # note, this is not used anymore as the columns are
                # now stored along with the sites under each column
                column = row[0].replace('COL', '')
                column = int(column.strip()) - 1 # the datafile starts at column 0, so subtract 1
                column_list.append(column) # create a list of column numbers               
                
                # store what this variable measuring
                var = row[2].replace(' ', '')   # strip all spaces from string
                var = var.replace('value=','')  # remove leading part of string
                variable_list.append(var) # this is a long description of the variable
                
                # store the units of this variable
                unit = row[3].strip()          # strip leading and trailing spces
                unit = unit.replace('units=','')  # remove leading part of string
                unit_list.append(unit)
                
                # store the method code for each variable.  This is an 
                # alpha-numeric code not to be confused with MethodID
                method = row[4].strip()
                method = method.replace('method=','')
                method_list.append(method)
                
                if DEBUG_PRINT:
                    print 'Header Found: COL' + str(column) + ' | ' + var + ' | ' + unit + ' | ' + method
        
        # iterate through each variable and createthe variable dictionary
        for var_counter in range(len(variable_list)):        
            var1 = czo_model.Variable()     # create a single variable instance

            var1.csvColumn = column_list[var_counter]   # store the column num that contains this var
            
            var1.VariableID = column_list[var_counter] - 1 # create a variable ID based on the var's column
            var1.VariableCode = variable_list[var_counter] + '_' + method_list[var_counter] # store the variable code (short string)
            var1.VariableName = variable_list[var_counter] # store the variable name (long string)
            var1.VariableDescription = variable_list[var_counter] + '_' + method_list[var_counter]

            var1_units = wof_base.BaseUnits() # createa sigle units instance
            var1_units.UnitsName = unit_list[var_counter] # set the units name
            var1_units.UnitsType = 'Unknown'    # TODO: Can the UnitsType be determined
            var1_units.UnitsAbbreviation = 'Unknown'    # TODO: Can the UnitsAbbrevition be determined
            var1.VariableUnits = var1_units # assign the variable units to the units instance

            # relate the method id to the csv column 
            col_to_method_id_dict[var1.csvColumn] = method_code_to_id_dict[method_list[var_counter]]

            variable_dict[var1.VariableCode] = copy.deepcopy(var1) # deepcopy is used to create new instance of var1

        self.col_to_method_id_dict = col_to_method_id_dict
        self.variable_dict = variable_dict

    def __del__(self):
        pass # Could end database session here for more sophisticated DAOs

    def create_site_from_row(self, csv_row):
        if DEBUG_PRINT:
            print '*create_site_from_row: ' + repr(csv_row) 
        site = czo_model.Site()             # create a new instance of site
        site.SiteCode = csv_row[0].strip()  # reove whitespace from front and back of string
        site.SiteName = csv_row[1]          # for some reason the upper level calls require at least a space in SiteName
        site.Latitude = csv_row[2].strip()
        site.Longitude = csv_row[3].strip()
        if csv_row[5].strip() != '.':        
            site.Elevation_m = csv_row[5].strip()
        site_spatialref = wof_base.BaseSpatialReference()
        # Decide which coordinate system is indicated
        if csv_row[4].strip() == 'NAD83':        
            site_spatialref.SRSID = 4269
        elif csv_row[4].strip() == 'WGS84':
            site_spatialref.SRSID = 4326
        else:
            site_spatialref.SRSID = -9999
        site.LatLongDatum = site_spatialref
        
        # the last columns of each site row contain a list of variables
        # the order of these variables determines the order of th variables
        # in the logger file
        num_vars = len(csv_row) - 12
        site.varCodeList = [] # this is a new variable that did not exist in the base site DAO
        for x in range(num_vars):
            site.varCodeList.append(csv_row[12+x])
        return copy.deepcopy(site)

    def get_all_sites(self):
        if DEBUG_PRINT:
            print '*get_all_sites'        
        sites = []
        site_id = 0
        with open(self.sites_file_path, 'rb') as f:
            reader = csv.reader(f)
            at_header = True
            for row in reader:
                if at_header:
                    at_header = False
                    continue
                site_id = site_id + 1  # just create a site id from the row number              
                site = self.create_site_from_row(row)
                site.SiteID = site_id  # assign the site ID
                if DEBUG_PRINT:
                    print site
                sites.append(site)
        return sites

    def get_site_by_code(self, site_code):
        if DEBUG_PRINT:
            print '*get_site_by_code: ' + site_code
        if site_code.strip():   # NEW.. only bother to check if site_code isn't blank
            with open(self.sites_file_path, 'rb') as f:
                reader = csv.reader(f)
                at_header = True
                for row in reader:
                    if at_header:
                        at_header = False
                        continue  
                    if row[0].strip() == site_code:     # strip removes whitespace:
                        site = self.create_site_from_row(row)
                        return site
        return None     # NEW
        
    def get_sites_by_codes(self, site_codes_arr):
        if DEBUG_PRINT:
            print '*get_sites_by_codes'        
        sites = []
        with open(self.sites_file_path, 'rb') as f:
            reader = csv.reader(f)
            at_header = True
            for row in reader:
                if at_header:
                    at_header = False
                    continue
                if row[0].strip() in site_codes_arr: #strip removes whitespace
                    site = self.create_site_from_row(row)
                    sites.append(site)
        return sites

    def get_all_variables(self):
        if DEBUG_PRINT:
            print '*get_all_variables'        
        return self.variable_dict.values()

    def get_variable_by_code(self, var_code):
        if DEBUG_PRINT:
            print '*get_variable_by_code: ' + var_code
        if var_code in self.variable_dict:
            return self.variable_dict[var_code]

    def get_variables_by_codes(self, var_codes_arr):
        if DEBUG_PRINT:
            print '*get_variables_by_code: ' + var_codes_arr[0]        
        vars = []
        for var_code in var_codes_arr:
            if var_code in self.variable_dict:
                vars.append(self.variable_dict[var_code])
        return vars

    def get_series_by_sitecode(self, site_code):
        if DEBUG_PRINT:
            print '*get_series_by_sitecode: ' + site_code
        series_list = []
        site = self.get_site_by_code(site_code)
        if site:
            all_vars = self.get_all_variables()
            for avariable in all_vars:
                # TODO: IF statement - IF avariable.VariableCode is associated with the sitecode only
                if avariable.VariableCode in site.varCodeList:# 'var1_PHSWRC': # does this variable apply
                    series = self.get_series_by_sitecode_and_varcode(site_code, avariable.VariableCode)
                    series_list.extend(series)
        return series_list

    def get_series_by_sitecode_and_varcode(self, site_code, var_code):
        if DEBUG_PRINT:
            print '*get_series_by_sitecode_and_varcode: ' + site_code + ' - ' + var_code
        series_list = []
        site = self.get_site_by_code(site_code)
        if site:
            var = self.get_variable_by_code(var_code)
            if var:
                series = czo_model.Series()
                #TODO: Need to define series begin and end time
                #  this metadata is not provided in the CZO display files
                #  therefore, it needs to be implied from a scan of the date time stamps
                # which doesn't make sense since the data is being updated in real-time
                series.BeginDateTime = '2000-01-01T00:00-06'
                series.EndDateTime = '2012-12-30T00:00-06' # this could be current time
                series.BeginDateTimeUTC = '2000-01-01T06:00Z'
                series.EndDateTimeUTC = '2012-12-30T06:00Z' # this could be current time
                series.SiteCode = site.SiteCode
                series.SiteName = site.SiteName
                series.VariableCode = var.VariableCode
                series.VariableName = var.VariableName
                series.VariableUnitsID = var.VariableUnitsID
                series.VariableUnitsName = var.VariableUnits.UnitsName
                series.SampleMedium = var.SampleMedium
                series.ValueType = var.ValueType
                series.TimeSupport = var.TimeSupport
                series.TimeUnitsID = var.TimeUnitsID
                series.TimeUnitsName = var.TimeUnits.UnitsName
                series.DataType = var.DataType
                series.GeneralCategory = var.GeneralCategory
                series.Site = site
                series.Variable = var
                series_list.append(series)
        return series_list

    def parse_date_strings(self, begin_date_time_string, end_date_time_string):
        if DEBUG_PRINT:
            print '*parse_date_strings: '# + begin_date_time_string + ' ' + end_date_time_string
        """Returns a list with parsed datetimes in the local time zone.

        Required Arguments:
            begin_date_time (begin datetime as text)
            end_date_time (end datetime as text)
        Remarks:
            The returned list has two items:
                begin datetime as datetime.datetime object
                end datetime as datetime.datetime object
        """
        
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

        # If we know time zone, convert to local time.  Otherwise, assume
        # local time.
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
        if DEBUG_PRINT:
            print '*create_datavalue_from_row: ' + repr(row) + ' - ' + repr(value_index)        
        datavalue = czo_model.DataValue()

        datavalue.DataValue = row[value_index].strip()

        # All values are in local time. For this example, Stroud time is always
        # five hours behind UTC time.
        value_date = parse(row[0])

        datavalue.LocalDateTime = value_date.isoformat() + '-05'

        value_date = value_date + timedelta(hours=5)
        datavalue.DateTimeUTC = value_date.isoformat() + 'Z'

        # since we know the value_index (row in data csv), we can tranlate that into a MethodID
        datavalue.MethodID = self.col_to_method_id_dict[value_index] # translate the value index into a MethodID

        return datavalue

    def get_datavalues(self, site_code, var_code, begin_date_time=None,
                       end_date_time=None):
        if DEBUG_PRINT:
            print '*get_datavalues: ' + site_code + ' - ' + var_code        
        # Find the site and variable
        siteResult = self.get_site_by_code(site_code)
        varResult = self.get_variable_by_code(var_code)
        valueResultArr = []

        if DEBUG_PRINT:
            print '- ' + varResult.VariableCode
            print '- ' + varResult.VariableName

        if siteResult and varResult:
            # Determine which column has the values
            #value_index = varResult.csvColumn            
            value_index = siteResult.varCodeList.index(var_code) + 3 

            # Parse input dates
            parse_result = self.parse_date_strings(begin_date_time, end_date_time)
            b = parse_result[0] # begin datetim
            e = parse_result[1] # end datetime

            # Read values from values file           
            with open(self.values_file_path, 'rb') as f:
                reader = csv.reader(f)
                #at_header = True
                for row in reader:
                    #if at_header:
                    #    if row[0].strip() == '\data':                        
                    #        at_header = False
                    #    continue

                    if len(row) > 3: # error check to be sure row has at least 4 elements
                        if row[2].strip() == site_code: # first check if this row is for the requested site
                            value_date = parse(row[0]) # read the data value timedate stamp
                            if DEBUG_PRINT:
                                print value_date
                            # Check that datetime stamp is within input date range
                            if (value_date >= b and value_date <= e):
                                # Add data value to result list
                                datavalue = self.create_datavalue_from_row(row, value_index)
                                if datavalue.DataValue != '-9999':       # only return real data                     
                                    valueResultArr.append(datavalue)

            if DEBUG_PRINT:
                print valueResultArr
            
            return valueResultArr


    def get_methods_by_ids(self, method_id_arr):
        if DEBUG_PRINT:
            print '****get_methods_by_ids: '
            print method_id_arr        
        if method_id_arr:        
            method = czo_model.Method()
            method.MethodID = method_id_arr[0]
            method.MethodDescription = self.method_id_to_description_dict[method_id_arr[0]]
            methods = []
            #if method.MethodID in method_id_arr:
            methods.append(method)
            return methods

    def get_sources_by_ids(self, source_id_arr):
        # Read header file for source
        if DEBUG_PRINT:
            print '*get_sources_by_ids'        

        source = czo_model.Source()

        # Open the header file and scan for abstract, title, investigator
        # gather the contact name, title, abstract, etc
        with open(self.header_file_path, 'rb') as f:
            reader = csv.reader(f)
            for row in reader:
                if row[0].strip() == 'ABSTRACT':
                    source.Metadata.Abstract = row[1].strip()
                    continue
                if row[0].strip() == 'TITLE':
                    source.Metadata.Title = row[1].strip()
                    continue
                if row[0].strip() == 'INVESTIGATOR':
                    source.SourceID = 1
                    source.Organization = row[2].strip()
                    source.ContactName = row[1].strip()
                    break
        # this function is expected to return an array of sources
        # for now, we are assuming there is just one source organization
        sources = []
        if 1 in source_id_arr:
            sources.append(source)
        return sources

    def get_qualifiers_by_ids(self, qualifier_id_arr):
        if DEBUG_PRINT:
            print '*get_qualifiers_by_ids'        
        return []

    def get_offsettypes_by_ids(self, offset_type_id_arr):
        if DEBUG_PRINT:
            print '*get_offsettypes_by_ids'
        return []
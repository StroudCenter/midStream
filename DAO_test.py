from DAO_DreamHost_simple import czoDao   # if metadata is contained in database


# print "^^^^^^Testing create_site_from_row^^^^^^"
# dao = czoDao()
# row = (1, 'WCC019', 'WCC 19mUS', 39.85993331, -75.78334808, 98.7, 'WGS84')
# dao.create_site_from_row(row)
# print "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
# print "\n"

# print "^^^^^^Testing get_table_datetime^^^^^^"
# dao = czoDao()
# dao.get_table_datetime('SL032', 'CTDdepth', 'min')
# print "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
# print "\n"

# print "^^^^^^Testing parse_date_strings^^^^^^"
# dao = czoDao()
# dao.parse_date_strings('2016-03-01', '2016-03-31')
# print "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
# print "\n"

# print "^^^^^^Testing get_methods_by_ids^^^^^^"
# dao = czoDao()
# method_id_arr = [1, 20]
# dao.get_methods_by_ids(method_id_arr)
# print "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
# print "\n"

# print "^^^^^^Testing get_all_sites^^^^^^"
# dao = czoDao()
# dao.get_all_sites()
# print "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
# print "\n"

# print "^^^^^^Testing get_site_by_code^^^^^^"
# dao = czoDao()
# dao.get_site_by_code('WCC019')
# print "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
# print "\n"

# print "^^^^^^Testing get_sites_by_codes^^^^^^"
# dao = czoDao()
# site_codes_arr = ['WCC019', 'WCC040', 'WCCPH']
# dao.get_sites_by_codes(site_codes_arr)
# print "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
# print "\n"

# print "^^^^^^Testing get_all_variables^^^^^^"
# dao = czoDao()
# dao.get_all_variables()
# print "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
# print "\n"

# print "^^^^^^Testing get_variable_by_code^^^^^^"
# dao = czoDao()
# dao.get_variable_by_code('BoardTemp')
# print "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
# print "\n"

# print "^^^^^^Testing get_variables_by_codes^^^^^^"
# dao = czoDao()
# var_codes_arr = ['CTDdepth', 'CTDtemp', 'CTDcond']
# dao.get_variables_by_codes(var_codes_arr)
# print "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
# print "\n"

print "^^^^^^Testing get_series_by_sitecode^^^^^^"
dao = czoDao()
dao.get_series_by_sitecode('DavisWeather')
print "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
print "\n"

# print "^^^^^^Testing get_series_by_sitecode_and_varcode^^^^^^"
# dao = czoDao()
# dao.get_series_by_sitecode_and_varcode('WCC019', 'CTDdepth')
# print "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
# print "\n"

# print "^^^^^^Testing get_datavalues^^^^^^"
# dao = czoDao()
# dao.get_datavalues('WCCPH', 'CTDdepth', begin_date_time='2014-03-01 00:00:00', end_date_time='2016-05-31 23:59:59')
# print "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
# print "\n"

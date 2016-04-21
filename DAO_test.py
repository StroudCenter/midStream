import DAO_DreamHost_simple as dao

row = ((1,'WCC019', 'WCC 19mUS',39.85993331,-75.78334808,98.7,'WGS84'))
dao.create_site_from_row(row)

dao.get_all_sites()

dao.get_site_by_code('WCC019')

site_codes_arr = ['WCC019','WCC040','WCCPH']
dao.get_sites_by_codes(site_codes_arr)

dao.get_all_variables()

dao.get_variable_by_code('BoardTemp')

var_codes_arr = ['CTDdepth','CTDtemp','CTDcond']
dao.get_variables_by_codes(var_codes_arr)

dao.get_series_by_sitecode('WCC019')

dao.get_series_by_sitecode_and_varcode('WCC019', 'CTDdepth')

dao.get_begin_datetime('WCC019', 'CTDdepth')

dao.get_end_datetime('WCC019', 'CTDdepth')

dao.parse_date_strings('2016-03-01','2016-03-31')

dao.get_datavalues('WCC019', 'CTDdepth', begin_date_time='2016-03-01',end_date_time='2016-03-31')

method_id_arr = ['BoardChip','Dec5TM']
dao.get_methods_by_ids(method_id_arr)
# -*- coding: utf-8 -*-

"""
Created by Sara Geleskie Damiano on 6/2/2016 at 4:50 PM


"""

import requests

# Bring in all of the database connection information.
from dbinfo import aq_publish2_url, aq_username, aq_password


def url_request(path):
    return aq_publish2_url + path


def get_auth_token(username, password):
    payload = {'userName': username, 'encryptedPassword': password}
    r = requests.get(url_request('GetAuthToken'), params=payload)
    return r.content, r.cookies
content, cookie = get_auth_token(aq_username, aq_password)


def get_approval_list(cookies=cookie):
    r = requests.get(url_request('GetApprovalList'), cookies=cookies)
    return r.json()['Approvals']


def get_grade_list(cookies=cookie):
    r = requests.get(url_request('GetGradeList'), cookies=cookies)
    return r.json()['Grades']


def get_parameter_list(cookies=cookie):
    r = requests.get(url_request('GetParameterList'), cookies=cookies)
    return r.json()['Parameters']


def get_unit_list(cookies=cookie):
    r = requests.get(url_request('GetUnitList'), cookies=cookies)
    return r.json()['Units']


def create_extended_filter(filter_dict):
    if filter_dict is None:
        return None
    else:
        attrib_filter = "ExtendedFilters=["
        for key in filter_dict:
            attrib_filter += "{FilterName:" + key + ",FilterValue:" + filter_dict[key] + "},"
        attrib_filter += "]"
        attrib_filter = attrib_filter.replace("},]", "}]")
        return attrib_filter


def get_location_description_list(text_loc_id=None, num_loc_id=None, loc_folder=None,
                                  ext_filter_dict=None, cookies=cookie):
    payload = {'LocationName': text_loc_id, 'LocationIdentifier': num_loc_id, 'LocationFolder': loc_folder,
               'ExtendedFilters': create_extended_filter(ext_filter_dict)}
    r = requests.get(url_request('GetLocationDescriptionList'), cookies=cookies, params=payload)
    return r.json()['LocationDescriptions']


def get_location_data(text_loc_id=None, cookies=cookie):
    payload = {'LocationIdentifier': text_loc_id}
    r = requests.get(url_request('GetLocationData'), cookies=cookies, params=payload)
    return r.json()


def get_time_series_description_list(text_loc_id=None, changes_since=None, param=None,
                                     publish=None, ext_filter_dict=None, computation_id=None,
                                     computation_period_id=None, cookies=cookie):
    if changes_since is not None:
        changes_since = changes_since.isoformat()
    payload = {'LocationIdentifier': text_loc_id, 'ChangesSince': changes_since,
               'Parameter': param, 'Publish': publish,
               'ExtendedFilters': create_extended_filter(ext_filter_dict), 'ComputationIdentifier': computation_id,
               'ComputationPeriodIdentifier': computation_period_id}
    r = requests.get(url_request('GetTimeSeriesDescriptionList'), cookies=cookies, params=payload)
    return r.json()['TimeSeriesDescriptions']


def get_correction_list(text_ts_id, query_to=None, query_from=None, cookies=cookie):
    if query_to is not None:
        query_to = query_to.isoformat()
    if query_from is not None:
        query_from = query_from.isoformat()
    payload = {'TimeSeriesIdentifier': text_ts_id, 'QueryTo': query_to, 'QueryFrom': query_from}
    r = requests.get(url_request('GetCorrectionList'), cookies=cookies, params=payload)
    return r.json()['Corrections']

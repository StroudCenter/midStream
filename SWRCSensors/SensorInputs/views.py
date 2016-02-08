from django.shortcuts import render, redirect
from django.http import HttpResponse
import pytz
from django.utils import timezone
from SensorInputs.models import MethodsForMidstream, SeriesForMidstream, SitesForMidstream, UnitsForMidstream, \
    VariablesForMidstream, Sl099

# Create your views here.

def index(request):
    context_dict = {}
    response = render(request, 'SensorInputs/index.html', context_dict)
    return response

def about(request):
    context_dict = {}
    return render(request, 'SensorInputs/about.html', context_dict)

def streaming_input(request):
    context_dict = {}

    if request.method == 'GET':

        num_params = len(request.GET)
        datetime_value_utc = timezone.now()
        # eastern = pytz.timezone('US/Eastern')
        # datetime_value_est = eastern.localize(datetime_value_utc, is_dst=False)
        print "Server Time in UTC: ", datetime_value_utc
        # print "Time in EST: ", datetime_value_est

        for param, value in request.GET.items():
            if param == 'LoggerID':
                logger_id = value
                num_params = num_params - 1
                print "LoggerID is ", logger_id
            elif param == "Loggertime":
                logger_time = value
                num_params = num_params - 1
                print "Logger Time is ", logger_time
            else:
                print param, 'is', value
                exec("%s=%s" % (param,value))

        print "Input contains %s parameters" % (num_params)

    return render(request, 'SensorInputs/input.html', context_dict)


from django.contrib import admin
from SensorInputs.models import MethodsForMidstream, VariablesForMidstream, UnitsForMidstream, SeriesForMidstream, SitesForMidstream, Sl099

# Register your models here.
admin.site.register(MethodsForMidstream)
admin.site.register(VariablesForMidstream)
admin.site.register(UnitsForMidstream)
admin.site.register(SeriesForMidstream)
admin.site.register(SitesForMidstream)
admin.site.register(Sl099)
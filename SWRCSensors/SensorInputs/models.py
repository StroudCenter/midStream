from __future__ import unicode_literals
from django.db import models


class MethodsForMidstream(models.Model):
    methodid = models.BigIntegerField(primary_key=True)
    term = models.CharField(max_length=16)
    definition = models.CharField(max_length=60)

    class Meta:
        verbose_name_plural = "Methods"

    def __unicode__(self):
        return self.term


class UnitsForMidstream(models.Model):
    unitid = models.BigIntegerField(primary_key=True)
    unitname = models.CharField(max_length=60)
    unittype = models.CharField(max_length=20)
    unitabbreviation = models.CharField(max_length=20)

    class Meta:
        verbose_name_plural = "Units"

    def __unicode__(self):
        return self.unitname


class VariablesForMidstream(models.Model):
    variableid = models.BigIntegerField(primary_key=True)
    variablecode = models.CharField(max_length=20)
    variablename = models.CharField(max_length=20)
    variableunitsid = models.ForeignKey(UnitsForMidstream, related_name='variableunitsid')
    methodid = models.ForeignKey(MethodsForMidstream)
    mediumtype = models.CharField(max_length=20)
    timesupportvalue = models.FloatField()
    timesupportunitsid = models.ForeignKey(UnitsForMidstream, related_name='timesupportunitsid')
    datatype = models.CharField(max_length=20)
    generalcategory = models.CharField(max_length=20)

    class Meta:
        verbose_name_plural = "Variables"

    def __unicode__(self):
        return self.variablecode


class SitesForMidstream(models.Model):
    siteid = models.BigIntegerField(primary_key=True)
    sitecode = models.CharField(max_length=20)
    sitename = models.CharField(max_length=60)
    latitude = models.FloatField()
    longitude = models.FloatField()
    elevation_m = models.FloatField()
    spatialreference = models.CharField(max_length=60)

    class Meta:
        verbose_name_plural = "Sites"

    def __unicode__(self):
        return self.sitecode


class SeriesForMidstream(models.Model):
    seriesid = models.BigIntegerField(primary_key=True)
    tablename = models.CharField(max_length=12)
    tablecolumnname = models.CharField(max_length=20)
    datetimeseriesstart = models.DateTimeField()
    datetimeseriesend = models.DateTimeField()
    siteid = models.ForeignKey('SitesForMidstream')
    variableid = models.ForeignKey('VariablesForMidstream')
    aqtimeseriesid = models.BigIntegerField()

    class Meta:
        verbose_name_plural = "Series"

    def __unicode__(self):
        return "id: %s, table: %s, column: %s" % (self.seriesid, self.tablename, self.tablecolumnname)



class Sl099(models.Model):
    id = models.IntegerField(primary_key=True)
    date = models.DateTimeField()
    loggertime = models.IntegerField()
    ctddepth = models.FloatField()
    ctdtemp = models.FloatField()
    ctdcond = models.FloatField()
    turblow = models.FloatField()
    turbhigh = models.FloatField()
    dotempc = models.FloatField()
    dopercent = models.FloatField()
    doppm = models.FloatField()
    boardtemp = models.FloatField()
    battery = models.FloatField()

    class Meta:
        verbose_name_plural = "SL099"

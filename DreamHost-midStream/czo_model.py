import wof.models as wof_base

class Variable(wof_base.BaseVariable):
    # Properties common to all variables in this example
    SampleMedium = wof_base.SampleMediumTypes.SURFACE_WATER
    IsRegular = True
    ValueType = 'Field Observation'
    DataType = 'Continuous'
    NoDataValue = -9999
    TimeSupport = 15 # This defines the number of minutes between sensor readings
    TimeUnitsID = 1
    GeneralCategory = wof_base.GeneralCategoryTypes.HYDROLOGY
    TimeUnits = wof_base.BaseUnits()
    TimeUnits.UnitsID = 102
    TimeUnits.UnitsName = 'minute'
    TimeUnits.UnitsType = 'Time'
    TimeUnits.UnitsAbbreviation = 'min'
    csvColumn = 4

class Site(wof_base.BaseSite):
    # Properties common to all sites in this example
    LatLongDatum = wof_base.BaseSpatialReference() # for SRSID and SRSName
    State = 'Pennsylvania'


class Source(wof_base.BaseSource):
    SourceID = 21    # in the database, personid 4 is Anthony
    ContactName = 'Steve Hicks'
    Phone = '610-268-2153'
    Email = 'info@stroudcenter.org'
    Organization = 'Stroud Water Research Center'
    SourceLink = 'http://www.stroudcenter.org/'
    SourceDescription = 'Data collected by Stroud Water Research Center'
    Address = '970 Spencer Road'
    City = 'Avondale'
    State = 'PA'
    ZipCode = '19311'
    MetadataID = 1
    Metadata = wof_base.BaseMetadata()
    Metadata.MetadataID = MetadataID
    Metadata.TopicCategory = None
    Metadata.Title = 'Christina River Basin'
    Metadata.Abstract = 'Critical Zone Observatory'
    Metadata.ProfileVersion = None
    Metadata.MetadataLink = None
    

class Method(wof_base.BaseMethod):
    # Default method info
    MethodID = 1
    MethodDescription = 'Measured using an unknown instrument'
    

class QualityControlLevel(wof_base.BaseQualityControlLevel):
    # Only one in this example
    QualityControlLevelID = \
            wof_base.QualityControlLevelTypes['RAW_DATA'][1]
    QualityControlLevelCode = \
            wof_base.QualityControlLevelTypes['RAW_DATA'][0]


class DataValue(wof_base.BaseDataValue):
    # Properties common to all data values
    UTCOffset = -5
    CensorCode = 'nc'
    MethodID = 1    # Default MethodID
    SourceID = 4    # Default SourceID
    QualityControlLevelID = \
            wof_base.QualityControlLevelTypes['RAW_DATA'][1]
    QualityControlLevel = \
            wof_base.QualityControlLevelTypes['RAW_DATA'][0]


class Series(wof_base.BaseSeries):
    # Properties common to all series in this example
    BeginDateTime = '2008-01-01T00:00-06'
    EndDateTime = '2008-04-30T00:00-06'
    BeginDateTimeUTC = '2008-01-01T06:00Z'
    EndDateTimeUTC = '2008-04-30T06:00Z'
    ValueCount = 0 # default to no values

    qc_level = QualityControlLevel()
    QualityControlLevelID = qc_level.QualityControlLevelID
    QualityControlLevelCode = qc_level.QualityControlLevelCode

    Method = Method()
    MethodID = Method.MethodID
    MethodDescription = Method.MethodDescription

    Source = Source()
    SourceID = Source.SourceID
    Organization = Source.Organization
    SourceDescription = Source.SourceDescription

    Variable = None
    Site = None
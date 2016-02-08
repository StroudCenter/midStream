SELECT DISTINCT
	concat(DataSeries.sensorid,dataseries.variableid,dataseries.mediumtypeid,dataseries.datatypeid,dataseries.variableunitsid) as variablecode,
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
  concat(DataSeries.sensorid,dataseries.variableid,dataseries.mediumtypeid,dataseries.datatypeid,dataseries.variableunitsid) = '1702231196';
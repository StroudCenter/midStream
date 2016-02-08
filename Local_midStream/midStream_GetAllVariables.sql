SELECT
	concat(DataSeries.sensorid,dataseries.variableid,dataseries.mediumtypeid,dataseries.datatypeid,dataseries.variableunitsid) as variablecode,
	Variables.VariableName,
	DataSeries.VariableUnitsID,
	DataSeries.MethodID,
	MediumTypes.Term as mediumtype,
	DataSeries.TimeSupportValue,
	DataSeries.TimeSupportUnitsID,
	DataTypes.Term as datatype,
	GeneralCategories.Term as generalcategory
FROM Methods
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
	ON Methods.MethodID = DataSeries.MethodID
WHERE (((Deployments.IsRealTime)=True) AND 
    (Deployments.DeploymentEndDateTime is NULL));
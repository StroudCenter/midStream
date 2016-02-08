SELECT DISTINCT
	concat(DataSeries.sensorid,dataseries.variableid,dataseries.mediumtypeid,dataseries.datatypeid,dataseries.variableunitsid) AS variablecode,
	Sites.SiteNameCode
FROM Sites
	RIGHT JOIN DataSeries
     RIGHT JOIN Deployments
     ON DataSeries.DeploymentID = Deployments.DeploymentID
	ON Sites.SiteID = DataSeries.SiteID
WHERE (((Deployments.IsRealTime)=True) AND 
    (Deployments.DeploymentEndDateTime is NULL) AND
  Sites.SiteNameCode = 'WCC000');
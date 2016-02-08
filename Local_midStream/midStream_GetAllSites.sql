SELECT DISTINCT
	Sites.SiteID,
	Sites.SiteNameCode,
	Sites.SiteFullName,
	Sites.Latitude,
	Sites.Longitude,
	Sites.Elevation,
	SpatialReferences.SRSID
FROM SpatialReferences
	RIGHT JOIN (Deployments
		RIGHT JOIN (Sites
			RIGHT JOIN DataSeries
			ON Sites.SiteID = DataSeries.SiteID)
		ON Deployments.DeploymentID = DataSeries.DeploymentID)
	ON SpatialReferences.SpatialReferenceID = Sites.SpatialReferenceID
WHERE (((Deployments.IsRealTime)=True) AND 
    (Deployments.DeploymentEndDateTime is NULL));

SELECT DISTINCT
	Sites.SiteID,
	Sites.SiteNameCode,
	Sites.SiteFullName,
	Sites.Latitude,
	Sites.Longitude,
	Sites.Elevation,
	SpatialReferences.SRSID
FROM SpatialReferences
	RIGHT JOIN (Sites
	ON SpatialReferences.SpatialReferenceID = Sites.SpatialReferenceID
WHERE sites.sitenamecode = %s;""", (site_code,))

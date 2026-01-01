SELECT
	DISTINCT n.reference_id,
	neo_name,
	absolute_magnitude_h,
	estimated_diameter_min_km,
	estimated_diameter_max_km,
	TIMEZONE('UTC', TO_TIMESTAMP(close_approach_date_epoch/1000)) AS close_approach_date_utc,
	relative_velocity_kms,
	miss_distance_km
FROM near_earth_objects AS n
JOIN close_approaches AS c
ON n.reference_id = c.reference_id
WHERE is_potentially_hazardous = true
ORDER BY close_approach_date_utc
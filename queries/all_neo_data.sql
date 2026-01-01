SET search_path TO nasa_neows;

SELECT
	n.reference_id,
	neo_name,
	nasa_jpl_url,
	absolute_magnitude_h,
	estimated_diameter_min_km,
	estimated_diameter_max_km,
	is_potentially_hazardous,
	is_sentry_object,
	close_approach_date_stamp AT TIME ZONE 'UTC' AS close_approach_date,
	relative_velocity_kms,
	miss_distance_km,
	o.body
FROM near_earth_objects as n
JOIN close_approaches as c
ON n.reference_id = c.reference_id
JOIN orbiting_bodies as o
ON c.orbiting_body = o.id
ORDER BY close_approach_date;
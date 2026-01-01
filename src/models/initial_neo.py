from pydantic import BaseModel

class InitialNEO(BaseModel):
    reference_id: int
    neo_name: str
    nasa_jpl_url: str
    absolute_magnitude_h: float
    estimated_diameter_min_km: float
    estimated_diameter_max_km: float
    is_potentially_hazardous: bool
    close_approaches: list[dict]
    is_sentry_object: bool
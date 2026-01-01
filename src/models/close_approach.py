from pydantic import BaseModel

class CloseApproach(BaseModel):
    reference_id: int
    close_approach_date_epoch: int
    relative_velocity_kms: float
    miss_distance_km: float
    orbiting_body: str
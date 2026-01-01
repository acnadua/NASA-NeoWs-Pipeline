from src.models.initial_neo import InitialNEO
from src.utils.logger import logger

def extract_neo(data: dict | None) -> dict:
    if not data:
        logger.warning("No data to transform.")
        return {}
    
    flattened_neo = {
        "neo_count": data["element_count"],
        "neo": []
    }

    for _, neo_list in data["near_earth_objects"].items():
        for neo in neo_list:
            flattened_neo["neo"].append(InitialNEO(
                reference_id=int(neo["neo_reference_id"]),
                neo_name=neo["name"],
                nasa_jpl_url=neo["nasa_jpl_url"],
                absolute_magnitude_h=neo["absolute_magnitude_h"],
                estimated_diameter_min_km=neo["estimated_diameter"]["kilometers"]["estimated_diameter_min"],
                estimated_diameter_max_km=neo["estimated_diameter"]["kilometers"]["estimated_diameter_max"],
                is_potentially_hazardous=neo["is_potentially_hazardous_asteroid"],
                close_approaches=neo["close_approach_data"],
                is_sentry_object=neo["is_sentry_object"]
            ).model_dump())

    logger.info("Successfully flattened NEO data, except for close approach data.")    
    return flattened_neo
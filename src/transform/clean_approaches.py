from src.models.close_approach import CloseApproach

def get_new_neo_approaches(data: dict) -> tuple[dict, list[dict]]:
    close_approaches = []
    for neo in data["neo"]:
        neo_id = neo["reference_id"]
        approaches = neo.pop("close_approaches", [])
        close_approaches.extend(
            _extract_close_approaches(neo_id, approaches)
        )

    return data, close_approaches


def _extract_close_approaches(neo_id: int, approaches: list) -> list[dict]:
    close_approaches = []

    for approach in approaches:
        close_approaches.append(CloseApproach(
            reference_id=neo_id,
            close_approach_date_epoch=approach["epoch_date_close_approach"],
            relative_velocity_kms=float(approach["relative_velocity"]["kilometers_per_second"]),
            miss_distance_km=float(approach["miss_distance"]["kilometers"]),
            orbiting_body=approach["orbiting_body"]
        ).model_dump())

    return close_approaches
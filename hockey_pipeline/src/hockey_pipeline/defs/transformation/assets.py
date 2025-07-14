from dagster import asset, ResourceParam
from sqlalchemy import Engine, text

@asset(
    deps=["game_events"],
    key=["transformed", "score_adjusted_events"]
)
def score_adjusted_events(hockeydb: ResourceParam[Engine]) -> None:
    with hockeydb.connect() as conn:
        with open("AddScoresTransform.sql") as file:
            query = text(file.read())
            conn.execute(query)
    return
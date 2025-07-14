from dagster import asset, ResourceParam
from sqlalchemy import Engine, text
from pathlib import Path
import sqlparse

# Helpers
def run_sql_script(hockeydb: ResourceParam[Engine], file_name: Path):
    with open(file_name) as file:
        statements = sqlparse.split(sqlparse.format(file.read(), strip_comments=True))
    with hockeydb.connect() as conn:
        for statement in statements:
            if statement.strip():
                conn.execute(text(statement))

@asset(
    pool="sqlite_write_pool",
    deps=["game_events"],
    key=["transformed", "score_adjusted_events"]
)
def score_adjusted_events(hockeydb: ResourceParam[Engine]) -> None:
    with hockeydb.connect() as conn:
        file_name = Path(__file__).parent / "AddScoresTransform.sql"
        with open(file_name) as file:
            query = text(file.read())
            conn.execute(query)
    return

@asset(
    pool="sqlite_write_pool",
    deps=["score_adjusted_events"],
    key=["transformed", "view", "shot_events"]
)
def shot_events(hockeydb: ResourceParam[Engine]) -> None:
    file_name = Path(__file__).parent / "shot_events.sql"
    run_sql_script(hockeydb, file_name)

@asset(
    pool="sqlite_write_pool",
    deps=["shot_events"],
    key=["transformed", "view", "goalie_save_pct"]
)
def goalie_save_pct(hockeydb: ResourceParam[Engine]) -> None:
    file_name = Path(__file__).parent / "GoalieLifetimeSavePct.sql"
    run_sql_script(hockeydb, file_name)

@asset(
    pool="sqlite_write_pool",
    deps=["shot_events", "teams"],
    key=["transformed", "view", "per_game_fenwick"]
)
def per_game_fenwick(hockeydb: ResourceParam[Engine]) -> None:
    file_name = Path(__file__).parent / "PerTeamGameFenwickAndScore.sql"
    run_sql_script(hockeydb, file_name)

@asset(
    pool="sqlite_write_pool",
    deps=["shot_events"],
    key=["transformed", "table", "fenwick_coeffs"]
)
def fenwick_coeffs(hockeydb: ResourceParam[Engine]) -> None:
    file_name = Path(__file__).parent / "AdjustedFenwick.sql"
    run_sql_script(hockeydb, file_name)
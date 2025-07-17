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
    key=["transformed", "score_adjusted_events"],
    description="Event results only show score for goals. This script populates new columns that detail the current score at the time of each event, based off of the most recent goal before that event."
)
def score_adjusted_events(hockeydb: ResourceParam[Engine]) -> None:
    file_name = Path(__file__).parent / "AddScoresTransform.sql"
    run_sql_script(hockeydb, file_name)
    return

@asset(
    pool="sqlite_write_pool",
    deps=["score_adjusted_events"],
    key=["transformed", "view", "shot_events"],
    description="Regular and playoff shot events, (excluding shoot-out), for all seasons 20102011 and after."
)
def shot_events(hockeydb: ResourceParam[Engine]) -> None:
    file_name = Path(__file__).parent / "shot_events.sql"
    run_sql_script(hockeydb, file_name)

@asset(
    pool="sqlite_write_pool",
    deps=["shot_events"],
    key=["transformed", "view", "goalie_save_pct"],
    description="Regular season 5v5 goals per shot-on-goal per goalie, diluted by adding 82 goals and 1000 shots to each goalie."
)
def goalie_save_pct(hockeydb: ResourceParam[Engine]) -> None:
    file_name = Path(__file__).parent / "GoalieLifetimeSavePct.sql"
    run_sql_script(hockeydb, file_name)

@asset(
    pool="sqlite_write_pool",
    deps=["shot_events", "teams", "game_toi", "fenwick_coeffs"],
    key=["transformed", "view", "per_game_fenwick"],
    description="Regular season per game fenwick (unblocked shots) totals, both raw and adjusted for score/venue"
)
def per_game_fenwick(hockeydb: ResourceParam[Engine]) -> None:
    file_name = Path(__file__).parent / "PerTeamGameFenwickAndScore.sql"
    run_sql_script(hockeydb, file_name)

@asset(
    pool="sqlite_write_pool",
    deps=["shot_events"],
    key=["transformed", "table", "fenwick_coeffs"],
    description="Adjustment multipliers by score differential for Fenwick."
)
def fenwick_coeffs(hockeydb: ResourceParam[Engine]) -> None:
    file_name = Path(__file__).parent / "AdjustedFenwick.sql"
    run_sql_script(hockeydb, file_name)

@asset(
    pool="sqlite_write_pool",
    deps=["per_game_fenwick"],
    key=["transformed", "view", "last25", "unblocked_shot_gen_sup_last25"],
    description="Regular season unblocked shots for and against (Fenwick), adjusted for score/venue, both totals and per hour, averaged over the past 25 games in that season."
)
def unblocked_shot_gen_sup_last25(hockeydb: ResourceParam[Engine]) -> None:
    file_name = Path(__file__).parent / "UnblockedShotGenSupLast25.sql"
    run_sql_script(hockeydb, file_name)

@asset(
    pool="sqlite_write_pool",
    deps=["shot_events", "teams", "game_toi"],
    key=["transformed", "view", "shot_gen_sup_5v4"],
    description="Regular season shots for at 5v4, against at 4v5, raw and per hour."
)
def shot_gen_sup_5v4(hockeydb: ResourceParam[Engine]) -> None:
    file_name = Path(__file__).parent / "ShotGenSup5v4.sql"
    run_sql_script(hockeydb, file_name)

@asset(
    pool="sqlite_write_pool",
    deps=["shot_gen_sup_5v4"],
    key=["transformed", "view", "last25", "shot_gen_sup_5v4_last25"],
    description="Regular season shots for at 5v4, against at 4v5, per hour averaged over the last 25 games."
)
def shot_gen_sup_5v4_last25(hockeydb: ResourceParam[Engine]) -> None:
    file_name = Path(__file__).parent / "ShotGenSup5v4Last25.sql"
    run_sql_script(hockeydb, file_name)

@asset(
    pool="sqlite_write_pool",
    deps=["shot_events"],
    key=["transformed", "view", "team_scoring_percentage"]
)
def team_scoring_percentage(hockeydb: ResourceParam[Engine]) -> None:
    file_name = Path(__file__).parent / "TeamScoringPercentage.sql"
    run_sql_script(hockeydb, file_name)
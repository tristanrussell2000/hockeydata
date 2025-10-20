from dagster import asset, ResourceParam, AssetKey
from sqlalchemy import Engine
import pandas as pd

@asset(
    pool="sqlite_write_pool",
    deps=[
        "shot_gen_sup_5v4_last25", 
        "unblocked_shot_gen_sup_last25",
        "team_scoring_percentage",
        "goalie_save_pct",
        AssetKey(["main", "games"])
    ],
    key_prefix=["model", "oscar"],
    description="Fill training dataset for the Oscar model. https://hockeyviz.com/txt/oscar"
)
def training_data(hockeydb: ResourceParam[Engine]):
    unblocked_query = """
    WITH GoalieIdJoin AS (
    SELECT shots.gameId,
           shots.season,
           shots.TeamId,
           shots.TeamName,
           shots.isHomeTeam,
           shots.Prev25FenwickForPerHour,
           shots.Prev25FenwickAgainstPerHour,
           pp.Prev25ShotsFor5v4PerHour,
           pp.Prev25ShotsAgainst4v5PerHour,
           spct.Prev25ShootingPercentage,
           playerId as GoalieId
    FROM UnblockedShotGenSupLast25 AS shots
    LEFT JOIN goalie_saves as gs ON shots.gameId = gs.gameId AND
                                 ((shots.isHomeTeam = 1 AND gs.homeRoad = 'H') OR
                                  (shots.isHomeTeam = 0 AND gs.homeRoad = 'R')) and
                                 gs.gamesStarted = 1
    LEFT JOIN ShotGenSup5v4Last25 as pp
           ON pp.id = shots.gameId AND pp.teamId = shots.TeamId
    LEFT JOIN TeamScoringPercentage as spct
           ON spct.id = shots.gameId AND spct.teamId = shots.TeamId)
    SELECT GoalieIdJoin.*,
         gsave.diluted_save_pct,
         games.homeScore > games.visitingScore AS HomeTeamWin
    FROM GoalieIdJoin
           LEFT JOIN GoalieSavePct as gsave ON gsave.goalieId = GoalieIdJoin.GoalieId
           LEFT JOIN games ON games.id = GoalieIdJoin.gameId \
    """
    df = pd.read_sql_query(unblocked_query, hockeydb)

    # Place home and away data on same row
    df_home = df[df["isHomeTeam"] == 1].rename(columns={
        "TeamName": "HomeTeamName",
        "TeamId": "HomeTeamId",
        "Prev25FenwickForPerHour": "HomePrev25FenwickForPerHour",
        "Prev25FenwickAgainstPerHour": "HomePrev25FenwickAgainstPerHour",
        "Prev25ShotsFor5v4PerHour": "HomePrev25ShotsFor5v4PerHour",
        "Prev25ShotsAgainst4v5PerHour": "HomePrev25ShotsAgainst4v5PerHour",
        "Prev25ShootingPercentage": "HomePrev25ShootingPercentage",
        "GoalieId": "HomeGoalieId",
        "diluted_save_pct": "HomeDilutedSavePct"
    }).drop(["isHomeTeam"], axis=1)
    df_away = df[df["isHomeTeam"] == 0].rename(columns={
        "TeamName": "AwayTeamName",
        "TeamId": "AwayTeamId",
        "Prev25FenwickForPerHour": "AwayPrev25FenwickForPerHour",
        "Prev25FenwickAgainstPerHour": "AwayPrev25FenwickAgainstPerHour",
        "Prev25ShotsFor5v4PerHour": "AwayPrev25ShotsFor5v4PerHour",
        "Prev25ShotsAgainst4v5PerHour": "AwayPrev25ShotsAgainst4v5PerHour",
        "Prev25ShootingPercentage": "AwayPrev25ShootingPercentage",
        "GoalieId": "AwayGoalieId",
        "diluted_save_pct": "AwayDilutedSavePct"
    }).drop(["season", "isHomeTeam", "HomeTeamWin"], axis=1)

    # Drop rows with no data (e.g. where either team hadn't played games yet that season, so the last 25 game data is null)
    df_com = pd.merge(left=df_home, right=df_away, how="left", on="gameId")
    df_nn = df_com.loc[df_com["HomePrev25FenwickAgainstPerHour"].notna() & df_com["AwayPrev25FenwickForPerHour"].notna()].copy()
    df_nn.dropna(inplace=True)

    # Standardize row data within each season
    df_std = df_nn.copy()
    cols_to_standardize = ["HomePrev25FenwickForPerHour", "HomePrev25FenwickAgainstPerHour", "HomePrev25ShotsFor5v4PerHour", "HomePrev25ShotsAgainst4v5PerHour", "HomePrev25ShootingPercentage", "HomeDilutedSavePct", "AwayPrev25FenwickForPerHour", "AwayPrev25FenwickAgainstPerHour", "AwayPrev25ShotsFor5v4PerHour", "AwayPrev25ShotsAgainst4v5PerHour", "AwayPrev25ShootingPercentage", "AwayDilutedSavePct"]
    df_std[cols_to_standardize] = df_std.groupby('season')[cols_to_standardize].transform(
        lambda x: (x - x.mean()) / x.std()
    )
    return df_std
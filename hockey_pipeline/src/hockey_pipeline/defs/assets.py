import os
from pathlib import Path
import pandas as pd
import requests
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text, inspect, Engine

from dagster import asset, get_dagster_logger, ResourceParam

# --- Configuration ---
NHL_API_BASE_URL = "https://api-web.nhle.com/v1"
NHL_STATS_API_BASE_URL = "https://api.nhle.com/stats/rest/en"

logger = get_dagster_logger()


# --- Helper Functions ---
def get_max_date_from_db(dbengine, table_name, date_col):
    """Gets the most recent date from a table to enable incremental loads."""
    if not inspect(dbengine).has_table(table_name):
        return datetime(1917, 1, 1).date() # NHL's first season
    with dbengine.connect() as conn:
        result = conn.execute(text(f"SELECT MAX({date_col}) FROM {table_name}")).scalar()
        if result is None:
            return datetime(1917, 1, 1).date()
        max_date = pd.to_datetime(result)
    return max_date.date() if pd.notna(max_date) else datetime(1917, 1, 1).date()

def flatten_event_json(event: dict) -> dict:
    """
    Flattens specified keys in a game dictionary.
    'details.assist2PlayerId' becomes 'assist2PlayerId'.
    """
    keys_to_flatten = {
        'details.assist2PlayerId', 'typeCode', 'details.servedByPlayerId',
        'details.secondaryReason', 'details.eventOwnerTeamId', 'details.yCoord', 'details.awaySOG',
        'details.committedByPlayerId', 'details.hittingPlayerId', 'details.drawnByPlayerId',
        'sortOrder', 'details.blockingPlayerId', 'details.hitteePlayerId', 'homeTeamDefendingSide',
        'details.homeScore', 'timeInPeriod', 'periodDescriptor.maxRegulationPeriods', 'details.playerId',
        'situationCode', 'details.winningPlayerId', 'details.goalieInNetId', 'details.duration',
        'details.reason', 'details.awayScore', 'details.shootingPlayerId',
        'details.assist2PlayerTotal', 'timeRemaining', 'details.zoneCode', 'details.losingPlayerId',
        'details.assist1PlayerId', 'details.xCoord', 'details.homeSOG', 'periodDescriptor.periodType',
        'details.scoringPlayerId', 'details.shotType', 'details.descKey', 'eventId',
        'typeDescKey', 'details.assist1PlayerTotal'
    }
    
    flattened_event = {}
    for key in keys_to_flatten:
        if '.' in key:
            # Handle nested keys like 'details.assist2PlayerId'
            parts = key.split('.')
            current_value = event
            for part in parts:
                if isinstance(current_value, dict) and part in current_value:
                    current_value = current_value[part]
                else:
                    current_value = None
                    break
            if current_value is not None:
                flattened_event[parts[-1]] = current_value
        else:
            # Handle top-level keys like 'typeCode'
            if key in event:
                flattened_event[key] = event[key]
    if "details" in event and "typeCode" in event["details"]:
        flattened_event["detailsTypeCode"] = event['details']['typeCode']
    if "periodDescriptor" in event and "number" in event["periodDescriptor"]:
        flattened_event["periodNumber"] = event["periodDescriptor"]["number"]
    return flattened_event


@asset(
    pool="sqlite_write_pool",
    key_prefix=["main"],
    code_version="0.1.0"
)
def games(hockeydb: ResourceParam[Engine]) -> None:
    """
    Fetches game data from the NHL API.
    Loads incrementally, but re-processes the last 7 days of data for revisions.
    Also loads the next 7 days of data into the future
    """
    # Logic to fetch only new/recent games
    max_date_in_db = get_max_date_from_db(hockeydb, "games", "gameDate")
    start_date = max_date_in_db - timedelta(days=7)
    # Load games 1 week into future
    end_date = datetime.now().date() + timedelta(days=7)

    logger.info(f"Fetching games from {start_date} to {end_date}")
    
    url = f"{NHL_STATS_API_BASE_URL}/game?cayenneExp=gameDate>=\"{start_date}\" and gameDate<=\"{end_date}\""
    logger.info(url)
    response = requests.get(url).json()
    if "message" in response.keys():
        logger.error(response["message"])
        return

    all_games = [game for game in response["data"]]

    df = pd.DataFrame(all_games)
    df['gameDate'] = pd.to_datetime(df['gameDate'])

    # Write to DB, replacing games in the 7-day window
    with hockeydb.connect() as conn:
        # Use a transaction to delete and insert atomically
        with conn.begin():
            if inspect(hockeydb).has_table("games"):
                conn.execute(text(f"DELETE FROM games WHERE gameDate >= '{start_date}'"))
            df.to_sql("games", conn, if_exists="append", index=False)
    logger.info(f"Wrote {len(df)} records to the 'games' table.")

@asset(
    pool="sqlite_write_pool",
    key_prefix=["main"]
)
def teams(hockeydb: ResourceParam[Engine]) -> None:
    url = f"{NHL_STATS_API_BASE_URL}/team"
    response = requests.get(url).json()

    if "data" not in response or not response["data"]:
        logger.error("Couldn't load team data or data is empty")
        return
    
    teams_df = pd.DataFrame(response["data"])
    teams_df.to_sql("teams", hockeydb, if_exists="replace", index=False)
    logger.info(f"Wrote {len(teams_df)} records to the 'teams' table")

@asset(
    pool="sqlite_write_pool",
    deps=["games"],
    key_prefix=["main"],
    description="Time on ice on PK, PP, etc for each game"
)
def game_toi(hockeydb: ResourceParam[Engine]) -> None:
    # Fetch game dates for the games to fetch TOI for
    with hockeydb.connect() as conn:
        games_with_dates_df = pd.read_sql("""
            SELECT id, gameDate, season FROM games
            WHERE id IN (SELECT id FROM games as g LEFT JOIN pk_pp_toi AS toi ON g.id = toi.gameId WHERE toi.gameId IS NULL AND g.season >= 20002001 AND g.gameType IN (2, 3))
            ORDER BY gameDate ASC
        """, conn)

    if games_with_dates_df.empty:
        logger.info("No games with dates to process for TOI.")
        return

    games_with_dates_df['gameDate'] = pd.to_datetime(games_with_dates_df['gameDate'])
    season_date_ranges = games_with_dates_df.groupby('season')['gameDate'].agg(['min', 'max']).reset_index()

    for _, row in season_date_ranges.iterrows():
        season = row['season']
        first_date = row['min'].strftime('%Y-%m-%d')
        last_date = row['max'].strftime('%Y-%m-%d')
        logger.info(f"Season: {season}, First Date: {first_date}, Last Date: {last_date}")
        
        url_pk = f"{NHL_STATS_API_BASE_URL}/team/penaltykilltime?limit=-1&isGame=true&cayenneExp=gameDate>=\"{first_date}\" and gameDate<=\"{last_date}\""
        url_pp = f"{NHL_STATS_API_BASE_URL}/team/powerplaytime?limit=-1&isGame=true&cayenneExp=gameDate>=\"{first_date}\" and gameDate<=\"{last_date}\""
        url_real = f"{NHL_STATS_API_BASE_URL}/team/realtime?limit=-1&isGame=true&cayenneExp=gameDate>=\"{first_date}\" and gameDate<=\"{last_date}\""
        toi_pk = requests.get(url_pk).json().get("data")
        toi_pp = requests.get(url_pp).json().get("data")
        toi_real = requests.get(url_real).json().get("data")
        if toi_pk is None or toi_pp is None or toi_real is None or len(toi_pk) == 0 or len(toi_pp) == 0 or len(toi_real) == 0:
            logger.warning("No pk/pp/real toi data for this season")
            continue

        pk_df = pd.DataFrame(toi_pk)
        pp_df = pd.DataFrame(toi_pp)
        real_df = pd.DataFrame(toi_real)
        cols_to_keep = ["gameId", "teamId", "gameDate_x", "homeRoad_x", "timeOnIce3v4", "timeOnIce3v5", "timeOnIce4v5", "timeOnIceShorthanded", "timeOnIce4v3", "timeOnIce5v3", "timeOnIce5v4", "timeOnIcePp", "timeOnIcePerGame5v5"]
        toi_all = pk_df.merge(pp_df, on=["gameId", "teamId"], how="inner").merge(real_df, on=["gameId", "teamId"], how="inner")[cols_to_keep].rename(columns={"gameDate_x": "gameDate", "homeRoad_x": "homeRoad"})
        toi_all.to_sql("pk_pp_toi", hockeydb, if_exists="append", index=False)

@asset(
    pool="sqlite_write_pool",
    deps=["games"],
    key_prefix=["main"],
    description="Per game stats for goalies. Uses goalie/savesByStrength stat api. Crucially, this tells whether goalie was the starter for that game, useful for Oscar and future models."
)
def goalie_saves(hockeydb: ResourceParam[Engine]) -> None:
    with hockeydb.connect() as conn:
        games_with_dates_df = pd.read_sql("""
            SELECT id, gameDate, season FROM games
            WHERE id IN (SELECT id FROM games as g LEFT JOIN goalie_saves as saves on g.id = saves.gameId WHERE saves.gameId IS NULL AND g.season >=20002001 AND g.gameType IN (2, 3))
            ORDER BY gameDate ASC                                 
        """, conn)

        if games_with_dates_df.empty:
            logger.info("No games with dates to process for goalie saves.")
            return
        
        games_with_dates_df['gameDate'] = pd.to_datetime(games_with_dates_df['gameDate'])
        season_date_ranges = games_with_dates_df.groupby('season')['id'].agg(['min', 'max']).reset_index()

        for _, row in season_date_ranges.iterrows():
            season = row['season']
            first_game_id = row['min']
            last_game_id = row['max']
            logger.info(f"Season: {season}, First Game: {first_game_id}, Last Game: {last_game_id}")

            url = f"{NHL_STATS_API_BASE_URL}/goalie/savesByStrength?limit=-1&isGame=True&cayenneExp=gameId>=\"{first_game_id}\" and gameId<=\"{last_game_id}\""
            goalie_saves = requests.get(url).json().get("data")
            if goalie_saves is None or len(goalie_saves) == 0:
                logger.warning("No goalie save data for this season")
                continue

            gs_df = pd.DataFrame(goalie_saves)
            gs_df.to_sql("goalie_saves", hockeydb, if_exists="append", index=False)

@asset(
    pool="sqlite_write_pool",
    deps=["games"],
    key_prefix=["main"],
    description="Raw event data"
    ) # This asset depends on the 'games' asset
def game_events(hockeydb: ResourceParam[Engine]) -> None:
    """
    For each new game in the 'games' table, fetch its event data.
    """
    with hockeydb.connect() as conn:
        # Get games we have in our DB but not in the events table yet
        games_to_fetch_df = pd.read_sql("""
            SELECT g.id FROM games AS g
            LEFT JOIN events AS e ON g.id = e.gameId
            WHERE e.gameId IS NULL AND g.id > 2000020067
            ORDER BY g.id DESC
        """, conn)

    if games_to_fetch_df.empty:
        logger.info("No new games to fetch events for.")
        return

    for game_id in games_to_fetch_df["id"]:
        logger.info(f"Fetching events for gameid: {game_id}")
        game_specific_events = []
        url = f"{NHL_API_BASE_URL}/gamecenter/{game_id}/play-by-play"
        response = requests.get(url)
        
        if response.status_code != 200:
            logger.warning(f"Game ID {game_id} not found {response.status_code}. Inserting dummy event.")
            game_specific_events.append({"gameid": game_id, "typeDescKey": "dummy"})
        else:
            try:
                response_json = response.json()
                if "plays" in response_json:
                    if not response_json["plays"]:
                        logger.info(f"{game_id} has no events")
                        # Insert dummy event to avoid fetching events for this game again
                        game_specific_events.append({"gameid": game_id, "typeDescKey": "dummy"})
                    else:
                        for play in response_json["plays"]:
                            flattened_play = flatten_event_json(play)
                            flattened_play["gameid"] = game_id
                            game_specific_events.append(flattened_play)
                else:
                    logger.warning(f"No 'plays' key in response for game ID {game_id}. Response: {response_json}")
                    # Insert dummy event if 'plays' key is missing to avoid refetching
                    game_specific_events.append({"gameid": game_id, "typeDescKey": "dummy"})
            except requests.exceptions.JSONDecodeError:
                logger.error(f"Failed to decode JSON for game ID {game_id}. Response content: {response.text[:200]}...")
                # Insert dummy event if JSON decoding fails to avoid refetching
                game_specific_events.append({"gameid": game_id, "typeDescKey": "dummy"})
        
        if game_specific_events:
            df_game_events = pd.DataFrame(game_specific_events)
            df_game_events.to_sql("events", hockeydb, if_exists="append", index=False)
            logger.info(f"Wrote {len(df_game_events)} events for gameid {game_id} to the 'game_events' table.")
        else:
            logger.info(f"No events (not even dummy) to write for gameid {game_id}.")

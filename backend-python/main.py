from typing import Annotated, Sequence, List
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import os
from pathlib import Path
from pydantic import BaseModel
import requests

BASE_DIR = os.path.dirname(Path(__file__).parent)
DATABASE_DIR = os.path.join(BASE_DIR, "db")
DATABASE_FILE_PATH = os.path.join(DATABASE_DIR, "game-data.sqlite")
NHL_API_BASE_URL = "https://api-web.nhle.com/v1"

sqlite_url = f"sqlite:///{DATABASE_FILE_PATH}"

class Team(BaseModel):
    id: int
    franchiseId: int
    fullName: str
    triCode: str

def get_cursor():
    conn = sqlite3.connect(DATABASE_FILE_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn.cursor()
    finally:
        conn.close()

CursorDep = Annotated[sqlite3.Cursor, Depends(get_cursor)]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
)

@app.get("/teams/")
def read_teams(
    cursor: CursorDep,
) -> List[Team]:
    standings = requests.get(f"{NHL_API_BASE_URL}/standings/now").json()
    print(standings)
    team_abbrevs = [team["teamAbbrev"]["default"] for team in standings["standings"]]
    
    # Create placeholders for SQL IN clause
    placeholders = ','.join('?' * len(team_abbrevs))
    
    result = cursor.execute(f"""
    SELECT * FROM teams
    WHERE triCode IN ({placeholders})
    """, team_abbrevs)
    
    teams = [Team(**dict(row)) for row in result.fetchall()]
    return teams

@app.get("/next_games/")
def read_next_games():
    pass

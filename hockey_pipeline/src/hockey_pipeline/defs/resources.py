from dagster import definitions, Definitions
from sqlalchemy import create_engine, text, inspect
import os
from pathlib import Path

BASE_DIR = os.path.dirname(Path(__file__).parent.parent.parent.parent)
DATABASE_DIR = os.path.join(BASE_DIR, "db")
DATABASE_FILE_PATH = os.path.join(DATABASE_DIR, "game-data.sqlite")
engine = create_engine(f"sqlite:///{DATABASE_FILE_PATH}")

@definitions
def resources():
    return Definitions(
        resources={"hockeydb": engine},
    )
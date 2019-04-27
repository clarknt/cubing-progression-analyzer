import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from cubingpa.config import db_config
from cubingpa.raw_data import RawData


def load() -> RawData:
    """
    Load raw SQL tables

    Returns
    -------
    RawData
    """
    engine = _get_db_engine()
    results = _get_raw_results(engine)
    competitions = _get_raw_competitions(engine)
    return RawData(results, competitions)


def _get_db_engine() -> Engine:
    return create_engine(f'{db_config.protocol}://{db_config.login}:{db_config.password}@{db_config.host}:{db_config.port}/{db_config.name}', echo=False)


def _get_raw_results(db_engine: Engine) -> DataFrame:
    # read the whole table without SQL filtering
    # pandas filtering is faster, and it allows reusing the same mechanisms for csv input
    results_query = "SELECT personId, eventId, best, competitionId FROM Results"
    
    return pd.read_sql_query(results_query, db_engine)


def _get_raw_competitions(db_engine: Engine) -> DataFrame:
    # read the whole table without SQL filtering
    # pandas filtering is faster, and it allows reusing the same mechanisms for csv input
    competitions_query = "SELECT id, YEAR, MONTH, DAY FROM Competitions"
    
    return pd.read_sql_query(competitions_query, db_engine)

# external imports
import pandas as pd
from sqlalchemy import create_engine

# internal imports
from config import db_config
from events import EventsId


def get_filtered_results_from_db(eventId):
    engine = get_db_engine()
    results = get_raw_results(engine)
    competitions = get_raw_competitions(engine)
    
    return filter_raw_dataframes(results, competitions, eventId)


def get_db_engine():
    return create_engine(f'{db_config.protocol}://{db_config.login}:{db_config.password}@{db_config.host}:{db_config.port}/{db_config.name}', echo=False)


def get_raw_results(db_engine):
    # read the whole table without SQL filtering
    # pandas filtering is faster, and it allows reusing the same mechanisms for csv input
    results_query = "SELECT personId, eventId, best, competitionId FROM Results"
    
    return pd.read_sql_query(results_query, db_engine)


def get_raw_competitions(db_engine):
    # read the whole table without SQL filtering
    # pandas filtering is faster, and it allows reusing the same mechanisms for csv input
    competitions_query = "SELECT id, YEAR, MONTH, DAY FROM Competitions"
    competitions = pd.read_sql_query(competitions_query, db_engine)
    competitions.rename(columns={'id': 'competitionId'}, inplace=True)
    
    return competitions


def filter_raw_dataframes(results, competitions, eventId):
    # simulate SQL filtering and joining

    # filter on event
    results = results[results['eventId'] == eventId.value]
    results = results.drop('eventId', axis = 1)

    # remove invalid results
    results = results[results['best'] != -1]

    # remove persons with less than 2 results
    results = remove_insufficient_results(results, 2)

    # convert results to seconds
    # enven though floats take more memory than integers it won't matter
    # because interpolating data will make float columns anyway
    results['best'] = results['best'] / 100

    # join results with competitions
    results = merge_results_and_competitions(results, competitions)

    return results


def remove_insufficient_results(results, minimum_results_per_person):
    # count each person's number of occurences
    persons_counts = results['personId'].value_counts()

    # get indexes of persons appearing less than twice
    persons_to_remove = persons_counts[persons_counts < minimum_results_per_person].index

    # remove said indexes from the dataframe
    results = results[~results['personId'].isin(persons_to_remove)]

    return results


def merge_results_and_competitions(results, competitions):
    # equivalent of SQL INNER JOIN (we don't do LEFT OUTER JOIN as we need existing dates)
    results = pd.merge(results, competitions, on='competitionId')
    results = results.drop('competitionId', axis = 1)

    # sort
    results = results.sort_values(by = ['personId', 'YEAR', 'MONTH', 'DAY'])

    # convert year, month, day columns to actual date
    results['date'] = pd.to_datetime(results[['YEAR', 'MONTH', 'DAY']])
    results = results.drop(columns=['YEAR', 'MONTH', 'DAY'])

    return results


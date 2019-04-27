import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine

from cubingpa.raw_data import RawData
from cubingpa.events import EventId


def filter(raw_data: RawData, event_id: EventId) -> DataFrame:
    """
    Filter, merge and organize raw data, retaining specified event only

    Parameters
    ----------
    raw_data: RawData
        Data as loaded from source (DB, CSV, etc)
    even_id: EventId
        Event to filter on

    Returns
    -------
    Dataframe
        Filtered data as a Dataframe
    """
    
    results = raw_data.results
    competitions = raw_data.competitions

    results = _filter_on_event(results, event_id)

    results = _remove_invalid_results(results)

    results = _remove_persons_with_insufficient_results(results, 2)

    results = _convert_results_to_seconds(results)

    results = _join_results_on_competitions(results, competitions)

    results = _sort_results(results)

    results = _convert_year_month_day_to_date(results)

    return results


def _filter_on_event(results: DataFrame, event_id: EventId) -> DataFrame:
    """
    Filter on event and drop unneeded eventId column
    """

    results = results[results['eventId'] == event_id.value]
    
    return results.drop('eventId', axis = 1)


def _remove_invalid_results(results: DataFrame) -> DataFrame:
    return results[results['best'] != -1]


def _convert_results_to_seconds(results: DataFrame) -> DataFrame:
    # enven though floats take more memory than integers it won't matter
    # because using NaN and interpolating data will make float columns anyway
    results['best'] = results['best'] / 100

    return results


def _remove_persons_with_insufficient_results(results: DataFrame, minimum_results_per_person: int) -> DataFrame:
    # count each person's number of occurences
    persons_counts = results['personId'].value_counts()

    # get indexes of persons appearing less than twice
    persons_to_remove = persons_counts[persons_counts < minimum_results_per_person].index

    # remove said indexes from the dataframe
    results = results[~results['personId'].isin(persons_to_remove)]

    return results


def _join_results_on_competitions(results: DataFrame, competitions: DataFrame) -> DataFrame:
    """
    Join results on competition and drop unneeded competitionId column
    """

    # set identical column name on both dataframes
    competitions = competitions.rename(columns={'id': 'competitionId'})

    # equivalent of SQL INNER JOIN (we don't do LEFT OUTER JOIN as we need existing dates)
    results = pd.merge(results, competitions, on='competitionId')
    
    return results.drop('competitionId', axis = 1)


def _sort_results(results: DataFrame) -> DataFrame:
    return results.sort_values(by = ['personId', 'YEAR', 'MONTH', 'DAY'])


def _convert_year_month_day_to_date(results: DataFrame) -> DataFrame:
    """
    Convert year, month and day to date and drop unneeded YEAR, MONTH, DAY columns
    """

    results['date'] = pd.to_datetime(results[['YEAR', 'MONTH', 'DAY']])
    
    return results.drop(columns=['YEAR', 'MONTH', 'DAY'])

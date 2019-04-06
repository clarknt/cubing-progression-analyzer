import pandas as pd
from sqlalchemy import create_engine

from cubingpa.events import EventId


def filter(raw_data, event_id):
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
    return _filter_raw_dataframes(raw_data.results, raw_data.competitions, event_id)


def _filter_raw_dataframes(results, competitions, event_id):
    # simulate SQL filtering and joining

    # filter on event
    results = results[results['eventId'] == event_id.value]
    results = results.drop('eventId', axis = 1)

    # remove invalid results
    results = results[results['best'] != -1]

    # remove persons with less than 2 results
    results = _remove_insufficient_results(results, 2)

    # convert results to seconds
    # enven though floats take more memory than integers it won't matter
    # because using NaN and interpolating data will make float columns anyway
    results['best'] = results['best'] / 100

    # join results with competitions
    results = _merge_results_and_competitions(results, competitions)

    return results


def _remove_insufficient_results(results, minimum_results_per_person):
    # count each person's number of occurences
    persons_counts = results['personId'].value_counts()

    # get indexes of persons appearing less than twice
    persons_to_remove = persons_counts[persons_counts < minimum_results_per_person].index

    # remove said indexes from the dataframe
    results = results[~results['personId'].isin(persons_to_remove)]

    return results


def _merge_results_and_competitions(results, competitions):
    # match competitions id name with the one from results
    competitions = competitions.rename(columns={'id': 'competitionId'})

    # equivalent of SQL INNER JOIN (we don't do LEFT OUTER JOIN as we need existing dates)
    results = pd.merge(results, competitions, on='competitionId')
    results = results.drop('competitionId', axis = 1)

    # sort
    results = results.sort_values(by = ['personId', 'YEAR', 'MONTH', 'DAY'])

    # convert year, month, day columns to actual date
    results['date'] = pd.to_datetime(results[['YEAR', 'MONTH', 'DAY']])
    results = results.drop(columns=['YEAR', 'MONTH', 'DAY'])

    return results

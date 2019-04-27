import math
import time
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta
from typing import List, Tuple, Any, cast
from pandas import DataFrame, Series

from cubingpa import utils


class ReferenceProcessor:
    """
    Process filtered results by aligning each person's results date with the others.

    After sorting the persons by highest time, the first person becomes the reference.
    For each next person, the alignement is made by matching their first solve time
    with the reference person times.
    
    If the reference person lowest time is higher than the current person's highest time,
    a new reference person is searched within persons having their results already aligned.
    
    If no other person has a lowest time lower than or equal to the current person's highest
    time, data of the person with the lowest time is interpolated to reach the current
    person's highest time.

    Parameters
    ----------
    filtered_results: Dataframe
        Results filtered on one event, sorted and cleaned by cubingpa.data_filter
    """

    _reference_df = None # type: DataFrame
    _reference_id = None # type: str
    _reference_values = None # type: Series
    _processed_results = None # type: DataFrame
    _df_to_concat = None # type: List[DataFrame]


    def __init__(self, filtered_results: DataFrame) -> None:
        self._persons_groups = filtered_results.groupby('personId')

        # further algorithms rely on the fact that dataframes are dealt with in descending max(time) order
        # since "not progressing solves" are removed later, max(time) is not groupby.max() but groupby.first()
        # (considering the results are already sorted by date)
        self._maxtimes = self._persons_groups.first()
        self._maxtimes = self._maxtimes.sort_values(['best'], ascending=False)

        # sort the _mintimes in the same order as the _maxtimes
        # mintime is indeed groupby.min() and not groupby.last() for the same reason
        self._mintimes = self._persons_groups.min()
        self._mintimes = self._mintimes.reindex(self._maxtimes.index)


    def process(self, log_progression: bool = False, log_debug: bool = False) -> DataFrame:
        """
        Launch processing

        Parameters
        ----------
        log_progression: bool, optional
            Indicates if process progression should be logged. Default: False
        log_debug: bool, optional
            Indicates if process progression debug information should be shown. Default: False

        Returns
        -------
        Dataframe
            Processed results
        """

        self._init_reference()
        self._init_processed_results()
        self._launch_main_process(log_progression, log_debug)

        return self._processed_results


    def _init_reference(self) -> None:
        
        reference_initialized = False
        
        while not reference_initialized:
            if len(self._maxtimes) < 1:
                raise ValueError("Not enough data to work on")
            
            self._reference_id = self._maxtimes.index[0]
            
            # create reference dataframe
            self._reference_df = self._create_person_dataframe(self._reference_id)
            
            self._reference_df = self._remove_duplicate_dates(self._reference_df)
            # ignore too small dataframes
            if len(self._reference_df.index) < 2:
                self._maxtimes = self._maxtimes.drop(self._reference_id)
                self._mintimes = self._mintimes.drop(self._reference_id)
                continue
            
            self._reference_df = utils.remove_not_progressing_solves(self._reference_df)
            # ignore too small dataframes
            if len(self._reference_df.index) < 2:
                self._maxtimes = self._maxtimes.drop(self._reference_id)
                self._mintimes = self._mintimes.drop(self._reference_id)
                continue

            self._reference_df = utils.interpolate_dates(self._reference_df)
            self._set_reference_values(self._reference_df)

            reference_initialized = True

    def _init_processed_results(self) -> None:
        self._processed_results = self._reference_df
        self._df_to_concat = [self._processed_results]


    def _update_processed_results(self, new_processed_results: DataFrame) -> None:
        self._processed_results = new_processed_results
        self._df_to_concat[0] = self._processed_results


    def _launch_main_process(self, log_progression: bool = False, log_debug: bool = False) -> None:
        if log_progression:
            # prepare process progression indication
            total_loops = len(self._maxtimes[1:len(self._maxtimes)])
            print_every_percent = 0.05
            loops_percent = round(total_loops * 0.05, 0)
            if loops_percent == 0:
                loops_percent = 1
            start_time = time.time()
            previous_time = start_time

        for i, row in enumerate(self._maxtimes[1:len(self._maxtimes)].itertuples()):
            
            if log_progression:
                current_time = time.time()
                current_running_time = current_time - previous_time
                previous_time = current_time
                total_running_time = current_time - start_time
                estimated_running_time = (total_loops * total_running_time) / (i + 1)
                # don't print every iteration
                if i == 0 or i == total_loops - 1 or (i + 1) % loops_percent == 0:
                    print(f'{(i + 1)}/{total_loops} loops, total elapsed/remaining/estimated: {round(total_running_time, 0)}/{round(estimated_running_time - total_running_time, 0)}/{round(estimated_running_time, 0)} seconds')
            
            person_df = self._create_person_dataframe(row.Index)

            person_df = self._remove_duplicate_dates(person_df)
            # ignore too small dataframes
            if len(person_df.index) < 2:
                continue

            person_df = utils.remove_not_progressing_solves(person_df)
            # ignore too small dataframes
            if len(person_df.index) < 2:
                continue

            # search matching date
            matching_date = self._find_closest_date(row[1], log_debug)
            # align dates
            delta = matching_date - person_df.index[0]
            person_df = self._shift_date(person_df, delta)

            # interpolate
            person_df = utils.interpolate_dates(person_df)
            
            # add current df to final df
            self._df_to_concat.append(person_df)

        if log_progression:
            print('Final concatenation...')

        self._processed_results = pd.concat(self._df_to_concat, axis=1, sort=False)
        self._df_to_concat = [self._processed_results]

        if log_progression:
            print('Done')


    def _create_person_dataframe(self, person_id: str) -> DataFrame:
        # create df
        person_df = self._persons_groups.get_group(person_id)
        person_df = person_df.rename(columns={'best': person_id})

        # make date the index
        return person_df.set_index('date')


    def _remove_duplicate_dates(self, person_dataframe: DataFrame) -> DataFrame:
        # remove duplicate dates by keeping best solve
        return person_dataframe.groupby('date').aggregate(np.min)


    def _set_reference_values(self, dataframe: DataFrame) -> None:
        self._reference_values = dataframe[self._reference_id].dropna().sort_values()


    def _find_date_for_value(self, dataframe: DataFrame, column_id: str, time: float) -> Any:
        """
        Find time in a dataframe column and return corresponding date
        /!\ It is assumed time exists in the dataframe

        Parameters
        ----------
        dataframe: DataFrame
            DataFrame to look into
        column_id: str
            ID of the column to look into
        time: float
            Value to look for. /!\ It is assumed time exists in the dataframe

        Returns
        -------
        datetime
            Found date
        """
        matching_rows = dataframe[dataframe[column_id] == time]

        return matching_rows.index[0]


    def _get_date_for_new_time(self, dataframe: DataFrame, column_id: str, time: float) -> Tuple[datetime, float]:
        # use data from the group (i.e. more spaced data) for a more precise value
        person_df = self._create_person_dataframe(column_id)
        person_df = self._remove_duplicate_dates(person_df)
        person_df = utils.remove_not_progressing_solves(person_df)
        
        next_to_last_date = person_df.index[len(person_df) - 2]
        next_to_last_value = person_df.iloc[len(person_df) - 2, 0]
        last_date = person_df.index[len(person_df) - 1]
        last_value = person_df.iloc[len(person_df) - 1, 0]
        days_delta = (last_date - next_to_last_date).days
        
        # number of days to add to next_to_last_date
        number_of_days_to_add = ((next_to_last_value - time) * days_delta) / (next_to_last_value - last_value)
        # number of days to add to last_date
        number_of_days_to_add = number_of_days_to_add - days_delta
        # upper round to make sure date encloses time
        number_of_days_to_add = math.ceil(number_of_days_to_add)

        new_date = self._find_date_for_value(dataframe, column_id, last_value) + timedelta(days=number_of_days_to_add)
        # recompute corresponding time to match the ceiled date
        new_time = last_value - (((next_to_last_value - last_value) * number_of_days_to_add) / days_delta)
        
        return new_date, new_time


    def _interpolate_column(self, dataframe: DataFrame, column_id: str, time: float) -> DataFrame:
        person_df = pd.DataFrame(dataframe[column_id], index=dataframe.index)
        person_df = person_df.dropna()

        date_to_add, time_to_add = self._get_date_for_new_time(dataframe, column_id, time)
    
        # create new entry and add it
        new_df = pd.DataFrame([time_to_add], columns = [column_id], index=[date_to_add]) 
        person_df = person_df.append(new_df)
        
        # interpolate
        person_df = utils.interpolate_dates(person_df)
        
        # modify /!\ IN PLACE /!\ using non-NA values from another DataFrame
        dataframe.update(person_df)

        # append new values, if any
        rows_to_append = person_df.loc[person_df.index.difference(dataframe.index, sort=False)]
        dataframe = dataframe.append(rows_to_append, sort=False)
        
        return dataframe


    def _get_reference_min_time(self) -> Any:
        """
        Get lowest time in reference. Assumes reference is sorted by ascending times.

        Returns
        -------
        float
            min time
        """
        return self._reference_values[0]


    def _update_reference(self, time: float, log_debug: bool = False) -> None:
        # test if current reference column still works for aligning current time
        # not safe using _mintimes_df: column might have been interpolated and differ
        if self._get_reference_min_time() <= time:
            return

        # next columns will be needed: update _processed_results
        self._processed_results = pd.concat(self._df_to_concat, axis=1, sort=False)
        self._df_to_concat = [self._processed_results]

        # col1 col2 col3 col4
        #  #0   #1   #2   #3
        # len = 4
        reference_column_number = self._processed_results.columns.get_loc(self._reference_id)
        last_column_number = len(self._processed_results.columns) - 1
        
        # CASE 1: no interpolation needed
        # knowing that first column is not suitable,
        # test subsequent columns to see if they can become the new reference
        # left value is inclusive, right value is exclusive in left:right
        for id in self._processed_results.columns[reference_column_number + 1:last_column_number + 1]:
            # safe (and faster) using _mintimes_df: subsequent columns have not been interpolated yet
            if self._mintimes.loc[id, 'best'] <= time:
                self._reference_id = id
                self._set_reference_values(self._processed_results)
                
                if log_debug:
                    print(f'CASE 1: no interpolation {self._reference_id}')
                
                return

        # CASE 2: interpolation needed
        # no column goes low enough: disjointed data
        # find the column with the lowest time
        min_id = self._reference_id  # give a default value to avoid mypy error (will be overriden in the loop anway)
        min_time = math.inf

        # left value is inclusive, right value is exclusive in left:right
        for id in self._processed_results.columns[reference_column_number:last_column_number + 1]:
            # skip interpolated values by using _mintimes_df, to favorise actual data
            # instead of interpolating systematically the same column
            if self._mintimes.loc[id, 'best'] < min_time:
                min_time = self._mintimes.loc[id, 'best']
                min_id = id
        
        # interpolate column to reach time of column currently added
        new_processed_results = self._interpolate_column(self._processed_results, min_id, time)
        self._update_processed_results(new_processed_results)
        self._reference_id = min_id
        self._set_reference_values(self._processed_results)
        
        if log_debug:
            print(f'CASE 2: interpolation {self._reference_id}')

        return


    def _find_closest_date(self, time: float, log_debug: bool = False) -> Any:
        """
        Find date corresponding to the closest matching time within the reference.
        Updates reference first to make sure closest date can be found.

        Parameters
        ----------
        time: float
            Time to look for
        log_debug: bool, optional
            Indicates if process progression debug information should be shown. Default: False

        Returns
        -------
        datetime
            Found date
        """

        if self._maxtimes.loc[self._reference_id, 'best'] < time:
            raise ValueError("Time is above reference max time")

        self._update_reference(time, log_debug)

        index = self._reference_values.searchsorted(time)

        #   time
        # 0  10
        # 1  20
        # if time searched for is <10, searchsorted will return index 0 (error case as the reference as been prepared to contain the value)
        # if time searched for is 10, searchsorted will return index 0
        # if time searched for is 20, searchsorted will return index 1
        # if time searched for is >20, searchsorted will return index 2 (error case as the reference as been prepared to contain the value)
        # if time searched for is 12, searchsorted will return index 1 (modify below to use the closest value instead, ie. 0)
        
        # rule out exterior bounds
        if index == 0:
            if self._reference_values[index] == time:
                return self._reference_values.index[index]

            # value is not in range
            raise RuntimeError(f"Algorithm error: could not find closest date, nor interpolate to find one. Time: {time}, Reference ID: {self._reference_id}")

        if index == len(self._reference_values):
            # value is not in range
            raise RuntimeError(f"Algorithm error: could not find closest date, nor interpolate to find one. Time: {time}, Reference ID: {self._reference_id}")

        # find closest value
        current_time = self._reference_values[index]
        previous_time = self._reference_values[index - 1]

        if current_time - time <= time - previous_time:
            return self._reference_values.index[index]
        else:
            return self._reference_values.index[index - 1]


    def _shift_date(self, dataframe: DataFrame, delta: datetime) -> DataFrame:
        return dataframe.tshift(1, freq=delta)



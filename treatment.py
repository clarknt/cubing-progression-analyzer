# external imports
import numpy as np
import pandas as pd
import math
import time
from datetime import timedelta

# internal imports
from general_data import GeneralData


def create_person_dataframe(gd, person_id):
    # create df
    person_df = gd.persons_groups.get_group(person_id)
    person_df = person_df.rename(columns={'best': person_id})

    # make date the index
    return person_df.set_index('date')


def remove_duplicate_dates(person_dataframe):
    # remove duplicate dates by keeping best solve
    return person_dataframe.groupby('date').aggregate(np.min)


def remove_not_progressing_solves(person_dataframe):
    """ remove not progressing solves from a dataframe where times are in the first column
    """
    
    column_number = 0
    rows_to_drop = []
    
    previous_time = person_dataframe.iloc[0, column_number]

    for row in person_dataframe[1:len(person_dataframe)].itertuples():
        # index is counted as a column in a row
        current_time = row[column_number + 1]
        if current_time >= previous_time:
            rows_to_drop.append(row.Index)
        else:
            # update previous_time only if current row is not to be removed
            previous_time = current_time
                
    return person_dataframe.drop(rows_to_drop)


def init_reference(gd):
    
    reference_initialized = False
    
    while not reference_initialized:
        if len(gd.maxtimes) < 1:
            raise ValueError("Not enough data to work on")
        
        gd.reference_id = gd.maxtimes.index[0]
        
        # create reference dataframe
        gd.reference_df = create_person_dataframe(gd, gd.reference_id)
        
        gd.reference_df = remove_duplicate_dates(gd.reference_df)
        # ignore too small dataframes
        if len(gd.reference_df.index) < 2:
            gd.maxtimes = gd.maxtimes.drop(gd.reference_id)
            gd.mintimes = gd.mintimes.drop(gd.reference_id)
            continue
        
        gd.reference_df = remove_not_progressing_solves(gd.reference_df)
        # ignore too small dataframes
        if len(gd.reference_df.index) < 2:
            gd.maxtimes = gd.maxtimes.drop(gd.reference_id)
            gd.mintimes = gd.mintimes.drop(gd.reference_id)
            continue

        gd.reference_df = interpolate_dates(gd.reference_df)
        set_reference_values(gd, gd.reference_df)

        reference_initialized = True


def set_reference_values(gd, dataframe):
    gd.reference_values = dataframe[gd.reference_id].dropna().sort_values()


def interpolate_dates(df):
    # build a 1-day frequency dataframe by interpolating missing data
    start_date = df.index[0]
    end_date = df.index[df.index.size-1]
    
    full_dates = pd.date_range(start=start_date, end=end_date, freq='D')
    full_index = df.index | pd.Index(full_dates)
    
    return df.reindex(full_index).interpolate()


def init_final_df(gd):
    gd.final_df = gd.reference_df
    gd.df_to_concat = [gd.final_df]


def update_final_df(gd, new_final_df):
    gd.final_df = new_final_df
    gd.df_to_concat[0] = gd.final_df


def find_date_for_value(dataframe, column_id, time):
    matching_rows = dataframe[dataframe[column_id] == time]

    return matching_rows.index[0]


def get_date_for_new_time(gd, dataframe, column_id, time):
    # use data from the group (i.e. more spaced data) for a more precise value
    person_df = create_person_dataframe(gd, column_id)
    person_df = remove_duplicate_dates(person_df)
    person_df = remove_not_progressing_solves(person_df)
    
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

    new_date = find_date_for_value(dataframe, column_id, last_value) + timedelta(days=number_of_days_to_add)
    # recompute corresponding time to match the ceiled date
    new_time = last_value - (((next_to_last_value - last_value) * number_of_days_to_add) / days_delta)
    
    return new_date, new_time


def interpolate_column(gd, dataframe, column_id, time):
    person_df = pd.DataFrame(dataframe[column_id], index=dataframe.index)
    person_df = person_df.dropna()

    date_to_add, time_to_add = get_date_for_new_time(gd, dataframe, column_id, time)
  
    # create new entry and add it
    new_df = pd.DataFrame([time_to_add], columns = [column_id], index=[date_to_add]) 
    person_df = person_df.append(new_df)
    
    # interpolate
    person_df = interpolate_dates(person_df)
    
    # modify /!\ IN PLACE /!\ using non-NA values from another DataFrame
    dataframe.update(person_df)

    # append new values, if any
    rows_to_append = person_df.loc[person_df.index.difference(dataframe.index, sort=False)]
    dataframe = dataframe.append(rows_to_append, sort=False)
    
    return dataframe


def get_reference_min_time(gd):
    return gd.reference_values[0]


def update_reference(gd, time, log_debug = False):
    # test if current reference column still works for aligning current time
    # not safe using mintimes_df: column might have been interpolated and differ
    if get_reference_min_time(gd) <= time:
        return

    # next columns will be needed: update final_df
    gd.final_df = pd.concat(gd.df_to_concat, axis=1, sort=False)
    gd.df_to_concat = [gd.final_df]

    # col1 col2 col3 col4
    #  #0   #1   #2   #3
    # len = 4
    reference_column_number = gd.final_df.columns.get_loc(gd.reference_id)
    last_column_number = len(gd.final_df.columns) - 1
    
    # CASE 1: no interpolation needed
    # knowing that first column is not suitable,
    # test subsequent columns to see if they can become the new reference
    # left value is inclusive, right value is exclusive in left:right
    for id in gd.final_df.columns[reference_column_number + 1:last_column_number + 1]:
        # safe (and faster) using mintimes_df: subsequent columns have not been interpolated yet
        if gd.mintimes.loc[id, 'best'] <= time:
            gd.reference_id = id
            set_reference_values(gd, gd.final_df)
            
            if log_debug:
                print(f'CASE 1: no interpolation {gd.reference_id}')
            
            return

    # CASE 2: interpolation needed
    # no column goes low enough: disjointed data
    # find the column with the lowest time
    min_id = None
    min_time = math.inf

    # left value is inclusive, right value is exclusive in left:right
    for id in gd.final_df.columns[reference_column_number:last_column_number + 1]:
        # skip interpolated values by using mintimes_df, to favorise actual data
        # instead of interpolating systematically the same column
        if gd.mintimes.loc[id, 'best'] < min_time:
            min_time = gd.mintimes.loc[id, 'best']
            min_id = id
    
    # interpolate column to reach time of column currently added
    new_final_df = interpolate_column(gd, gd.final_df, min_id, time)
    update_final_df(gd, new_final_df)
    gd.reference_id = min_id
    set_reference_values(gd, gd.final_df)
    
    if log_debug:
        print(f'CASE 2: interpolation {gd.reference_id}')

    return


def find_closest_date(gd, time, log_debug = False):
    
    if gd.maxtimes.loc[gd.reference_id, 'best'] < time:
        raise ValueError("Time is above reference max time")

    update_reference(gd, time, log_debug)

    index = gd.reference_values.searchsorted(time)

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
        if gd.reference_values[index] == time:
            return gd.reference_values.index[index]

        # value is not in range
        raise RuntimeError(f"Algorithm error: could not find closest date, nor interpolate to find one. Time: {time}, Reference ID: {gd.reference_id}")

    if index == len(gd.reference_values):
        # value is not in range
        raise RuntimeError(f"Algorithm error: could not find closest date, nor interpolate to find one. Time: {time}, Reference ID: {gd.reference_id}")

    # find closest value
    current_time = gd.reference_values[index]
    previous_time = gd.reference_values[index - 1]

    if current_time - time <= time - previous_time:
        return gd.reference_values.index[index]
    else:
        return gd.reference_values.index[index - 1]


def shift_date(dataframe, delta):
    return dataframe.tshift(1, freq=delta)


def launch_main_treatment(gd, log_progression = False, log_debug = False):
    if log_progression:
        # prepare treatment progression indication
        total_loops = len(gd.maxtimes[1:len(gd.maxtimes)])
        print_every_percent = 0.05
        loops_percent = round(total_loops * 0.05, 0)
        if loops_percent == 0:
            loops_percent = 1
        start_time = time.time()
        previous_time = start_time

    for i, row in enumerate(gd.maxtimes[1:len(gd.maxtimes)].itertuples()):
        
        if log_progression:
            current_time = time.time()
            current_running_time = current_time - previous_time
            previous_time = current_time
            total_running_time = current_time - start_time
            estimated_running_time = (total_loops * total_running_time) / (i + 1)
            # don't print every iteration
            if i == 0 or i == total_loops - 1 or (i + 1) % loops_percent == 0:
                print(f'{(i + 1)}/{total_loops} loops, total elapsed/remaining/estimated: {round(total_running_time, 0)}/{round(estimated_running_time - total_running_time, 0)}/{round(estimated_running_time, 0)} seconds')
        
        person_df = create_person_dataframe(gd, row.Index)

        person_df = remove_duplicate_dates(person_df)
        # ignore too small dataframes
        if len(person_df.index) < 2:
            continue

        person_df = remove_not_progressing_solves(person_df)
        # ignore too small dataframes
        if len(person_df.index) < 2:
            continue

        # search matching date
        matching_date = find_closest_date(gd, row[1], log_debug)
        # align dates
        delta = matching_date - person_df.index[0]
        person_df = shift_date(person_df, delta)

        # interpolate
        person_df = interpolate_dates(person_df)
        
        # add current df to final df
        gd.df_to_concat.append(person_df)

    if log_progression:
        print('Final concatenation...')

    gd.final_df = pd.concat(gd.df_to_concat, axis=1, sort=False)
    gd.df_to_concat = [gd.final_df]

    if log_progression:
        print('Done')


def convert_date_index_to_timedelta(df):
    timedelta_index = pd.timedelta_range(start='0 days', periods=len(df.index), freq='D')
    return df.set_index(timedelta_index)

import pandas as pd


def remove_not_progressing_solves(dataframe, column_number=0):
    """
    Considering a dataframe with a column containing solve results (usually sorted by ascending date),
    remove rows with solves not making progress over the previous solves

    Parameters
    ----------
    dataframe: Dataframe
        Dataframe having a column containing solve results
    column_number: int, optional
        Number of the column, starting at zero, where the solve results to consider are. Default: 0

    Returns
    -------
    Dataframe with progressing solves results only
    """
    
    rows_to_drop = []
    
    previous_time = dataframe.iloc[0, column_number]

    for row in dataframe[1:len(dataframe)].itertuples():
        # index is counted as a column in a row
        current_time = row[column_number + 1]
        if current_time >= previous_time:
            rows_to_drop.append(row.Index)
        else:
            # update previous_time only if current row is not to be removed
            previous_time = current_time
                
    return dataframe.drop(rows_to_drop)


def interpolate_dates(dataframe):
    """
    Considering a dataframe with dates as an index and numerical columns, sorted in ascending date order,
    build a 1-day frequency dataframe by interpolating missing data

    Parameters
    ----------
    dataframe: Dataframe
        Dataframe with dates as an index and numerical columns, sorted in ascending date order

    Returns
    -------
    Dataframe with a 1-day frequency
    """

    start_date = dataframe.index[0]
    end_date = dataframe.index[dataframe.index.size-1]
    
    full_dates = pd.date_range(start=start_date, end=end_date, freq='D')
    full_index = dataframe.index | pd.Index(full_dates)
    
    return dataframe.reindex(full_index).interpolate()


def convert_date_index_to_timedelta(dataframe):
    """
    Considering a dataframe with dates as an index, sorted in ascending date order, with a 1-day frequency,
    convert the date index to a timedelta index in days

    Parameters
    ----------
    dataframe: Dataframe
        Dataframe with dates as an index, sorted in ascending date order, with a 1-day frequency

    Returns
    -------
    Dataframe with a 1-day timedelta index
    """
    timedelta_index = pd.timedelta_range(start='0 days', periods=len(dataframe.index), freq='D')
    return dataframe.set_index(timedelta_index)

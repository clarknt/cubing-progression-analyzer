import pandas as pd
from datetime import datetime

from cubingpa import data_filter
from cubingpa.events import EventId


def debug_print(df_before, df_after, df_expected):
    print(f"---------\ndf_before\n---------\n{df_before}\n{df_before.info()}\n")
    print(f"--------\ndf_after\n--------\n{df_after}\n{df_after.info()}\n")
    print(f"-----------\ndf_expected\n-----------\n{df_expected}\n{df_expected.info()}")

def debug_print_results_competitions(df_results, df_competitions, df_after, df_expected):
    print(f"---------\ndf_results\n---------\n{df_results}\n{df_results.info()}\n")
    print(f"---------------\ndf_competitions\n---------------\n{df_competitions}\n{df_competitions.info()}\n")
    print(f"--------\ndf_after\n--------\n{df_after}\n{df_after.info()}\n")
    print(f"-----------\ndf_expected\n-----------\n{df_expected}\n{df_expected.info()}")



def test_filter_on_event_existing():
    df_before = pd.DataFrame({'eventId': [EventId.E_333.value, EventId.E_333_BF.value,
        EventId.E_333.value], 'personId': ['person1', 'person1', 'person2']}, index=[0,1,2])
    df_expected = pd.DataFrame({'personId': ['person1', 'person2']},
        index=[0,2])
    df_after = data_filter._filter_on_event(df_before, EventId.E_333)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)

def test_filter_on_event_no_removal():
    df_before = pd.DataFrame({'eventId': [EventId.E_333.value, EventId.E_333.value,
        EventId.E_333.value], 'personId': ['person1', 'person1', 'person2']}, index=[0,1,2])
    df_expected = pd.DataFrame({'personId': ['person1', 'person1', 'person2']}, index=[0,1,2])
    df_after = data_filter._filter_on_event(df_before, EventId.E_333)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)

def test_filter_on_event_remove_all():
    df_before = pd.DataFrame({'eventId': [EventId.E_333.value, EventId.E_333_BF.value,
        EventId.E_333.value], 'personId': ['person1', 'person1', 'person2']}, index=[0,1,2])
    # create a row then drop it so that column type is identical to original one
    df_expected = pd.DataFrame({'personId': ['person1']}, index=[0])
    df_expected = df_expected.drop(0)
    df_after = data_filter._filter_on_event(df_before, EventId.E_555)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)




def test_remove_invalid_results_some_removal():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2',
        'person3'], 'best': [50, -1, 50, 40]}, index=[0,1,2,3])
    df_expected = pd.DataFrame({'personId': ['person1', 'person2',
        'person3'], 'best': [50, 50, 40]}, index=[0,2, 3])
    df_after = data_filter._remove_invalid_results(df_before)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)

def test_remove_invalid_results_no_removal():
    df_before = pd.DataFrame({'personId': ['person1', 'person2',
        'person3'], 'best': [50, 50, 40]}, index=[0,1,2])
    df_expected = df_before
    df_after = data_filter._remove_invalid_results(df_before)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)

def test_remove_invalid_results_remove_all():
    df_before = pd.DataFrame({'personId': ['person1', 'person2',
        'person3'], 'best': [-1, -1, -1]}, index=[0,1,2])
    # create a row then drop it so that column type is identical to original one
    df_expected = pd.DataFrame({'personId': ['person1'], 'best': [50]}, index=[0])
    df_expected = df_expected.drop(0)
    df_after = data_filter._remove_invalid_results(df_before)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)



def test_convert_results_to_seconds_some_results():
    df_before = pd.DataFrame({'best': [1020, 1238, 912, 1196, 1052]}, index=[0,1,2,3,4])
    df_expected = pd.DataFrame({'best': [10.20, 12.38, 9.12, 11.96, 10.52]}, index=[0,1,2,3,4])
    df_after = data_filter._convert_results_to_seconds(df_before)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)

def test_convert_results_to_seconds_no_results():
    df_before = pd.DataFrame({'best': []}, index=[])
    df_expected = df_before
    df_after = data_filter._convert_results_to_seconds(df_before)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)



def test_remove_persons_with_insufficient_results_two_middle():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2',
        'person3', 'person3'], 'best': [50, 40, 50, 50, 40]}, index=[0,1,2,3,4])
    df_expected = pd.DataFrame({'personId': ['person1', 'person1',
        'person3', 'person3'], 'best': [50, 40, 50, 40]}, index=[0,1,3,4])
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 2)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_two_first():
    df_before = pd.DataFrame({'personId': ['person1', 'person2', 'person2',
        'person3', 'person3'], 'best': [50, 40, 50, 50, 40]}, index=[0,1,2,3,4])
    df_expected = pd.DataFrame({'personId': ['person2', 'person2',
        'person3', 'person3'], 'best': [40, 50, 50, 40]}, index=[1,2,3,4])
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 2)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_two_last():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2',
        'person2', 'person3'], 'best': [50, 40, 50, 50, 40]}, index=[0,1,2,3,4])
    df_expected = pd.DataFrame({'personId': ['person1', 'person1',
        'person2', 'person2'], 'best': [50, 40, 50, 50]}, index=[0,1,2,3])
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 2)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_two_last():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2',
        'person2', 'person3'], 'best': [50, 40, 50, 50, 40]}, index=[0,1,2,3,4])
    df_expected = pd.DataFrame({'personId': ['person1', 'person1',
        'person2', 'person2'], 'best': [50, 40, 50, 50]}, index=[0,1,2,3])
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 2)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_two_multiple():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2',
        'person3'], 'best': [50, 40, 50, 50]}, index=[0,1,2,3])
    df_expected = pd.DataFrame({'personId': ['person1', 'person1'],
        'best': [50, 40]}, index=[0,1])
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 2)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_two_all():
    df_before = pd.DataFrame({'personId': ['person1', 'person2',
        'person3'], 'best': [50, 40, 50]}, index=[0,1,2])
    # create a row then drop it so that column type is identical to original one
    df_expected = pd.DataFrame({'personId': ['whatever'], 'best': [50]}, index=[0])
    df_expected = df_expected.drop(0)
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 2)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_two_none():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2', 'person2',
        'person3', 'person3'], 'best': [50, 40, 50, 40, 50, 40]}, index=[0,1,2,3,4,5])
    df_expected = df_before
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 2)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_one_none():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2',
        'person3', 'person3'], 'best': [50, 40, 50, 50, 40]}, index=[0,1,2,3,4])
    df_expected = df_before
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 1)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_zero_none():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2',
        'person3', 'person3'], 'best': [50, 40, 50, 50, 40]}, index=[0,1,2,3,4])
    df_expected = df_before
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 0)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_minus_one_none():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2',
        'person3', 'person3'], 'best': [50, 40, 50, 50, 40]}, index=[0,1,2,3,4])
    df_expected = df_before
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, -1)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_three_middle():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person1', 'person2',
        'person3', 'person3', 'person3'],
        'best': [50, 40, 50, 50, 40, 30, 20]}, index=[0,1,2,3,4,5,6])
    df_expected = pd.DataFrame({'personId': ['person1', 'person1', 'person1',
        'person3', 'person3', 'person3'],
        'best': [50, 40, 50, 40, 30, 20]}, index=[0,1,2,4,5,6])
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 3)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)



def test_join_results_on_competitions_nominal():
    df_results = pd.DataFrame({'personId': ['person1', 'person2',
        'person3'], 'eventId': ['333', '333', '333'],
        'best': [15, 20, 7], 'competitionId': [1, 1, 2]}, index=[0,1,2])
    df_competitions = pd.DataFrame({'id': [1, 2], 'YEAR': [2011, 2012],
        'MONTH': [4, 2], 'DAY': [12, 4]}, index=[0,1])
    df_expected = pd.DataFrame({'personId': ['person1', 'person2',
        'person3'], 'eventId': ['333', '333', '333'],
        'best': [15, 20, 7], 'YEAR': [2011, 2011, 2012],
        'MONTH': [4, 4, 2], 'DAY': [12, 12, 4]}, index=[0,1,2])
    df_after = data_filter._join_results_on_competitions(df_results, df_competitions)
    debug_print_results_competitions(df_results, df_competitions, df_after, df_expected)
    assert df_expected.equals(df_after)

def test_join_results_on_competitions_no_competition():
    df_results = pd.DataFrame({'personId': ['person1', 'person2', 'person3'],
        'best': [15, 20, 7], 'competitionId': [1, 1, 2]}, index=[0,1,2])
    # create a row then drop it so that column type is identical to expected one
    df_competitions = pd.DataFrame({'id': [1], 'YEAR': [1],
        'MONTH': [1], 'DAY': [1]}, index=[0])
    df_competitions = df_competitions.drop(0)
    # create a row then drop it so that column type is identical to original one
    df_expected = pd.DataFrame({'personId': ['whatever'], 'best': [1],
        'YEAR': [1], 'MONTH': [1], 'DAY': [1]}, index=[0])
    df_expected = df_expected.drop(0)
    df_after = data_filter._join_results_on_competitions(df_results, df_competitions)
    debug_print_results_competitions(df_results, df_competitions, df_after, df_expected)
    assert df_expected.equals(df_after)



def test_sort_results_nominal():
    df_before = pd.DataFrame({'personId': ['person2', 'person1', 'person1', 'person1', 
        'person3', 'person3', 'person3'], 'best': [50, 50, 40, 50, 40, 30, 20],
        'YEAR': [2018, 2014, 2015, 2015, 2016, 2017, 2017], 'MONTH': [5, 1, 8, 3, 4, 8, 7],
        'DAY': [22, 5, 3, 3, 2, 3, 15]}, index=[0,1,2,3,4,5,6])
    df_expected = pd.DataFrame({'personId': ['person1', 'person1', 'person1', 'person2',
        'person3', 'person3', 'person3'], 'best': [50, 50, 40, 50, 40, 20, 30],
        'YEAR': [2014, 2015, 2015, 2018, 2016, 2017, 2017], 'MONTH': [1, 3, 8, 5, 4, 7, 8],
        'DAY': [5, 3, 3, 22, 2, 15, 3]}, index=[1,3,2,0,4,6,5])
    df_after = data_filter._sort_results(df_before)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)



def test_convert_year_month_day_to_date_nominal():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person1', 'person2',
        'person3', 'person3', 'person3'], 'best': [50, 50, 40, 50, 40, 20, 30],
        'YEAR': [2014, 2015, 2015, 2018, 2016, 2017, 2017], 'MONTH': [1, 3, 8, 5, 4, 7, 8],
        'DAY': [5, 3, 3, 22, 2, 15, 3]}, index=[1,3,2,0,4,6,5])
    df_expected = pd.DataFrame({'personId': ['person1', 'person1', 'person1', 'person2',
        'person3', 'person3', 'person3'], 'best': [50, 50, 40, 50, 40, 20, 30],
        'date': [datetime(2014,1,5), datetime(2015,3,3), datetime(2015,8,3),
            datetime(2018,5,22), datetime(2016,4,2), datetime(2017,7,15),
            datetime(2017,8,3)]}, index=[1,3,2,0,4,6,5])
    df_after = data_filter._convert_year_month_day_to_date(df_before)
    debug_print(df_before, df_after, df_expected)
    assert df_expected.equals(df_after)


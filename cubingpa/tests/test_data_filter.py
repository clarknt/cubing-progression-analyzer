import pandas as pd

from cubingpa import data_filter
from cubingpa.events import EventId


def debug_print(df_before, df_after, df_expected):
    print(f"---------\ndf_before\n---------\n{df_before}\n{df_before.info()}\n")
    print(f"--------\ndf_after\n--------\n{df_after}\n{df_after.info()}\n")
    print(f"-----------\ndf_expected\n-----------\n{df_expected}\n{df_expected.info()}")



def test_filter_on_event_existing():
    df_before = pd.DataFrame({'eventId': [EventId.E_333.value, EventId.E_333_BF.value,
        EventId.E_333.value], 'personId': ['person1', 'person1', 'person2']}, index=[0,1,2])
    df_expected = pd.DataFrame({'personId': ['person1', 'person2']},
        index=[0,2])
    df_after = data_filter._filter_on_event(df_before, EventId.E_333)
    assert df_expected.equals(df_after)

def test_filter_on_event_no_removal():
    df_before = pd.DataFrame({'eventId': [EventId.E_333.value, EventId.E_333.value,
        EventId.E_333.value], 'personId': ['person1', 'person1', 'person2']}, index=[0,1,2])
    df_expected = pd.DataFrame({'personId': ['person1', 'person1', 'person2']}, index=[0,1,2])
    df_after = data_filter._filter_on_event(df_before, EventId.E_333)
    assert df_expected.equals(df_after)

def test_filter_on_event_remove_all():
    df_before = pd.DataFrame({'eventId': [EventId.E_333.value, EventId.E_333_BF.value,
        EventId.E_333.value], 'personId': ['person1', 'person1', 'person2']}, index=[0,1,2])
    # create a row then drop it so that column type is identical to original one
    df_expected = pd.DataFrame({'personId': ['person1']}, index=[0])
    df_expected = df_expected.drop(0)
    df_after = data_filter._filter_on_event(df_before, EventId.E_555)
    assert df_expected.equals(df_after)




def test_remove_invalid_results_some_removal():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2',
        'person3'], 'best': [50, -1, 50, 40]}, index=[0,1,2,3])
    df_expected = pd.DataFrame({'personId': ['person1', 'person2',
        'person3'], 'best': [50, 50, 40]}, index=[0,2, 3])
    df_after = data_filter._remove_invalid_results(df_before)
    assert df_expected.equals(df_after)

def test_remove_invalid_results_no_removal():
    df_before = pd.DataFrame({'personId': ['person1', 'person2',
        'person3'], 'best': [50, 50, 40]}, index=[0,1,2])
    df_expected = df_before
    df_after = data_filter._remove_invalid_results(df_before)
    assert df_expected.equals(df_after)

def test_remove_invalid_results_remove_all():
    df_before = pd.DataFrame({'personId': ['person1', 'person2',
        'person3'], 'best': [-1, -1, -1]}, index=[0,1,2])
    # create a row then drop it so that column type is identical to original one
    df_expected = pd.DataFrame({'personId': ['person1'], 'best': [50]}, index=[0])
    df_expected = df_expected.drop(0)
    df_after = data_filter._remove_invalid_results(df_before)
    assert df_expected.equals(df_after)



def test_convert_results_to_seconds_some_results():
    df_before = pd.DataFrame({'best': [1020, 1238, 912, 1196, 1052]}, index=[0,1,2,3,4])
    df_expected = pd.DataFrame({'best': [10.20, 12.38, 9.12, 11.96, 10.52]}, index=[0,1,2,3,4])
    df_after = data_filter._convert_results_to_seconds(df_before)
    assert df_expected.equals(df_after)

def test_convert_results_to_seconds_no_results():
    df_before = pd.DataFrame({'best': []}, index=[])
    df_expected = df_before
    df_after = data_filter._convert_results_to_seconds(df_before)
    assert df_expected.equals(df_after)



def test_remove_persons_with_insufficient_results_two_middle():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2',
        'person3', 'person3'], 'best': [50, 40, 50, 50, 40]}, index=[0,1,2,3,4])
    df_expected = pd.DataFrame({'personId': ['person1', 'person1',
        'person3', 'person3'], 'best': [50, 40, 50, 40]}, index=[0,1,3,4])
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 2)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_two_first():
    df_before = pd.DataFrame({'personId': ['person1', 'person2', 'person2',
        'person3', 'person3'], 'best': [50, 40, 50, 50, 40]}, index=[0,1,2,3,4])
    df_expected = pd.DataFrame({'personId': ['person2', 'person2',
        'person3', 'person3'], 'best': [40, 50, 50, 40]}, index=[1,2,3,4])
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 2)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_two_last():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2',
        'person2', 'person3'], 'best': [50, 40, 50, 50, 40]}, index=[0,1,2,3,4])
    df_expected = pd.DataFrame({'personId': ['person1', 'person1',
        'person2', 'person2'], 'best': [50, 40, 50, 50]}, index=[0,1,2,3])
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 2)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_two_last():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2',
        'person2', 'person3'], 'best': [50, 40, 50, 50, 40]}, index=[0,1,2,3,4])
    df_expected = pd.DataFrame({'personId': ['person1', 'person1',
        'person2', 'person2'], 'best': [50, 40, 50, 50]}, index=[0,1,2,3])
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 2)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_two_multiple():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2',
        'person3'], 'best': [50, 40, 50, 50]}, index=[0,1,2,3])
    df_expected = pd.DataFrame({'personId': ['person1', 'person1'],
        'best': [50, 40]}, index=[0,1])
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 2)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_two_all():
    df_before = pd.DataFrame({'personId': ['person1', 'person2',
        'person3'], 'best': [50, 40, 50]}, index=[0,1,2])
    # create a row then drop it so that column type is identical to original one
    df_expected = pd.DataFrame({'personId': ['whatever'], 'best': [50]}, index=[0])
    df_expected = df_expected.drop(0)
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 2)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_two_none():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2', 'person2',
        'person3', 'person3'], 'best': [50, 40, 50, 40, 50, 40]}, index=[0,1,2,3,4,5])
    df_expected = df_before
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 2)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_one_none():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2',
        'person3', 'person3'], 'best': [50, 40, 50, 50, 40]}, index=[0,1,2,3,4])
    df_expected = df_before
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 1)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_zero_none():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2',
        'person3', 'person3'], 'best': [50, 40, 50, 50, 40]}, index=[0,1,2,3,4])
    df_expected = df_before
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 0)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_minus_one_none():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person2',
        'person3', 'person3'], 'best': [50, 40, 50, 50, 40]}, index=[0,1,2,3,4])
    df_expected = df_before
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, -1)
    assert df_expected.equals(df_after)

def test_remove_persons_with_insufficient_results_three_middle():
    df_before = pd.DataFrame({'personId': ['person1', 'person1', 'person1', 'person2',
        'person3', 'person3', 'person3'],
        'best': [50, 40, 50, 50, 40, 30, 20]}, index=[0,1,2,3,4,5,6])
    df_expected = pd.DataFrame({'personId': ['person1', 'person1', 'person1',
        'person3', 'person3', 'person3'],
        'best': [50, 40, 50, 40, 30, 20]}, index=[0,1,2,4,5,6])
    df_after = data_filter._remove_persons_with_insufficient_results(df_before, 3)
    assert df_expected.equals(df_after)


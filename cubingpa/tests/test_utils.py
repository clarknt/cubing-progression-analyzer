import pandas as pd

from cubingpa import utils

def test_remove_not_progressing_solves_superior_in_the_middle() -> None:
    df_before = pd.DataFrame({'best': [50, 40, 45, 35]}, index=[0,1,2,3])
    df_expected = pd.DataFrame({'best': [50, 40, 35]}, index=[0,1,3])
    df_after = utils.remove_not_progressing_solves(df_before)
    assert df_expected.equals(df_after)

def test_remove_not_progressing_solves_equal_in_the_middle() -> None:
    df_before = pd.DataFrame({'best': [50, 40, 40, 35]}, index=[0,1,2,3])
    df_expected = pd.DataFrame({'best': [50, 40, 35]}, index=[0,1,3])
    df_after = utils.remove_not_progressing_solves(df_before)
    assert df_expected.equals(df_after)

def test_remove_not_progressing_solves_superior_starting() -> None:
    df_before = pd.DataFrame({'best': [50, 60, 40, 35]}, index=[0,1,2,3])
    df_expected = pd.DataFrame({'best': [50, 40, 35]}, index=[0,2,3])
    df_after = utils.remove_not_progressing_solves(df_before)
    assert df_expected.equals(df_after)

def test_remove_not_progressing_solves_equals_starting() -> None:
    df_before = pd.DataFrame({'best': [50, 50, 40, 35]}, index=[0,1,2,3])
    df_expected = pd.DataFrame({'best': [50, 40, 35]}, index=[0,2,3])
    df_after = utils.remove_not_progressing_solves(df_before)
    assert df_expected.equals(df_after)

def test_remove_not_progressing_solves_superior_ending() -> None:
    df_before = pd.DataFrame({'best': [50, 45, 40, 50]}, index=[0,1,2,3])
    df_expected = pd.DataFrame({'best': [50, 45, 40]}, index=[0,1,2])
    df_after = utils.remove_not_progressing_solves(df_before)
    assert df_expected.equals(df_after)

def test_remove_not_progressing_solves_equals_ending() -> None:
    df_before = pd.DataFrame({'best': [50, 45, 40, 40]}, index=[0,1,2,3])
    df_expected = pd.DataFrame({'best': [50, 45, 40]}, index=[0,1,2])
    df_after = utils.remove_not_progressing_solves(df_before)
    assert df_expected.equals(df_after)

def test_remove_not_progressing_solves_mixed() -> None:
    df_before = pd.DataFrame({'best': [50, 60, 60, 50, 45, 45, 70, 45]}, index=[0,1,2,3,4,5,6,7])
    df_expected = pd.DataFrame({'best': [50, 45]}, index=[0,4])
    df_after = utils.remove_not_progressing_solves(df_before)
    assert df_expected.equals(df_after)

def test_remove_not_progressing_solves_nothing_to_remove() -> None:
    df_before = pd.DataFrame({'best': [50, 40, 30]}, index=[0,1,2])
    df_expected = df_before
    df_after = utils.remove_not_progressing_solves(df_before)
    assert df_expected.equals(df_after)

def test_remove_not_progressing_solves_not_default_column() -> None:
    df_before = pd.DataFrame({'event': ['333', '333', '333'], 'best': [50, 60, 30]}, index=[0,1,2])
    df_expected = pd.DataFrame({'event': ['333', '333'], 'best': [50, 30]}, index=[0,2])
    df_after = utils.remove_not_progressing_solves(df_before, column_number=1)
    assert df_expected.equals(df_after)



def test_interpolate_dates_interpolate_once() -> None:
    df_before = pd.DataFrame({'best': [50.0, 40.0, 30.0]}, index=pd.to_datetime(['01/01/2019','01/05/2019','01/06/2019']))
    df_expected = pd.DataFrame({'best': [50.0, 47.5, 45.0, 42.5, 40.0, 30.0]}, index=pd.to_datetime(['01/01/2019','01/02/2019','01/03/2019','01/04/2019','01/05/2019','01/06/2019']))
    df_after = utils.interpolate_dates(df_before)
    assert df_expected.equals(df_after)

def test_interpolate_dates_interpolate_multiple() -> None:
    df_before = pd.DataFrame({'best': [50.0, 40.0, 30.0]}, index=pd.to_datetime(['01/01/2019','01/03/2019','01/05/2019']))
    df_expected = pd.DataFrame({'best': [50.0, 45.0, 40.0, 35.0, 30.0]}, index=pd.to_datetime(['01/01/2019','01/02/2019','01/03/2019','01/04/2019','01/05/2019']))
    df_after = utils.interpolate_dates(df_before)
    assert df_expected.equals(df_after)

def test_interpolate_dates_interpolate_nothing() -> None:
    df_before = pd.DataFrame({'best': [50.0, 40.0, 30.0]}, index=pd.to_datetime(['01/01/2019','01/02/2019','01/03/2019']))
    df_expected = df_before
    df_after = utils.interpolate_dates(df_before)
    assert df_expected.equals(df_after)



def test_convert_date_index_to_timedelta_multiple_days() -> None:
    df_before = pd.DataFrame({'best': [50.0, 40.0, 30.0]}, index=pd.to_datetime(['01/01/2019','01/02/2019','01/03/2019']))
    df_expected = pd.DataFrame({'best': [50.0, 40.0, 30.0]}, index=[pd.Timedelta(days=0),pd.Timedelta(days=1),pd.Timedelta(days=2)])
    df_after = utils.convert_date_index_to_timedelta(df_before)
    assert df_expected.equals(df_after)

def test_convert_date_index_to_timedelta_one_day() -> None:
    df_before = pd.DataFrame({'best': [50.0]}, index=pd.to_datetime(['01/01/2019']))
    df_expected = pd.DataFrame({'best': [50.0]}, index=[pd.Timedelta(days=0)])
    df_after = utils.convert_date_index_to_timedelta(df_before)
    assert df_expected.equals(df_after)


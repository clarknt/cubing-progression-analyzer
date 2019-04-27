import pandas as pd

class RawData:
    """
    Data from WCA tables, directly as loaded from DB, CSV, etc.

    Attributes
    ----------
    results: Dataframe
        Dataframe holding the Results table data
    competitions: Dataframe
        Dataframe holding the Competitions table data
    """

    def __init__(self, results: pd.DataFrame, competitions: pd.DataFrame) -> None:
        self._results = results
        self._competitions = competitions

    @property
    def results(self) -> pd.DataFrame:
        return self._results

    @property
    def competitions(self) -> pd.DataFrame:
        return self._competitions


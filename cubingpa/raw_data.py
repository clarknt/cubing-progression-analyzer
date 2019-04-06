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

    def __init__(self, results, competitions):
        self._results = results
        self._competitions = competitions

    @property
    def results(self):
        return self._results

    @property
    def competitions(self):
        return self._competitions


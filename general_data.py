class GeneralData:

    def __init__(self, results):
        self._results = results

        self._persons_groups = self._results.groupby('personId')

        # further algorithms rely on the fact that dataframes are dealt with in descending max(time) order
        # since "not progressing solves" are removed later, max(time) is not groupby.max() but groupby.first()
        # (considering the results are already sorted by date)
        self._maxtimes = self._persons_groups.first()
        self._maxtimes = self._maxtimes.sort_values(['best'], ascending=False)

        # sort the mintimes in the same order as the maxtimes
        # mintime is indeed groupby.min() and not groupby.last() for the same reason
        self._mintimes = self._persons_groups.min()
        self._mintimes = self._mintimes.reindex(self._maxtimes.index)

        self._reference_df = None
        self._reference_id = None
        self._reference_values = None

        self._final_df = None
        self._df_to_concat = None


    @property
    def results(self):
        return self._results


    @property
    def persons_groups(self):
        return self._persons_groups


    @property
    def maxtimes(self):
        return self._maxtimes

    @maxtimes.setter
    def maxtimes(self, value):
        self._maxtimes = value


    @property
    def mintimes(self):
        return self._mintimes

    @mintimes.setter
    def mintimes(self, value):
        self._mintimes = value


    @property
    def reference_df(self):
        return self._reference_df

    @reference_df.setter
    def reference_df(self, value):
        self._reference_df = value


    @property
    def reference_id(self):
        return self._reference_id

    @reference_id.setter
    def reference_id(self, value):
        self._reference_id = value


    @property
    def reference_values(self):
        return self._reference_values

    @reference_values.setter
    def reference_values(self, value):
        self._reference_values = value


    @property
    def final_df(self):
        return self._final_df

    @final_df.setter
    def final_df(self, value):
        self._final_df = value


    @property
    def df_to_concat(self):
        return self._df_to_concat

    @df_to_concat.setter
    def df_to_concat(self, value):
        self._df_to_concat = value

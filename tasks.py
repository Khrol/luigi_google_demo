from lib.tasks import TableTask, AvroBigQueryTask
import pandas as pd


class Developers(TableTask):
    table = 'developers'

    def main(self):
        developers = [
            {'id': 10, 'name': 'Igor', 'country_id': 100},
        ]
        return pd.DataFrame(developers)


class Countries(TableTask):
    table = 'countries'

    def main(self):
        countries = [
            {'id': 100, 'name': 'Belarus'},
        ]
        return pd.DataFrame(countries)


class DevelopersByCountries(TableTask):
    table = 'developers_by_countries_stat'

    def requires(self):
        return {
            'developers': Developers(),
            'countries': Countries(),
        }

    def main(self, developers, countries):
        joined_data = developers[['country_id']]\
            .merge(countries, how='outer', left_on='country_id', right_on='id')
        result = joined_data.groupby('name').agg({'country_id': lambda p: len(p.dropna())}).reset_index()
        result.columns = ['country', 'developers_count']
        result.developers_count = result.developers_count.astype(int)
        return result

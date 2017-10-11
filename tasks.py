from lib.tasks import TableTask, AvroBigQueryTask
import pandas as pd


class Developers(TableTask):
    table = 'developers'

    def main(self):
        developers = [
            {'id': 10, 'name': 'Igor', 'country_id': 1},
        ]
        return pd.DataFrame(developers)


class Countries(TableTask):
    table = 'countries'

    def main(self):
        countries = [
            {'id': 1, 'name': 'Belarus'},
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
            .merge(countries, how='left', left_on='country_id', right_on='id')
        result = joined_data.groupby('name').size().to_frame().reset_index()
        result.columns = ['country', 'developers_count']
        return result

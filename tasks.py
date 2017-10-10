from lib.tasks import TableTask, AvroBigQueryTask
import pandas as pd


class Developers(TableTask):
    table = 'developers'

    def main(self):
        developers = [
            {'id': 10, 'name': 'Vasya', 'country_id': 1},
            {'id': 11, 'name': 'Kolya', 'country_id': 1},
            {'id': 12, 'name': 'Mikola', 'country_id': 2},
            {'id': 13, 'name': 'Luda', 'country_id': 2},
            {'id': 14, 'name': 'Alex', 'country_id': 2},
            {'id': 15, 'name': 'Grisha', 'country_id': 2},
            {'id': 16, 'name': 'Leh', 'country_id': 3},
            {'id': 17, 'name': 'Paul', 'country_id': 3},
            {'id': 18, 'name': 'Jan', 'country_id': 3},
        ]
        return pd.DataFrame(developers)


class Countries(TableTask):
    table = 'countries'

    def main(self):
        countries = [
            {'id': 1, 'name': 'Belarus'},
            {'id': 2, 'name': 'Ukraine'},
            {'id': 3, 'name': 'Poland'},
        ]
        return pd.DataFrame(countries)


class DevelopersByCountries(TableTask):
    table = 'developers_by_countries_stat'

    def requires(self):
        return {
            'developers': Developers(),
            'countries': Countries()
        }

    def main(self, developers, countries):
        joined_data = developers[['country_id']]\
            .merge(countries, how='left', left_on='country_id', right_on='id')
        result = joined_data.groupby('name').size().to_frame().reset_index()
        result.columns = ['country', 'developers_count']
        return result

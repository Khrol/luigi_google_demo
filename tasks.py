from lib.tasks import TableTask


class Developers(TableTask):
    table = 'developers'


class Countries(TableTask):
    table = 'countries'


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

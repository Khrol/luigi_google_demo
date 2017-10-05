developers = {
    'name': 'Developer',
    'namespace': 'com.toptal.etl',
    'type': 'record',
    'fields': [
        {'name': 'id', 'type': 'int'},
        {'name': 'name', 'type': 'string'},
        {'name': 'country_id', 'type': 'int'},
    ]
}

countries = {
    'name': 'Country',
    'namespace': 'com.toptal.etl',
    'type': 'record',
    'fields': [
        {'name': 'id', 'type': 'int'},
        {'name': 'name', 'type': 'string'},
    ]
}

developers_by_countries_stat = {
    'name': 'DevelopersByCountriesStat',
    'namespace': 'com.toptal.etl',
    'type': 'record',
    'fields': [
        {'name': 'country', 'type': 'string'},
        {'name': 'developers_count', 'type': 'int'},
    ]
}

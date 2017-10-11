To run ETL only:
```bash
$ pipenv run python -m luigi --module tasks Developers --local-scheduler
```

To run BigQuery upload:
```bash
$ pipenv run python -m luigi --module tasks AvroBigQueryTask --task DevelopersByCountries --local-scheduler
```

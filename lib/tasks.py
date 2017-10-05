import luigi
from luigi.contrib.bigquery import BigQueryTarget
from luigi.contrib.gcs import GCSTarget
from google.cloud.bigquery.client import Client as BigQueryClient
from google.cloud.bigquery.table import Table as BigQueryTable
from google.cloud.bigquery.dataset import Dataset as BigQueryDataset
from time import time

from lib.avro_utils import dataframe_from_storage, dataframe_to_storage
import schema


class AvroBigQueryTask(luigi.Task):
    task = luigi.TaskParameter()
    dataset = luigi.Parameter(default='warehouse_khrol_demo')
    project = luigi.Parameter(default='analytics-warehouse-dev')

    def requires(self):
        return self.task

    def run(self):
        client = BigQueryClient()
        job = client.load_table_from_storage(
            'demo-avro-upload-from-gs-{}-{}'.format(self.dataset, self.task.table),
            BigQueryTable(self.task.table, BigQueryDataset(self.dataset, client, project=self.project)),
            self.task.output.path
        )
        job.begin()
        start = time()
        while time() - start < 600:  # 10 minutes
            if job.state == 'DONE':
                return
        raise TimeoutError

    def output(self):
        return BigQueryTarget(self.project, self.dataset, self.task.table)


class TableTask(luigi.Task):
    table = luigi.Parameter()

    def run(self):
        params = {k: dataframe_from_storage(v.gs_path()) for k, v in self.requires().items()}
        result = self.main(**params)
        dataframe_to_storage(result, getattr(schema, self.table), self.gs_path())

    def main(self, *args, **kwargs):
        raise NotImplementedError

    def output(self):
        return GCSTarget('gs://com-toptal-analytics-dev-warehouse-khrol-demo/{}.avro'.format(self.table))

    def gs_path(self):
        return '{}.avro'.format(self.table)

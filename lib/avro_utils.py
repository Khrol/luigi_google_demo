import io
import pandas as pd
import fastavro
from google.cloud import storage
from google.cloud.storage import Blob


class GsStream(object):
    def __init__(self, path):
        self._bucket = 'com-toptal-analytics-dev-warehouse-khrol-demo'
        self._path = path
        self._file = io.BytesIO()
        self._blob = None

    def _create_blob(self):
        bucket = storage.Client(project='analytics-warehouse-dev').get_bucket(self._bucket)
        return Blob(self._path, bucket, chunk_size=262144)


class GsStreamReader(GsStream):
    def read(self, *args):
        if self._blob is None:
            self._blob = self._create_blob()
            self._blob.download_to_file(self._file)
            self._file.seek(0)
        return self._file.read(*args)


class GsStreamWriter(GsStream):
    def write(self, *args):
        if self._blob is None:
            self._blob = self._create_blob()
        self._file.write(*args)

    def flush(self):
        self._blob.upload_from_file(self._file, rewind=True)


def dataframe_from_storage(gs_path):
    reader = fastavro.reader(GsStreamReader(gs_path))
    return pd.DataFrame(list(reader))


def dataframe_to_storage(dataframe, schema, gs_path):
    stream = GsStreamWriter(gs_path)
    fastavro.writer(stream, schema, dataframe.to_dict('records'), codec='deflate')

from google.cloud import bigquery
from google.oauth2 import service_account

_credentials = service_account.Credentials.from_service_account_file('../keyfile.json')
_client = bigquery.Client(credentials=_credentials, project=_credentials.project_id)


class Schema:
    messages = bigquery.Table(
        _credentials.project_id + '.messenger.messages',
        schema=[
            bigquery.SchemaField("sender_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("content", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("reactions", "INTEGER"),
        ],
    )

    reactions = bigquery.Table(
        _credentials.project_id + '.messenger.reactions',
        schema=[
            bigquery.SchemaField("actor", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("reaction", "STRING", mode="REQUIRED"),
        ],
    )

    files = bigquery.Table(
        _credentials.project_id + '.messenger.files',
        schema=[
            bigquery.SchemaField("sender_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("file", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("reactions", "INTEGER", mode="REQUIRED"),
        ],
    )


def init():
    _client.create_table(table=Schema.messages, exists_ok=True)
    _client.create_table(table=Schema.reactions, exists_ok=True)
    _client.create_table(table=Schema.files, exists_ok=True)


def insert(table: bigquery.Table, rows):
    _client.insert_rows(table=table, rows=rows)

#Working
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import relational_db
import oracledb
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account_dataeng.json"

records = [
    {'name': 'Jan', 'num': 1},
    {'name': 'Feb', 'num': 2},
    {'name': 'Mar', 'num': 3},
    {'name': 'Apr', 'num': 4},
    {'name': 'May', 'num': 5},
]
import oracledb
source_config = relational_db.SourceConfiguration(
    drivername='oracle+oracledb',  #postgresql+pg8000
    host='172.17.35.52',
    port=1521,
    username='biusr',
    password='biusr123',
    database='XE',
    create_if_missing=True  # create the database if not there 
)

table_config = relational_db.TableConfiguration(
    name='months_col',
    create_if_missing=True,
    primary_key_columns=['num']
)

with beam.Pipeline(options=PipelineOptions()) as p:
    months = p | "Reading month records" >> beam.Create(records)
    months | 'Writing to DB table' >> relational_db.Write(
        source_config=source_config,
        table_config=table_config
    )

if __name__ == "__main__":
    print('demo code ran successful')
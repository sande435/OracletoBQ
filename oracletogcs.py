import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import oracledb

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account_dataeng.json"

class ReadFromOracle(beam.DoFn):
    
    def start_bundle(self):
        import oracledb
        # Build the DSN for the Oracle connection
        dsn = oracledb.makedsn("172.17.35.52", 1521, service_name="XE")
        # Connect using the provided credentials.
        # In thin mode (default), no additional Oracle Client libraries are needed.
        self.connection = oracledb.connect(user="biusr", password="biusr123", dsn=dsn)
        self.cursor = self.connection.cursor()

    def process(self, element):
        # Execute a query to fetch all rows from the index_data table.
        query = "SELECT id, name, value FROM index_data"
        self.cursor.execute(query)
        # Yield each row as a dictionary that matches the BigQuery schema.
        for row in self.cursor:
            yield {"id": row[0], "name": row[1], "value": row[2]}

    def finish_bundle(self):
        # Clean up resources.
        self.cursor.close()
        self.connection.close()

def run(argv=None):
    pipeline_options = PipelineOptions(argv)
    
    with beam.Pipeline(options=pipeline_options) as p:
        # Use a dummy PCollection to trigger the Oracle read.
        rows = (
            p
            | "CreateDummy" >> beam.Create([None])
            | "ReadFromOracle" >> beam.ParDo(ReadFromOracle())
        )

        # Define the BigQuery table schema.
        table_schema = {
            "fields": [
                {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "value", "type": "FLOAT", "mode": "NULLABLE"}
            ]
        }

        # Write the rows to BigQuery.
        rows | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            'mss-data-engineer-sandbox:Srikanth_Dataset.index_data_bq_new', 
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':
    run()

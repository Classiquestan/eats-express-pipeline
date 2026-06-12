
import re
import apache_beam as beam
from google.cloud import bigquery
import argparse
from apache_beam.options.pipeline_options import PipelineOptions

# Define Arguments
parser = argparse.ArgumentParser()
parser.add_argument('--input', dest='input', required=True, help='Input file to process')
paths_args, pipeline_args = parser.parse_known_args()

input_file = paths_args.input

#df = pd.read_csv(r"food_daily.csv")

# input_file = 'food_daily.csv'
# output_path = 'outputs/processed'

delivered_table_spec = 'eats-express-498821:eats_express_dataset.delivered_orders'
others_table_spec = 'eats-express-498821:eats_express_dataset.other_status_orders'

# Initialize BigQuery
client = bigquery.Client()
dataset_id = 'eats-express-498821:eats_express_dataset'

try:
    client.get_dataset(dataset_id)
except:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = 'US'
    dataset_description = "Dataset for food orders"
    client.create_dataset(dataset_id, exist_ok=True)



def remove_last_colon(item):
    if item.endswith(":"):
        return item[:-1]
    return item
    
def process_row(row):
    columns = row.split(',')
    if len(columns) > 4:
        columns[4] = remove_last_colon(columns[4])
    return ','.join(columns)

def remove_special_characters(row):
    columns = row.split(',')
    ret = ''
    for col in columns:
        ret += re.sub(r'[^a-zA-Z0-9 ]', '', col) + ','
    return ret[:-1]

def print_row(row):
    print(row)

def to_json(row):
    fields = row.split(',')
    return {
        'customer_id': fields[0],
        'date': fields[1],
        'time': fields[2],
        'order_id': fields[3],
        'items':fields[4],
        'amount':fields[5], 
        'mode': fields[6],
        'restaurnt': fields[7],
        'status': fields[8],
        'ratings': fields[9],
        'feedback': fields[10],
        'new_column': fields[11]

    }


table_schema = '''
    customer_id: STRING,
    date: STRING,
    time: STRING,
    order_id: STRING,
    items: STRING,
    amount: STRING, 
    mode: STRING,
    restaurnt: STRING,
    status: STRING,
    ratings: STRING,
    feedback: STRING,
    new_column: STRING
'''

options = PipelineOptions(pipeline_args)

with beam.Pipeline(options=options) as p:
    cleaned_data = (
        p
        | "Read Input File" >> beam.io.ReadFromText(input_file, skip_header_lines=1)
        | "Process Items Column" >> beam.Map(process_row)
        | "Convert to lowercase" >> beam.Map(lambda row: row.lower())
        | "Remove special characters" >> beam.Map(remove_special_characters)
        | "Add Count Column" >> beam.Map(lambda row: row + ',1')
    )

    delivered_orders = (
        cleaned_data
            | 'Filter delivered data' >> beam.Filter(lambda row: row and len(row.split(',')) > 8 and row.split(',')[8].strip().lower() == 'delivered')
 #           | 'Write Delivered File' >> beam.io.WriteToText(output_path + '/delivered', file_name_suffix='.csv')
        )

    undelivered_orders = (
        cleaned_data
            | 'Filter undelivered data' >> beam.Filter(lambda row: row and len(row.split(',')) > 8 and row.split(',')[8].strip().lower() != 'delivered')
#            | 'Write Undelivered File' >> beam.io.WriteToText(output_path + '/undelivered', file_name_suffix='.csv')
        )
    
    (delivered_orders
        | 'Delivered to JSON' >> beam.Map(to_json)
        | 'Write Delivered order to BigQuery' >> beam.io.WriteToBigQuery(
            delivered_table_spec,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}

        )
     )
    
    (undelivered_orders
        | 'Undelivered to JSON' >> beam.Map(to_json)
        | 'Write Delivered order to BigQuery' >> beam.io.WriteToBigQuery(
            delivered_table_spec,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}

        )
     )

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.runners.runner import PipelineState
import argparse
import re
from google.cloud import bigquery


"""
The parse_CLI_argument() function parses the command-line arguments and returns a tuple containing the path arguments and the pipeline arguments. 
The path arguments are the arguments that specify the input and output files, while the pipeline arguments are the arguments that specify the parameters of the pipeline.
The --input argument is required and specifies the input file to process.
Finally, the parse_CLI_argument() function calls the parser.parse_known_args() method to parse the command-line arguments. 
dest="input" is how we are gg call the arguments later on the script
"""
def parse_terminmal_argument():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input",
                    dest= "input",
                    required=True,
                    help="input file to process")

    path_args, pipeline_args = parser.parse_known_args()

    return path_args, pipeline_args

path_args, pipeline_args = parse_terminmal_argument()

inputs_pattern = path_args.input
options = PipelineOptions(pipeline_args)


"""--------------------------------------------------"""
"""beam pipeline"""
#initialize the beam object
beam_pipeline = beam.Pipeline(options=options)

#python methods used by the beam pipeline to perform the transformation
"""
takes a row from the data and remove the last character from the column "item(4)#
"""
def remove_last_colon(row):
    columns = row.split(",")
    item = str(columns[4]) # we are gg use the column 4 where we want to remove the ":"
    if item.endswith(":"):
        columns[4] = item[:-1]

    return ",".join(columns) 
    
"""
remove special character from each row
"""
def remove_special_characters(row):
    columns = row.split(",")
    clean_str = ""
    for col in columns:
        cleaned_col = re.sub(r'[?%$&]',"",col)
        clean_str = clean_str + cleaned_col + ","

    return clean_str[:-1]

"""
passes collection from csv to json
"""
def to_json(csv_str):
    fields = csv_str.split(",")
    
    json_str = {"customer_id":fields[0],
                 "date": fields[1],
                 "timestamp": fields[2],
                 "order_id": fields[3],
                 "items": fields[4],
                 "amount": fields[5],
                 "mode": fields[6],
                 "restaurant": fields[7],
                 "status": fields[8],
                 "ratings": fields[9],
                 "feedback": fields[10],
                 "new_col": fields[11]
                 }
    return json_str


#collection to store clean data
cleaned_data = (
    beam_pipeline
    | beam.io.ReadFromText(inputs_pattern, skip_header_lines=1) #we read the file and skip headers
    | beam.Map(remove_last_colon) #Map applies the function to each row
    | beam.Map(lambda row: row.lower()) #lamba function to make each row lower case}
    | beam.Map(remove_special_characters)
    | beam.Map(lambda row: row + ",1")
)

#collection to store delivered orders but this time it takes the collection we used to clean
# the "">> simply defines a unique label
delivered_orders = (
    cleaned_data
    | "delivered filter" >> beam.Filter(lambda row: row.split(",")[8].lower() == "delivered")

)

#collection to store undelivered orders but this time it takes the collection we used to clean
other_orders = (
    cleaned_data
    | "undelivered filter" >> beam.Filter(lambda row: row.split(",")[8].lower() != "delivered")
    
)

"""--------------------------------------------------------------------"""
"""For debugging and we are gg print on the logging the count of the element for each collection"""

(
    cleaned_data
    |"count total cleaned" >> beam.combiners.Count.Globally() #counts all the records on the collection
    |"total map cleaned" >> beam.Map(lambda x: "total count :" + str(x))
    |"print total cleaned" >> beam.Map(lambda row: print(row)) 
)

(
    delivered_orders
    |"count total delivered" >> beam.combiners.Count.Globally() #counts all the records on the collection
    |"total map delivered" >> beam.Map(lambda x: "total count :" + str(x))
    |"print total delivered" >> beam.Map(lambda row: print(row)) 
)

(
    other_orders
    |"count total other" >> beam.combiners.Count.Globally() #counts all the records on the collection
    |"total map other" >> beam.Map(lambda x: "total count :" + str(x))
    |"print total other" >> beam.Map(lambda row: print(row)) 
)

"""
biquery part to crate the data set 
"""
#we do not use service key this time, because we are gg execute the code from cloud shell 
client = bigquery.Client()

#we define the name we wanna ugive to the dataset 
dataset_food_daily_to_create = "coral-sonar-395901.food_order_dataset"

try:
    client.get_dataset(dataset_food_daily_to_create)
    print("The dataset already exists")
except Exception as e:
    dataset = bigquery.Dataset(dataset_food_daily_to_create)
    dataset.location = "US"
    dataset.description = "dataset to store food orders daily"

    dataset_ref = client.create_dataset(dataset, timeout=30)

#we can create tha tables using the usual bugquery api or also beam
#for porpuses of this exersice lets use the beam api
"""
tables creating using beam
"""
delivered_table = "coral-sonar-395901.food_order_dataset.delivered_orders"

others_table = "coral-sonar-395901.food_order_dataset.other_status_orders"



table_schema = """customer_id:STRING,
                  date:STRING,
                  timestamp:STRING,
                  order_id:STRING,
                  items:STRING,
                  amount:STRING,
                  mode:STRING,
                  restaurant:STRING,
                  status:STRING,
                  ratings:STRING,
                  feedback:STRING,
                  new_col:STRING"""

#cration of delivered orders
(
    delivered_orders
    |"delivered to json" >> beam.Map(to_json) #we pass the collection to json cause it is the only format this method uses
    |"write delivered orders" >> beam.io.WriteToBigQuery(
        delivered_table,
        schema = table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, #if the table does not exists, create it
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,#will load data appending
        additional_bq_parameters={
            "timePartitioning": {"type": "DAY"}
        }

    )
)

#cration of non delivered orders
(
    other_orders
    |"other orders to json" >> beam.Map(to_json) #we pass the collection to json cause it is the only format this method uses
    |"write other orders orders" >> beam.io.WriteToBigQuery(
        others_table,
        schema = table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, #if the table does not exists, create it
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,#will load data appending
        additional_bq_parameters={
            "timePartitioning": {"type": "DAY"}
        }

    )
)

"""
check the state of the execution
"""
response = beam_pipeline.run()
if response.state == PipelineState.DONE:print("Success!!!")
else:print("Error running beam pipeline")


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
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
    |"count total" >> beam.combiners.Count.Globally() #counts all the records on the collection
    |"total map" >> beam.Map(lambda x: "total count :" + str(x))
    |"print total" >> beam.Map(lambda row: print(row)) 
)

(
    delivered_orders
    |"count total" >> beam.combiners.Count.Globally() #counts all the records on the collection
    |"total map" >> beam.Map(lambda x: "total count :" + str(x))
    |"print total" >> beam.Map(lambda row: print(row)) 
)

(
    other_orders
    |"count total" >> beam.combiners.Count.Globally() #counts all the records on the collection
    |"total map" >> beam.Map(lambda x: "total count :" + str(x))
    |"print total" >> beam.Map(lambda row: print(row)) 
)


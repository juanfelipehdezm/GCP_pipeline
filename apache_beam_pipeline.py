import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse


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
 
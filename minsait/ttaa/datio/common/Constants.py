import os
from minsait.ttaa.datio.utils.Configuration import Configuration

# Archivo de configuracion
file_config = os.path.join("config.ini")
print(file_config)
config = Configuration( file_config = file_config, section_names=["DEFAULT"])

SPARK_MODE = "local[*]"
HEADER = "header"
INFER_SCHEMA = "inferSchema"
INPUT_PATH = config.input_path #"resources/data/players_21.csv"
OUTPUT_PATH = config.output_path #"resources/data/output"
OVERWRITE = "overwrite"
AGE_VALIDATION = int(config.age_validation)

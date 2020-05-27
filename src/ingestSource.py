import sys, os, argparse, pprint, json

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "lib"))

from Validations import Validations
from Transformations import Transformations
from ObjectContainer import ObjectContainer
from DupManager import DupManager

from ingestion.IngestFactory import IngestFactory

objParser = argparse.ArgumentParser(
    prog='processData.py',
    formatter_class=lambda prog: argparse.HelpFormatter(prog,max_help_position=50,width=160), 
    description='Ingest Movie Data'
)

objParser.add_argument('file_paths', type=json.loads, help="List of files to import, formatted as a comma delimited list with brackets")
objParser.add_argument('config_path', type=str, help="Path to the JSON config file")
objParser.add_argument('output_dir', type=str, help="Directory to output data to")

dicArgs = objParser.parse_args()

objObjectContainer = ObjectContainer()

objObjectContainer.importRef("validations", Validations()) 
objObjectContainer.importRef("transformations", Transformations()) 
objObjectContainer.importRef("dups", DupManager())

objIngestFactory = IngestFactory(objObjectContainer, dicArgs.config_path, dicArgs.file_paths, dicArgs.output_dir)
objSourceIngest = objIngestFactory.getObject()

objSourceIngest.run()

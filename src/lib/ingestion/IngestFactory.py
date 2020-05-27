import sys, os, json

from importlib import import_module

class IngestFactory:
    strIngestionType = None
    dicDataFilePaths = None
    strOutputDir = None
    objObjectContainer = None
    dicConfig = {}

    def __init__(self, objObjectContainer: object, strJsonConfigPath: str, dicDataFilePaths: str, strOutputDir: str):
        """constructor

        args:
            objObjectContainer: container for depencies
            strJsonConfigPath: the path to the json configurations file
            dicDataFilePaths: files to include in the process
            strOutputDir: the output directory
        """
        self.objObjectContainer = objObjectContainer
        self.dicDataFilePaths = dicDataFilePaths
        self.strOutputDir = strOutputDir

        if not os.path.isfile(strJsonConfigPath):
            raise ValueError("The configuration file does not exist")

        if not os.path.isdir(self.strOutputDir):
            raise ValueError("The output directory doesn't exist")

        #convert json configuration to dictionary
        with open(strJsonConfigPath) as fp:
            try:
                self.dicConfig = json.load(fp)
            except json.decoder.JSONDecodeError as strMessage:
                raise ValueError("Failed to parse JSON configuration:" + strMessage)

        self.objObjectContainer.importRef("config", self.dicConfig) 

        try:
            self.strIngestionType = self.dicConfig["ingestion_type"].replace("_", " ").title().replace(" ", "")
        except:
            raise ValueError("Ingestion type not provided in configuration")

    def getObject(self) -> object:
        """return the data parser type

        return: object
        """
        try:
            objSourceType = import_module("ingestion.source_types." + self.strIngestionType)
        except:
            raise ImportError("Ingestion type " + self.strIngestionType + " cannot be found")

        strClass = getattr(objSourceType, self.strIngestionType)

        return strClass(self.objObjectContainer, self.dicDataFilePaths, self.strOutputDir)

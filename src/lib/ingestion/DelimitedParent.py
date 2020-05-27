import os, sys, csv, json

from pprint import pprint
from Validations import Validations
from Transformations import Transformations
from CsvCleanup import CsvCleanupAttempt

class DelimitedParent:
    dicConfig = None
    dicDataFilePaths = None
    strOutputDir = None
    intDataCounter = 0
    lstHeader = []
    objValidations = None
    dicWriters = {"files":{}, "csv_writers":{}}
    objObjectContainer = None
    objDups = None
    lstResponse = []
    lstCsvExceptions = []

    def __init__(self, objObjectContainer: object, dicDataFilePaths: str, strOutputDir: str):
        """constructor method

        args:
            objObjectContainer: dependency container
            dicDataFilePaths: dicionary of file paths
            strOutputDir: the output directory
        """
        self.objObjectContainer = objObjectContainer

        self.dicConfig = self.objObjectContainer.getRef("config")
        self.dicDataFilePaths = dicDataFilePaths
        self.strOutputDir = strOutputDir
        self.objValidations = self.objObjectContainer.getRef("validations")
        self.objTransformations = self.objObjectContainer.getRef("transformations")
        self.objDups = self.objObjectContainer.getRef("dups")

        for strFilePath in self.dicDataFilePaths:
            if self.lstCsvExceptions:
                objCleanup = CsvCleanupAttempt()
                objCleanup.process(strFilePath, self.lstCsvExceptions)

            if not os.path.isfile(strFilePath):
                raise ValueError("The data file does not exist:" + strFilePath)

        if "delimiter" not in self.dicConfig:
            raise ValueError("The configuration is missing the delimiter")

        if "quotechar" not in self.dicConfig:
            raise ValueError("The configuration is missing the quotechar")

        if "validations" not in self.dicConfig:
            raise ValueError("Validations not provided in configuration")

        #validate the existence of the validations from the configuration
        for strCol, dicValidations in self.dicConfig["validations"].items():
            if "top-level" in dicValidations:
                for strValidation in dicValidations["top-level"]:
                    lstValType = strValidation.split(":")
                    if not hasattr(self.objValidations, lstValType[0]):
                        raise ValueError("Method not available for top-level validation type:" + lstValType[0])
            
            if "sub-level" in dicValidations:
                for strSubCol, dicSubValidations in dicValidations["sub-level"].items():
                    for strValidation in dicSubValidations:
                        lstValType = strValidation.split(":")
                        if not hasattr(self.objValidations, lstValType[0]):
                            raise ValueError("Method not available for sub-level validation type:" + lstValType[0])

    def __del__(self):
        """destructor
        """
        self.closeAll()
                
    def iterate(self) -> dict:
        """generator iterating through the lines in the file

        yield: dict
        """
        for strFilePath in self.dicDataFilePaths:
            with open(strFilePath) as fp:
                reader = csv.DictReader(fp, delimiter=self.dicConfig["delimiter"], quotechar=self.dicConfig["quotechar"])

                self.intDataCounter = 0
                for line in reader:
                    self.intDataCounter += 1

                    blnYield = True

                    if self.intDataCounter == 1:
                        self.lstHeader = list(line.keys())

                    if(len(line.keys()) != len(self.lstHeader)):
                        continue

                    for strCol, mixValue in line.items():
                        if "validations" in self.dicConfig and strCol in self.dicConfig["validations"]:
                            if "top-level" in self.dicConfig["validations"][strCol]:
                                for strValidationType in self.dicConfig["validations"][strCol]["top-level"]:
                                    lstValType = strValidationType.split(":")

                                    if not getattr(self.objValidations, lstValType[0])(mixValue):
                                        if len(lstValType) > 1 or lstValType[1] == "skip": 
                                            blnYield = False
                                        else: 
                                            raise ValueError("Validation type " + lstValType[0] + " faied for " + strCol + " on line " + str(self.intDataCounter))

                            if "sub-level" in self.dicConfig["validations"][strCol]:
                                pass

                        if "transformations" in self.dicConfig and strCol in self.dicConfig["transformations"]:
                            if "top-level" in self.dicConfig["transformations"][strCol]:
                                for strTransformation in self.dicConfig["transformations"][strCol]["top-level"]:
                                    if not hasattr(self.objTransformations, strTransformation):
                                        raise ValueError("Transformation type " + strTransformation + " not available for " + strCol + " on line " + str(self.intDataCounter))

                                    line[strCol] = getattr(self.objTransformations, strTransformation)(line[strCol])

                    if blnYield == True:
                        yield line

    def output(self, lstData, strFileName) -> bool:
        """write data to the out file

        args:
            lstData: the data to write
            strFileName: the filename to write to
        """
        if(strFileName not in self.dicWriters["files"]):
            self.dicWriters["files"][strFileName] = open(os.path.join(self.strOutputDir, strFileName), "w+")
            self.dicWriters["csv_writers"][strFileName] = csv.writer(self.dicWriters["files"][strFileName])

        self.dicWriters["csv_writers"][strFileName].writerow(lstData)

    def closeAll(self):
        """cloase all writers
        """
        for strKey in self.dicWriters["files"]:
            self.dicWriters["files"][strKey].close()

    def registerResponse(self, strFileName, strTableName):
        """add to the output of the sub process

        args:
            strFileName: the file name
            strTableName: the table name
        """
        self.lstResponse.append(
            {
                "table":strTableName,
                "file":strFileName
            }
        )

    def returnResponse(self):
        """get the response to output from the sub process
        """
        print(json.dumps(self.lstResponse))

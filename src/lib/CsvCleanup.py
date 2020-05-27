import os, sys, shutil, csv, pprint

class CsvCleanupAttempt:
    def process(self, strFileName: str, lstCsvExceptions: list):
        """attempt to cleanup malformed csv data

        args:
            strFileName: the name of the file to fix
            lstCsvExceptions: find and replace key value pairs
        """
        intCounter = 0

        strStageFilename = os.path.join(os.path.dirname(strFileName), "stage_cleanup_" + os.path.basename(strFileName))

        lstHeader = []

        with open(strFileName) as r_fp:
            with open(strStageFilename, "w+") as w_fp:
                for strLine in r_fp:
                    for lstReplace in lstCsvExceptions:
                        if strLine.find(lstReplace[0]):
                            strLine = strLine.replace(lstReplace[0], lstReplace[1])
                        
                    w_fp.write(strLine)

        shutil.move(strStageFilename, strFileName)

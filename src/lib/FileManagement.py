import os, wget, zipfile

class FileManagement:
    strDataDir = "data"

    def __init__(self, strDataDir: str):
        """constructor

        args:
            strDataDir: the data directory
        """
        self.strDataDir = strDataDir

    def cleanDataDir(self):
        """clear out the data directory
        """
        for strFile in os.listdir(self.strDataDir):
            os.remove(os.path.join(self.strDataDir, strFile))

    def downloadData(self, strZipUrl: str) -> str:
        """download a zip file

        args:
            strZipUrl: the zip file url

        return: the path to the file
        """
        wget.download(strZipUrl, os.path.join(self.strDataDir, os.path.basename(strZipUrl)))

        return os.path.join(self.strDataDir, os.path.basename(strZipUrl))

    def extractSourceToDestination(self, strZipFile: str):
        """extract the zip file contents to the data directory

        args:
            strZipFile: the zip file local path

        return: boolean
        """
        with zipfile.ZipFile(strZipFile, 'r') as zipRef:
            zipRef.extractall(self.strDataDir)

        os.remove(strZipFile)

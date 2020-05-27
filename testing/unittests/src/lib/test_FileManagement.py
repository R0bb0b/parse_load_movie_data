import sys, os, unittest

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../../../src/lib"))

from FileManagement import FileManagement

class FileManagementTestCase(unittest.TestCase):
    def test_downloadData(self):
        os.system("rm -rf " + os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../data/*"))

        objFileManagement = FileManagement("data")

        objFileManagement.downloadData("https://file-examples.com/wp-content/uploads/2017/02/zip_2MB.zip")

        assert os.path.isfile(os.path.join("data", "zip_2MB.zip"))

    def test_extractSourceToDestination(self):
        objFileManagement = FileManagement("data")
        
        objFileManagement.extractSourceToDestination(os.path.join("data", "zip_2MB.zip"))

        assert os.path.isfile(os.path.join("data", "zip_10MB", "file-sample_1MB.doc"))

    def test_cleanDataDir(self):
        lstFiles = os.listdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../data/zip_10MB"))

        assert len(lstFiles) == 3

        objFileManagement = FileManagement("data/zip_10MB")
        objFileManagement.cleanDataDir()

        lstFiles = os.listdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../data/zip_10MB"))

        assert len(lstFiles) == 0

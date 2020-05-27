import sys, os, unittest

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../../../src/lib"))

from CsvCleanup import CsvCleanupAttempt

class CsvCleanupTestCase(unittest.TestCase):
    def test_process(self):
        #create a file to test with 
        strFileName = "data/test.csv"

        with open(strFileName, "w+") as w_fp:
            w_fp.write("testing the cleanup process\nline number 2\nline number 3")

        objCsvCleanup = CsvCleanupAttempt()

        objCsvCleanup.process(strFileName, [
            ["line number", "number line"],
            ["testing the", "the testing"]
        ])

        intCounter = 0
        with open(strFileName) as r_fp:
            intCounter += 1

            line = r_fp.readline()

            if intCounter == 1:
                assert line.find("the testing") 
            else:
                assert line.find("number line") 

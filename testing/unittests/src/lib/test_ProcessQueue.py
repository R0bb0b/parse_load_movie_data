import sys, os, unittest, pprint

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../../../src/lib"))

from ProcessQueue import ProcessQueue

class ProcessQueueTestCase(unittest.TestCase):
    def test_run(self):
        objProc = ProcessQueue(2)

        dicProcs = {
            "test1":[
                "sleep",
                "2"
            ],
            "test2":[
                "echo",
                "testing"
            ],
            "test3":[
                "sleep",
                "3"
            ]
        }

        objProc.run(dicProcs)

        dicResponses = objProc.getProcessData()

        assert dicResponses["test2"]["stdout"].decode('utf-8').strip() == "testing"
        assert objProc.getProcessData("test2")["stdout"].decode('utf-8').strip() == "testing"
        assert objProc.getProcessData("test5") == False

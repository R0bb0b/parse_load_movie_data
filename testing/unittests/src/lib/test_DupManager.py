import sys, os, unittest

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../../../src/lib"))

from DupManager import DupManager

class DupManagerTestCase(unittest.TestCase):
    def test_isDup(self):
        objDup = DupManager()

        objDup.isDup("test1", "test1value")
        objDup.isDup("test2", "test2value")

        assert objDup.isDup("test2", "test2value")
        assert not objDup.isDup("test2", "test2eulav")

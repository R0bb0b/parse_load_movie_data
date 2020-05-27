import sys, os, unittest

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../../../src/lib"))

from ObjectContainer import ObjectContainer

class ObjectContainerTestCase(unittest.TestCase):
    def test_importGetRef(self):   
        objObjectContainer = ObjectContainer()

        dicTest = {
            "foo":"bar",
            "bar":"foo"
        }

        objObjectContainer.importRef("test1", dicTest)

        assert not objObjectContainer.getRef("test2")

        assert objObjectContainer.getRef("test1")["foo"] == "bar"

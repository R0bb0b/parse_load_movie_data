import sys, os, unittest, pprint, time

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../../../src/lib"))

from Transformations import Transformations

class TransformationsTestCase(unittest.TestCase):
    def test_malformedJsonToDict(self):
        objTrans = Transformations()

        dicObject = objTrans.malformedJsonToDict("{'foo':'bar', 'bar':'foo'}")

        pprint.pprint(dicObject)

        assert dicObject["foo"] == "bar"

    def test_malformedJsonToDict_blank_string(self):
        objTrans = Transformations()

        dicObject = objTrans.malformedJsonToDict("")

        assert not dicObject

    def test_malformedJsonToDict_valid_json(self):
        objTrans = Transformations()

        dicObject = objTrans.malformedJsonToDict(123412344)

        assert not dicObject

    def test_deAlphaize(self):
        objTrans = Transformations()

        assert objTrans.deAlphaize("abc123") == "123"
        
    def test_unixTimestampToSql(self):
        objTrans = Transformations()

        strTimeStamp = objTrans.unixTimestampToSql(time.time())

        #validating the format should be enough for this
        strDate, strTime = strTimeStamp.split(" ")

        assert len(strDate.split("-")) == 3
        assert len(strTime.split(":")) == 3

    def test_roundToWholeNumber(self):
        objTrans = Transformations()

        intWholeNumber = objTrans.roundToWholeNumber("12342134.12341234")

        assert int(intWholeNumber) == 12342134

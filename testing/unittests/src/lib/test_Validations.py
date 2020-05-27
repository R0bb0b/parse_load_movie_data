import sys, os, unittest

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../../../src/lib"))

from Validations import Validations

class ValidationsTestCase(unittest.TestCase):
    def test_notNull(self):
        objValidations = Validations()

        assert objValidations.notNull("test") == True
        assert objValidations.notNull("") == False

    def test_isInteger(self):
        objValidations = Validations()

        assert objValidations.isInteger("123") == True
        assert objValidations.isInteger("abc") == False

    def test_isDecimal(self):
        objValidations = Validations()

        assert objValidations.isDecimal("123.123") == True
        assert objValidations.isDecimal("abc") == False

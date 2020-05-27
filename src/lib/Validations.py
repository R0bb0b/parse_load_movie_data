import json, re, ast, decimal

from pprint import pprint

class Validations:
    def notNull(self, strValue: str) -> bool:
        """determine if the value is null

        args:
            strValue: the zip file url

        return: boolean
        """
        if not strValue:
            return False
        
        return True
        
    def isInteger(self, strValue: str) -> bool:
        """determine if the value is an integer

        args:
            strValue: the value to validate

        return: boolean
        """
        return strValue.isdigit()
        
    def isDecimal(self, strValue: str) -> bool:
        """determine if the value is a decimal

        args:
            strValue: the value to validate

        return: boolean
        """
        try:
            decimal.Decimal(strValue)
        except:
            return False

        return True

import json, re, ast

from pprint import pprint
from datetime import datetime

class Transformations:
    def malformedJsonToDict(self, strObject: str) -> dict:
        """attempt to return a dictionary from malformed json data

        args:
            strObject: the object to transform

        return: dict
        """
        if not strObject:
            return {}

        # TODO: determine if all of the data in quesiton os malformed json and if so we may need to change our approach here
        try:
            mixJson = json.loads(json.dumps(ast.literal_eval(strObject)))
        except:
            return {}

        return mixJson if isinstance(mixJson, dict) or isinstance(mixJson, list) else []

    def deAlphaize(self, strValue: str) -> str:
        """remove alpha numeric characters from a string

        args:
            strValue: the value to transform

        return: string
        """
        return re.sub('[^0-9]','', strValue)

    def unixTimestampToSql(self, strTimestamp) -> str:
        """convert epoch timestamp to SQL syntax

        args:
            strTimestamp: the value to transform

        return: string
        """
        return datetime.fromtimestamp(
            int(strTimestamp)
        ).strftime('%Y-%m-%d %H:%M:%S')

    def roundToWholeNumber(self, strValue) -> int:
        """drop the decimal from a string 

        args:
            strValue: the value to transform

        return: string
        """
        if not strValue:
            return 0
        else:
            return round(float(strValue))

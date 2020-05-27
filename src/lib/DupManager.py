class DupManager:
    def __init__(self):
        """constructor
        """
        self.dicDups = {}

    def isDup(self, strDupName: str, strValue: str) -> bool:
        """check if the value is a duplicate

        args:
            strDupName: the key to associate the value to in the dictionary
            strValue: the value to check

        return: boolean
        """
        if strDupName not in self.dicDups:
            self.dicDups[strDupName] = {}

        if strValue not in self.dicDups[strDupName]:
            self.dicDups[strDupName][strValue] = True
            return False

        return True

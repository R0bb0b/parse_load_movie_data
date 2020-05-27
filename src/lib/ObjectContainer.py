class ObjectContainer:
    def __init__(self):
        """constructor
        """
        self.dicObjects = {}

    def importRef(self, strObjName: str, objObject: object):
        """Import a reference into the object container

        args:
            strObjName: the name of the list to append the value
            objObject: the object to import
        """
        self.dicObjects[strObjName] = objObject

    def getRef(self, strObjName: str):
        """get the reference by key

        args:
            strObjName: key with which to identify the object

        return: mixed
        """
        if strObjName not in self.dicObjects:
            return False

        return self.dicObjects[strObjName]

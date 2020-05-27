import os

class Redshift:
    objConn = None
    strIAmRole = ""
    strAWSRegion = ""
    lstSwapTables = []

    def __init__(self, objConn: object, strIAmRole: str, strAWSRegion: str):
        """constructor

        args:
            objConn: the database connecton object
            strIAmRole: string identifying the aws roles associated with the database
            strAWSRegion: the AWS region
        """
        self.objConn = objConn
        self.strIAmRole = strIAmRole
        self.strAWSRegion = strAWSRegion

    def __del__(self):
        """destructor
        """
        self.cleanUp()

    def cleanUp(self):
        """any cleanup tasks to take care of before the ojbect is destructed
        """
        pass

    def loadTables(self, dicLoadInfo: dict, strS3BucketName: str, strS3Folder: str) -> bool:
        """load data into Redshift tables

        args:
            dicLoadInfo: dictionary id table and file names
            strS3BucketName: the bucket name used for AWS
            strS3Folder: the s3 folder name

        return: boolean
        """
        #create and load staging tables
        for strKey, lstTables in dicLoadInfo.items():
            for dicTableInfo in lstTables:
                if not self.createStagingTable(dicTableInfo["table"]):
                    raise ValueError("creation of staging table for " + dicTableInfo["table"] + " failed")

                if not self.loadStagingTable(dicTableInfo["table"], strS3BucketName, os.path.join(strS3Folder, dicTableInfo["file"])):
                    raise ValueError("loading of staging table for " + dicTableInfo["table"] + " failed")

        self.swapTables()

        return True

    def swapTables(self):
        """swap staging tables for their landing coutner parts
        """
        cursor = self.objConn.cursor()
        cursor.execute("begin")

        try:
            for strTable in self.lstSwapTables:
                cursor.execute("drop table " + strTable)
                cursor.execute("alter table staging_" + strTable + " rename to " + strTable)
        except:
            cursor.execute("rollback")
            raise ValueError("table swap failed")

        cursor.execute("commit")

        cursor.close()
        self.objConn.commit()

    def createStagingTable(self, strTableName: str) -> bool: 
        """create a staging table based on the current table

        args:
            strTableName: the table to base the staging structure on

        return: boolean
        """
        strStagingTable = "staging_" + strTableName

        cursor = self.objConn.cursor()
        strQuery = "create table " + strStagingTable + " (like " + strTableName + ")"

        try:
            cursor.execute(strQuery)
            self.objConn.commit()

            self.lstSwapTables.append(strTableName)
        except Exception as error:
            print("creating staging table failed with error:" + str(error))
            return False

        cursor.close()

        return True

    def loadStagingTable(self, strTableName: str, strBucketName: str, strRemoteFile: str) -> bool: 
        """load the staging table from S3 data

        args:
            strTableName: the table name associated with the staging table
            strBucketName: the bucket name for S3
            strRemoteFile: the remote file name

        return: boolean
        """
        strStagingTable = "staging_" + strTableName

        cursor = self.objConn.cursor()

        strQuery = "copy " + strStagingTable + " from 's3://" + strBucketName + "/" + strRemoteFile + "' \
                IGNOREHEADER 1 credentials 'aws_iam_role=" + self.strIAmRole + "' \
                CSV region '" + self.strAWSRegion + "';\
            "
        try:
            cursor.execute(strQuery)

            self.objConn.commit()
        except Exception as error:
            print("loading staging table failed with error:" + str(error))
            return False

        cursor.close()

        return True

class S3:
    objS3 = None 

    def __init__(self, objS3: object):
        """constructor

        args:
            objS3: the S3 connection object
        """
        self.objS3 = objS3

    def uploadFile(self, strLocalFile: str, strRemoteFile: str, strBucket: str) -> bool:
        """Upload a file to S3

        args:
            strLocalFile: the local file name
            strRemoteFile: the remote file name
            strBucket: the bucket name

        return: boolean
        """
        try:
            self.objS3.upload_file(strLocalFile, strBucket, strRemoteFile)
            return True
        except:
            return False

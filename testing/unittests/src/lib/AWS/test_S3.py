import sys, os, unittest, boto3
from unittest.mock import Mock

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../../../src/lib"))

from AWS.S3 import S3

class S3TestCase(unittest.TestCase):
    def test_uploadFile_success(self):
        objMock_boto3 = Mock()
        objMock_boto3.upload_file = Mock(return_value=True)

        objS3 = S3(objMock_boto3)

        assert objS3.uploadFile("foo", "bar", "foobar") == True

    def test_uploadFile_fail(self):
        objMock_boto3 = Mock()
        objMock_boto3.upload_file = Mock(side_effect=KeyError('foo'))

        objS3 = S3(objMock_boto3)

        assert objS3.uploadFile("foo", "bar", "foobar") == False

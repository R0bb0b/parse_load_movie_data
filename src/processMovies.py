import os, sys, shutil, json, boto3, psycopg2

from pprint import pprint

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "lib"))

from FileManagement import FileManagement
from ProcessQueue import ProcessQueue
from AWS.S3 import S3
from AWS.Redshift import Redshift


dicConfig = {}
try:
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "config", "process_data.json")) as r_fp:
        dicConfig = json.loads(r_fp.read())
except:
    raise ValueError("AWS access data unavailable")


strOutputDir = dicConfig["output_dir"]
strInputDir = dicConfig["input_dir"]


#ingest AWS access keys
dicAWSAccess = {}
try:
    with open(dicConfig["AWSAuthInfoPath"]) as r_fp:
        dicAWSAccess = json.loads(r_fp.read())
except:
    raise ValueError("AWS access data unavailable")


# File configurations
dicMovieData = dicConfig["movieData"]


#download source zip file
print("Downloading source data")
objFileManagement = FileManagement(strInputDir)
objFileManagement.cleanDataDir()

strZipFile = objFileManagement.downloadData("https://s3-us-west-2.amazonaws.com/com.guild.us-west-2.public-data/project-data/the-movies-dataset.zip")

#extract zip file to 
print("\nExtracting data")
objFileManagement.extractSourceToDestination(strZipFile)


#generate process commands
objProc = ProcessQueue(5)
dicCommands = {}

for strConfigName, dicConfig in dicMovieData.items():
    dicCommands[strConfigName] = [
        "python3",
        "ingestSource.py",
        str('["' + '", "'. join(dicConfig["files"]) + '"]').replace("[DATA_DIR]", strInputDir),
        dicConfig["config"],
        strOutputDir
    ]


#run processes
print("Parsing Data")
objProc.run(dicCommands)


#handle process responses
dicDeployData = {}
for strKey, dicResponse in objProc.getProcessData().items():
    if dicResponse["retcode"] != 0:
        if "stderr" in dicResponse:
            raise ValueError(dicResponse["stderr"].decode('utf-8'))
        else:
            raise ValueError("Error:" + strKey + " failed to parse correctly")
    else:
        dicDeployData[strKey] = json.loads(dicResponse["stdout"])


#initiate transfer to S3
print("uploading to S3")
objS3 = S3(
    boto3.client(
        's3', 
        aws_access_key_id=dicAWSAccess["AWSAccessKeyId"], 
        aws_secret_access_key=dicAWSAccess["AWSSecretKey"]
    )
)

for strKey, lstResponse in dicDeployData.items():
    for dicDataInfo in lstResponse:
        if not objS3.uploadFile(os.path.join(strOutputDir, dicDataInfo["file"]), os.path.join("data_engineer_project", dicDataInfo["file"]), dicAWSAccess["S3Bucket"]):
            raise ValueError(dicDataInfo["file"] + " failed to upload to S3")


print("loading data")
objRedshift = Redshift(
    psycopg2.connect(
        host=dicAWSAccess["redshift_host"],
        user=dicAWSAccess["redshift_user"],
        port=dicAWSAccess["redshift_port"],
        password=dicAWSAccess["redshift_password"],
        dbname=dicAWSAccess["redshift_db"]
    ),
    dicAWSAccess["IAmRole"],
    dicAWSAccess["AWSRegion"]
)

objRedshift.loadTables(dicDeployData, dicAWSAccess["S3Bucket"], "data_engineer_project")

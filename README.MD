# Movie MetaData

This project is an answer to a coding assessment and does the following:
1. Download and unzip the file located at https://s3-us-west-2.amazonaws.com/com.guild.us-west-2.public-data/project-data/the-movies-dataset.zip
2. The data is downloaded and unzipped to the data directory.
3. The configuration files defining how the data is parsed is located in the config directory.
4. The parent process forks off sub processes to parse the data into csv files ready for import.
    - 5 sub processes in total
        - python3 ingestSource.py '["data/credits.csv"]' config/credits.json output
        - python3 ingestSource.py '["data/keywords.csv"]' config/keywords.json output
        - python3 ingestSource.py '["data/links.csv", "data/links_small.csv"]' config/links.json output
        - python3 ingestSource.py '["data/ratings.csv", "data/ratings_small.csv"]' config/ratings.json output
        - python3 ingestSource.py '["data/movies_metadata.csv"]' config/movies_metadata.json output
    - Each sub process uses a json configuration identifying the config name, source file delimiter, source file quote character, field transformations and validations
    - Each sub process then drops the resulting csv files into the output directory.  Some, specifically the metadata file, result in several csv output files.
    - Each sub process then returns a json string back to the parent identifying tables and associated source files to be uploaded to S3
5. Using the data returned from the sub processes, the parent uploads the sources files to S3, creates staging tables in Redshift, loads said staging tables using the source files, and then within a transaction, deletes the current landing tables and swaps the staging tables in their place.

With the data in Redshift it should be very easy to set up routes to serve the data.

### Usage
To kick of the process just run the following command
```
python3 processMovies.py
```
In the main configuration file (config/process_data.json) you will find the following configurations:
- output directory name
    - the directory where parsed files are stored after they have been processed
- input directory name
    - the directory where the zip file in extracted to
- AWS auth info path
    - the path containing AWS authorization information
    - should be kept in a secure location
    - adheres to the following structure:
        - ```
            {
                "AWSAccessKeyId":"Your AWS Access Key",
                "AWSSecretKey":"Your AWS Secret Key",
                "IAmRole":"Your IAmRole arn info",
                "AWSRegion":"AWS Region",
                "redshift_host":"your redshift host name",
                "redshift_user":"your redshift user name",
                "redshift_port":"your redshift port",
                "redshift_password":"your redshift password",
                "redshift_db":"your redshift db name",
                "S3Bucket":"your s3 bucket name"
            }
        ```
- Movie data configuration
    - dictionary containing individual configuration file locations and files for each input file type 

Each sub process can be run individually if one so desires.  Below is the help output associated with the sub process python file:
```
usage: processData.py [-h] file_paths config_path output_dir

Ingest Movie Data

positional arguments:
  file_paths   List of files to import, formatted as a comma delimited list with brackets
  config_path  Path to the JSON config file
  output_dir   Directory to output data to

optional arguments:
  -h, --help   show this help message and exit
```
### Prerequisites and Installation
psycopg2 requires libpq, which I installed on Ubuntu using the following command:
```sudo apt-get install libpq-dev```

To then install the Python3 requirements just run the following command inside the root of the project:
```
pip3 install -r requirements.txt
```
The previous command will isntall the following:
- wget
- boto3
- psycopg2
- pytest
- pytest-cov

Developed using Python version: 3.6.9


## Running the tests

navigate to the testing/unittests directory:
```
cd testing/unittests
```

Run tests
```
python3 -m pytest
```

Run tests with code coverage report
```
python3 -m pytest --cov-report term-missing --cov=../../lib
```

## Author

* **Robert Baldessari** - (https://github.com/R0bb0b)

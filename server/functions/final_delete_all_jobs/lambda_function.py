#
# Lambda function that handles the deletion of all jobs by
# querying the RDS database for all job rows, deleting all
# the rows, and deleting any results files from the S3 bucket
#
# Authors:
#   Bennett Lindberg
#
#   Prof. Joe Hummel (initial template, from project03)
#   Northwestern University
#   CS 310
#

import json
import boto3
import os
import datatier

from configparser import ConfigParser


def lambda_handler(event, context):
    try:
        print("**STARTING**")
        print("**lambda: final_delete_all_jobs**")

        #
        # set up AWS based on config file
        #
        config_file = "graphapp-config.ini"
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = config_file

        configur = ConfigParser()
        configur.read(config_file)

        #
        # configure for S3 access
        #
        s3_profile = "s3readwrite"
        boto3.setup_default_session(profile_name=s3_profile)

        bucketname = configur.get("s3", "bucket_name")

        s3 = boto3.resource("s3")
        bucket = s3.Bucket(bucketname)

        #
        # configure for RDS access
        #
        rds_endpoint = configur.get("rds", "endpoint")
        rds_portnum = int(configur.get("rds", "port_number"))
        rds_username = configur.get("rds", "user_name")
        rds_pwd = configur.get("rds", "user_pwd")
        rds_dbname = configur.get("rds", "db_name")

        #
        # open connection to database
        #
        print("**Opening DB connection**")

        dbConn = datatier.get_dbConn(
            rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname
        )

        #
        # get all job rows from database
        #
        print("**Retrieving all job rows from database**")

        sql = "SELECT * FROM jobs;"

        rows = datatier.retrieve_all_rows(dbConn, sql)

        #
        # delete job results files from S3
        #
        print("**Deleting all job results files from S3**")

        for row in rows:
            resultsfilekey = row[3]

            if resultsfilekey != None:
                bucket.delete_objects(Delete={"Objects": [{"Key": resultsfilekey}]})

        #
        # delete all job rows from database
        #
        print("**Deleting all job rows from database**")

        sql = "DELETE FROM jobs;"

        datatier.perform_action(dbConn, sql)

        #
        # success: 200 OK
        #
        print("**DONE, returning success**")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "success"}),
        }

    #
    # error: 500 INTERNAL SERVER ERROR
    #
    except Exception as err:
        print("**ERROR**")
        print(str(err))

        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(err)}),
        }

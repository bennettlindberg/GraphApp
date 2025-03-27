#
# Lambda function that handles downloading of
# graph analysis results, if available, and reports
# on the status of current graph analysis jobs
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
import base64
import datatier

from configparser import ConfigParser


def lambda_handler(event, context):
    try:
        print("**STARTING**")
        print("**lambda: final_download_results**")

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
        # get jobid from request
        #
        print("**Accessing event/pathParameters**")

        if "jobid" in event:
            jobid = event["jobid"]
        elif "pathParameters" in event:
            if "jobid" in event["pathParameters"]:
                jobid = event["pathParameters"]["jobid"]
            else:
                raise Exception("endpoint requires jobid parameter in pathParameters")
        else:
            raise Exception("endpoint requires jobid parameter in event")

        print("Requested jobid:", jobid)

        #
        # open connection to database
        #
        print("**Opening DB connection**")

        dbConn = datatier.get_dbConn(
            rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname
        )

        #
        # get job row from database
        #
        print("**Retrieving job row from database**")

        sql = "SELECT * FROM jobs WHERE jobid = %s;"

        row = datatier.retrieve_one_row(dbConn, sql, [jobid])

        if row == ():  # no such job
            print("**No such job, returning...**")
            return {
                "statusCode": 404,
                "body": json.dumps(
                    {
                        "message": f"jobid {jobid} does not exist in the database",
                        "data": "",
                    }
                ),
            }

        graphid = row[1]
        status = row[2]
        resultsfilekey = row[3]

        print("Successfully retrieved row:", row)

        #
        # status: still processing
        #
        if status == "processing":
            return {
                "statusCode": 481,
                "body": json.dumps(
                    {
                        "message": f"results for jobid {jobid} not available yet",
                        "data": "",
                    }
                ),
            }

        #
        # status: error occurred
        #
        elif status == "error":
            return {
                "statusCode": 482,
                "body": json.dumps(
                    {
                        "message": f"jobid {jobid} terminated due to an unknown error",
                        "data": "",
                    }
                ),
            }

        #
        # status: unexpected value
        #
        elif status != "completed":
            raise Exception(f"jobid {jobid} has unexpected status: {status}")

        #
        # status: completed successfully (even if analysis itself failed)
        #

        #
        # sanity check: we have a results file
        #
        if resultsfilekey == None:
            raise Exception(f"jobid {jobid} has completed but has no results file key")

        #
        # download file from bucket
        #
        local_filename = "/tmp/local_graph_results_file.json"

        print("**Downloading graph from S3**")

        bucket.download_file(resultsfilekey, local_filename)

        #
        # read bytes from downloaded file
        #
        infile = open(local_filename, "rb")
        bytes = infile.read()
        infile.close()

        #
        # convert file byte format
        #
        data = base64.b64encode(bytes)
        datastr = data.decode()

        #
        # success: 200 OK
        #
        print("**DONE, returning success**")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "success", "data": datastr}),
        }

    #
    # error: 500 INTERNAL SERVER ERROR
    #
    except Exception as err:
        print("**ERROR**")
        print(str(err))

        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(err), "data": ""}),
        }

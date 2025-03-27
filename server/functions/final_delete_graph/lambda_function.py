#
# Lambda function that handles the deletion of graphs by
# querying the RDS database for the relevant row, deleting
# the row, and deleting any relevant files from the S3 bucket
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
        print("**lambda: final_delete_graph**")

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
        # get graphid from request
        #
        print("**Accessing event/pathParameters**")

        if "graphid" in event:
            graphid = event["graphid"]
        elif "pathParameters" in event:
            if "graphid" in event["pathParameters"]:
                graphid = event["pathParameters"]["graphid"]
            else:
                raise Exception("endpoint requires graphid parameter in pathParameters")
        else:
            raise Exception("endpoint requires graphid parameter in event")

        print("Requested graphid:", graphid)

        #
        # open connection to database
        #
        print("**Opening DB connection**")

        dbConn = datatier.get_dbConn(
            rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname
        )

        #
        # get graph row from database
        #
        print("**Retrieving graph row from database**")

        sql = "SELECT * FROM graphs WHERE graphid = %s;"

        row = datatier.retrieve_one_row(dbConn, sql, [graphid])

        if row == ():  # no such graph
            print("**No such graph, returning...**")
            return {
                "statusCode": 404,
                "body": json.dumps(
                    {"message": f"graphid {graphid} does not exist in the database"}
                ),
            }

        datafilekey = row[1]
        visualfilekey = row[2]

        print("Successfully retrieved row:", row)

        #
        # delete graph row from database
        #
        print("**Deleting graph row from database**")

        sql = "DELETE FROM graphs WHERE graphid = %s;"

        datatier.perform_action(dbConn, sql, [graphid])

        #
        # delete graph data file from S3
        #
        bucket.delete_object(datafilekey)

        #
        # delete graph visualization file from S3
        #
        if visualfilekey != None:
            bucket.delete_object(visualfilekey)

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

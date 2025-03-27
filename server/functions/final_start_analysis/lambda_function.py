#
# Lambda function that handles requests for graph
# analysis by creating a new analysis job in the
# database and triggering a separate asynchronous
# lambda function
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
        print("**lambda: final_start_analysis**")

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
        # get type from request
        #
        print("**Accessing event/pathParameters**")

        if "type" in event:
            type = event["type"]
        elif "pathParameters" in event:
            if "type" in event["pathParameters"]:
                type = event["pathParameters"]["type"]
            else:
                raise Exception("endpoint requires type parameter in pathParameters")
        else:
            raise Exception("endpoint requires type parameter in event")

        print("Requested graph type:", type)

        #
        # get root from request
        #
        print("**Accessing event/queryStringParameters**")

        if "root" in event:
            root = event["root"]
        elif (
            "queryStringParameters" in event and event["queryStringParameters"] != None
        ):
            if "root" in event["queryStringParameters"]:
                root = event["queryStringParameters"]["root"]
            else:
                root = None
        else:
            root = None

        if root != None:
            root = int(root)

        print("Requested root vertex:", root)

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
                    {
                        "message": f"graphid {graphid} does not exist in the database",
                        "jobid": -1,
                    }
                ),
            }

        datafilekey = row[1]

        print("Successfully retrieved row:", row)

        #
        # download file from bucket
        #
        local_filename = "/tmp/local_graph_data_file.json"

        print("**Downloading graph from S3**")

        bucket.download_file(datafilekey, local_filename)

        #
        # read bytes from downloaded file
        #
        infile = open(local_filename, "rb")
        bytes = infile.read()
        infile.close()

        graph_data = json.loads(bytes.decode())

        #
        # validate passed parameters
        #
        validation_results = validate_parameters(type, root, graph_data)
        if validation_results != None:
            return validation_results

        #
        # create new job row in database
        #
        print("**Adding job row to database**")

        sql = """
        INSERT INTO jobs(graphid, status, resultsfilekey)
                    VALUES(%s, %s, %s);
        """

        datatier.perform_action(dbConn, sql, [graphid, "processing", None])

        #
        # grab the jobid that was auto-generated by mysql
        #
        sql = "SELECT LAST_INSERT_ID();"

        row = datatier.retrieve_one_row(dbConn, sql)

        jobid = row[0]

        print("Created row with jobid:", jobid)

        #
        # invoke analysis lambda function
        #
        print("**Invoking lambda function 'final_perform_analysis'**")

        lambda_client = boto3.client("lambda")

        lambda_client.invoke(
            FunctionName="final_perform_analysis",
            InvocationType="Event",
            Payload=json.dumps({"jobid": jobid, "type": type, "root": root}),
        )

        #
        # success: 200 OK
        #
        print("**DONE, returning success**")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "success", "jobid": jobid}),
        }

    #
    # error: 500 INTERNAL SERVER ERROR
    #
    except Exception as err:
        print("**ERROR**")
        print(str(err))

        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(err), "jobid": -1}),
        }


def validate_parameters(type, root, graph_data):
    if type not in [
        "is_connected",
        "has_cycle",
        "shortest_paths",
        "reachable_nodes",
        "mst",
    ]:
        return {
            "statusCode": 400,
            "body": json.dumps(
                {
                    "message": f"analysis type {type} is invalid",
                    "jobid": -1,
                }
            ),
        }

    if type in ["shortest_paths", "reachable_nodes"]:
        if root == None:
            return {
                "statusCode": 400,
                "body": json.dumps(
                    {
                        "message": f"analysis type {type} requires a root node identifier",
                        "jobid": -1,
                    }
                ),
            }
        elif root not in graph_data["vertices"]:
            return {
                "statusCode": 400,
                "body": json.dumps(
                    {
                        "message": f"root node {root} does not exist in the graph",
                        "jobid": -1,
                    }
                ),
            }

    # success case
    return None

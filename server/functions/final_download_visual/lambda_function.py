#
# Lambda function that handles the retrieval of graph
# visualizations by returning the existing visual or
# generating one if a visual does not already exist
#
# Authors:
#   Bennett Lindberg
#
#   Prof. Joe Hummel (initial template, from project03)
#   Northwestern University
#   CS 310
#

import os
import json
import boto3
import uuid
import base64
import datatier
import networkx as nx
import matplotlib.pyplot as plt

from configparser import ConfigParser


def lambda_handler(event, context):
    try:
        print("**STARTING**")
        print("**lambda: final_download_visual**")

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
                    {
                        "message": f"graphid {graphid} does not exist in the database",
                        "data": "",
                    }
                ),
            }

        datafilekey = row[1]
        visualfilekey = row[2]

        print("Successfully retrieved row:", row)

        #
        # create a visual if one does not already exist
        #
        if visualfilekey == None:
            print("**Generating visual for graph**")

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

            #
            # generate equivalent networkx graph
            #
            graph_data = json.loads(bytes.decode())

            nx_graph = make_nx_graph(graph_data)

            #
            # create PNG visualization file
            #
            local_filename = "/tmp/local_graph_visual_file.png"

            print("**Creating graph visualization file**")

            pos = pos = nx.spring_layout(nx_graph)

            nx.draw_networkx_nodes(nx_graph, pos=pos, node_size=700, alpha=0.9)

            nx.draw_networkx_edges(
                nx_graph, pos=pos, width=1.5, alpha=0.7, edge_color="gray"
            )

            nx.draw_networkx_labels(
                nx_graph,
                pos=pos,
                font_size=10,
                font_color="black",
                font_family="sans-serif",
                font_weight="bold",
            )

            edge_labels = {}
            for start, end, weight_dict in nx_graph.edges(data=True):
                edge_labels[(start, end)] = weight_dict["weight"]

            nx.draw_networkx_edge_labels(
                nx_graph,
                pos=pos,
                edge_labels=edge_labels,
                font_size=8,
                font_family="sans-serif",
            )

            plt.axis("off")
            plt.tight_layout()
            plt.savefig(local_filename)
            plt.clf()

            #
            # generate unique filename in preparation for the S3 upload
            #
            print("**Uploading local file to S3**")

            bucketkey = "graphapp/" + "graph_visual_file_" + str(uuid.uuid4()) + ".png"

            print("Using S3 bucketkey:", bucketkey)

            #
            # upload file to S3
            #
            bucket.upload_file(
                local_filename,
                bucketkey,
                ExtraArgs={"ACL": "public-read", "ContentType": "application/png"},
            )

            #
            # create new graph row in database
            #
            print("**Updating graph row with visual file key**")

            sql = "UPDATE graphs SET visualfilekey = %s WHERE graphid = %s;"

            datatier.perform_action(dbConn, sql, [bucketkey, graphid])

            #
            # update local variable visualfilekey
            #
            visualfilekey = bucketkey

        #
        # download file from bucket
        #
        local_filename = "/tmp/local_graph_visual_file.png"

        print("**Downloading visual from S3**")

        bucket.download_file(visualfilekey, local_filename)

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


def make_nx_graph(graph_data):
    #
    # create empty graph
    #
    nx_graph = nx.Graph()

    #
    # add all vertices to graph
    #
    nx_graph.add_nodes_from(graph_data["vertices"])

    #
    # add all edges to graph
    #
    for edge in graph_data["edges"]:
        nx_graph.add_edge(edge[0], edge[1], weight=edge[2])

    return nx_graph

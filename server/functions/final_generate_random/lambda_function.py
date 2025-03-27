#
# Lambda function that handles the generation and
# saving of new random graphs of various types
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
import uuid
import base64
import datatier
import random

from configparser import ConfigParser


def lambda_handler(event, context):
    try:
        print("**STARTING**")
        print("**lambda: final_generate_random**")

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
        # get vertices from request
        #
        print("**Accessing event/queryStringParameters**")

        if "vertices" in event:
            vertices = event["vertices"]
        elif (
            "queryStringParameters" in event and event["queryStringParameters"] != None
        ):
            if "vertices" in event["queryStringParameters"]:
                vertices = event["queryStringParameters"]["vertices"]
            else:
                vertices = -1
        else:
            vertices = -1

        print("Requested graph vertices:", vertices)

        #
        # get edges from request
        #
        print("**Accessing event/queryStringParameters**")

        if "edges" in event:
            edges = event["edges"]
        elif (
            "queryStringParameters" in event and event["queryStringParameters"] != None
        ):
            if "edges" in event["queryStringParameters"]:
                edges = event["queryStringParameters"]["edges"]
            else:
                edges = -1
        else:
            edges = -1

        print("Requested graph edges:", edges)

        #
        # validate passed parameters
        #
        try:
            vertices = int(vertices)
            edges = int(edges)
        except Exception as e:
            return {
                "statusCode": 400,
                "body": {
                    "message": f"vertex or edge count is not an integer",
                    "graphid": -1,
                    "data": "",
                },
            }

        validation_results = validate_parameters(type, vertices, edges)
        if validation_results != None:
            return validation_results

        print("All parameters verified")

        #
        # generate random graph
        #
        generation_results = make_random_graph(type, vertices, edges)
        if "vertices" not in generation_results:
            return {
                "statusCode": 400,
                "body": json.dumps(generation_results),
            }

        bytes = json.dumps(generation_results).encode()

        #
        # copy random graph to file in tmp folder
        #
        print("**Writing local data file**")

        local_filename = "/tmp/local_graph_data_file.json"
        f = open(local_filename, "wb")
        f.write(bytes)
        f.close()

        #
        # generate unique filename in preparation for the S3 upload
        #
        print("**Uploading local file to S3**")

        bucketkey = "graphapp/" + "graph_data_file_" + str(uuid.uuid4()) + ".json"

        print("Using S3 bucketkey:", bucketkey)

        #
        # upload file to S3
        #
        bucket.upload_file(
            local_filename,
            bucketkey,
            ExtraArgs={"ACL": "public-read", "ContentType": "application/json"},
        )

        #
        # open connection to database
        #
        print("**Opening DB connection**")

        dbConn = datatier.get_dbConn(
            rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname
        )

        #
        # create new graph row in database
        #
        print("**Adding graph row to database**")

        sql = """
        INSERT INTO graphs(datafilekey, visualfilekey)
                    VALUES(%s, %s);
        """

        datatier.perform_action(dbConn, sql, [bucketkey, None])

        #
        # grab the graphid that was auto-generated by mysql
        #
        sql = "SELECT LAST_INSERT_ID();"

        row = datatier.retrieve_one_row(dbConn, sql)

        graphid = row[0]

        print("Created row with graphid:", graphid)

        #
        # download file from bucket
        #
        local_filename = "/tmp/local_graph_data_file.json"

        print("**Downloading graph from S3**")

        bucket.download_file(bucketkey, local_filename)

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
            "body": json.dumps(
                {"message": "success", "graphid": graphid, "data": datastr}
            ),
        }

    #
    # error: 500 INTERNAL SERVER ERROR
    #
    except Exception as err:
        print("**ERROR**")
        print(str(err))

        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(err), "graphid": -1, "data": ""}),
        }


def validate_parameters(type, vertices, edges):
    if type not in ["any", "complete", "connected", "acyclic", "tree", "bipartite"]:
        return {
            "statusCode": 400,
            "body": json.dumps(
                {
                    "message": f"graph type {type} is invalid",
                    "graphid": -1,
                    "data": "",
                }
            ),
        }

    if vertices != -1 and vertices <= 0:
        return {
            "statusCode": 400,
            "body": json.dumps(
                {
                    "message": f"vertex count {vertices} is not positive",
                    "graphid": -1,
                    "data": "",
                }
            ),
        }

    if edges != -1 and edges < 0:
        return {
            "statusCode": 400,
            "body": json.dumps(
                {
                    "message": f"edge count {edges} is not non-negative",
                    "graphid": -1,
                    "data": "",
                }
            ),
        }

    if edges != -1 and vertices == -1:
        return {
            "statusCode": 400,
            "body": json.dumps(
                {
                    "message": f"cannot specify number of edges but not number of vertices",
                    "graphid": -1,
                    "data": "",
                }
            ),
        }

    # success case
    return None


def make_random_graph(type, num_vertices=-1, num_edges=-1):
    match type:
        case "any":
            if num_vertices == -1:
                num_vertices = random.randint(5, 10)
            if num_edges == -1:
                num_edges = random.randint(5, num_vertices * (num_vertices - 1) // 2)

            if num_edges > num_vertices * (num_vertices - 1) // 2:
                return {
                    "message": f"number of vertices and edges is illegal",
                    "graphid": -1,
                    "data": "",
                }

            # create all vertices
            vertices = []
            for i in range(num_vertices):
                vertices.append(i)

            # start with all edges existing
            edges = []
            for start in range(num_vertices):
                for end in range(start + 1, num_vertices):
                    edges.append([start, end, random.randint(1, 100)])

            # delete random edges until we get to num_edges
            while len(edges) > num_edges:
                rand_edge = random.randint(0, len(edges) - 1)
                edges.pop(rand_edge)

            return {"vertices": vertices, "edges": edges}

        case "complete":
            if num_vertices == -1:
                num_vertices = random.randint(5, 10)
            if num_edges == -1:
                num_edges = num_vertices * (num_vertices - 1) // 2

            if num_edges != num_vertices * (num_vertices - 1) // 2:
                return {
                    "message": f"number of vertices and edges is illegal",
                    "graphid": -1,
                    "data": "",
                }

            # create all vertices
            vertices = []
            for i in range(num_vertices):
                vertices.append(i)

            # all edges must exist
            edges = []
            for start in range(num_vertices):
                for end in range(start + 1, num_vertices):
                    edges.append([start, end, random.randint(1, 100)])

            return {"vertices": vertices, "edges": edges}

        case "connected":
            if num_vertices == -1:
                num_vertices = random.randint(5, 10)
            if num_edges == -1:
                num_edges = random.randint(
                    num_vertices - 1, num_vertices * (num_vertices - 1) // 2
                )

            if (
                num_edges > num_vertices * (num_vertices - 1) // 2
                or num_edges < num_vertices - 1
            ):
                return {
                    "message": f"number of vertices and edges is illegal",
                    "graphid": -1,
                    "data": "",
                }

            # create all vertices
            vertices = []
            for i in range(num_vertices):
                vertices.append(i)

            # create all possible edges
            edges = []
            remaining_edges = []
            for start in range(num_vertices):
                for end in range(start + 1, num_vertices):
                    remaining_edges.append([start, end, random.randint(1, 100)])

            # start with a tree
            # each vertex can attach itself to prior vertices only
            for i in range(1, num_vertices):
                other = random.randint(0, i - 1)

                selected_edge = None
                for edge in remaining_edges:
                    if edge[0] == other and edge[1] == i:
                        selected_edge = edge
                        break
                if selected_edge == None:
                    raise Exception(
                        "could not find selected edge when building connected graph"
                    )

                edges.append(selected_edge)
                remaining_edges.remove(selected_edge)

            # add remaining edges until we get to num_edges
            while len(edges) < num_edges:
                rand_edge = random.randint(0, len(remaining_edges) - 1)
                selected_edge = remaining_edges[rand_edge]
                edges.append(selected_edge)
                remaining_edges.remove(selected_edge)

            return {"vertices": vertices, "edges": edges}

        case "acyclic":
            if num_vertices == -1:
                num_vertices = random.randint(5, 10)
            if num_edges == -1:
                num_edges = random.randint(4, num_vertices - 1)

            if num_edges > num_vertices - 1:
                return {
                    "message": f"number of vertices and edges is illegal",
                    "graphid": -1,
                    "data": "",
                }

            # create all vertices
            vertices = []
            for i in range(num_vertices):
                vertices.append(i)

            # number of disconnected trees
            spore_count = num_vertices - num_edges

            edges = []
            offset = 0
            for spore in range(spore_count):
                # evenly distribute vertices between spores
                num_spore_vertices = num_vertices // spore_count
                if spore < num_vertices % spore_count:
                    num_spore_vertices += 1

                # each vertex can attach itself to prior spore vertices only
                for i in range(offset + 1, offset + num_spore_vertices):
                    edges.append(
                        [random.randint(offset, i - 1), i, random.randint(1, 100)]
                    )

                offset += num_spore_vertices

            return {"vertices": vertices, "edges": edges}

        case "tree":
            if num_vertices == -1:
                num_vertices = random.randint(5, 10)
            if num_edges == -1:
                num_edges = num_vertices - 1

            if num_edges != num_vertices - 1:
                return {
                    "message": f"number of vertices and edges is illegal",
                    "graphid": -1,
                    "data": "",
                }

            # create all vertices
            vertices = []
            for i in range(num_vertices):
                vertices.append(i)

            # each vertex can attach itself to prior vertices only
            edges = []
            for i in range(1, num_vertices):
                edges.append([random.randint(0, i - 1), i, random.randint(1, 100)])

            return {"vertices": vertices, "edges": edges}

        case "bipartite":
            if num_vertices == -1:
                num_vertices = random.randint(5, 10)
            if num_edges == -1:
                num_edges = random.randint(4, num_vertices * num_vertices // 4)

            if num_edges > num_vertices * num_vertices // 4:
                return {
                    "message": f"number of vertices and edges is illegal",
                    "graphid": -1,
                    "data": "",
                }

            # create all vertices and split into groups
            vertices = []
            group_A = []
            group_B = []
            for i in range(num_vertices):
                vertices.append(i)

                # evenly split into groups
                if i < num_vertices // 2:
                    group_A.append(i)
                else:
                    group_B.append(i)

            # start with all edges existing
            edges = []
            for A in group_A:
                for B in group_B:
                    edges.append([A, B, random.randint(1, 100)])

            # delete random edges until we get to num_edges
            while len(edges) > num_edges:
                rand_edge = random.randint(0, len(edges) - 1)
                edges.pop(rand_edge)

            return {"vertices": vertices, "edges": edges}

        case _:
            return {
                "message": f"graph type {type} is invalid",
                "graphid": -1,
                "data": "",
            }

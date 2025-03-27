#
# Lambda function that handles the asynchronous
# analysis of graphs when triggered by a graph
# analysis request handler function
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
import datatier
import heapq

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
        print("**Accessing jobid from event payload**")

        if "jobid" in event:
            jobid = event["jobid"]
        else:
            jobid = -1  # so database UPDATE doesn't fail
            raise Exception("endpoint requires jobid parameter in event")

        print("Requested jobid:", jobid)

        #
        # get type from request
        #
        print("**Accessing type from event payload**")

        if "type" in event:
            type = event["type"]
        else:
            raise Exception("endpoint requires type parameter in event")

        print("Requested graph type:", type)

        #
        # get root from request
        #
        print("**Accessing root from event payload**")

        if "root" in event:
            root = event["root"]
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
        # get job row from database
        #
        print("**Retrieving job row from database**")

        sql = "SELECT * FROM jobs WHERE jobid = %s;"

        row = datatier.retrieve_one_row(dbConn, sql, [jobid])

        if row == ():  # no such job
            print("**No such job, returning...**")
            raise Exception(f"jobid {jobid} does not exist in the database")

        graphid = row[1]
        status = row[2]

        print("Successfully retrieved row:", row)

        #
        # check that job is currently "processing"
        #
        if status != "processing":
            raise Exception("analysis trigger was activated for a finished job")

        #
        # get graph row from database
        #
        print("**Retrieving graph row from database**")

        sql = "SELECT * FROM graphs WHERE graphid = %s;"

        row = datatier.retrieve_one_row(dbConn, sql, [graphid])

        if row == ():  # no such graph
            print("**No such graph, returning...**")
            raise Exception(f"graphid {graphid} does not exist in the database")

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

        #
        # perform analysis and generate results
        #
        graph_data = json.loads(bytes.decode())

        analysis_results = analyze_graph(type, root, graph_data)

        bytes = json.dumps(analysis_results).encode()

        #
        # copy analysis results to file in tmp folder
        #
        print("**Writing local data file**")

        local_filename = "/tmp/local_analysis_results.json"
        f = open(local_filename, "wb")
        f.write(bytes)
        f.close()

        #
        # generate unique filename in preparation for the S3 upload
        #
        print("**Uploading local file to S3**")

        bucketkey = "graphapp/" + "graph_results_file_" + str(uuid.uuid4()) + ".json"

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
        # success: 200 OK
        # note: not really returning anywhere
        #
        print("**DONE, returning success**")

        #
        # update job status: completed
        #
        print("**Updating job row with results file key and status: completed**")

        sql = "UPDATE jobs SET status = %s, resultsfilekey = %s WHERE jobid = %s;"

        datatier.perform_action(dbConn, sql, ["completed", bucketkey, jobid])

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "success"}),
        }

    #
    # error: 500 INTERNAL SERVER ERROR
    # note: not really returning anywhere
    #
    except Exception as err:
        print("**ERROR**")
        print(str(err))

        #
        # update job status: error
        #
        dbConn = datatier.get_dbConn(
            rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname
        )

        print("**Updating job row with status: error**")

        sql = "UPDATE jobs SET status = %s WHERE jobid = %s;"

        datatier.perform_action(dbConn, sql, ["error", jobid])

        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(err)}),
        }


def analyze_graph(type, root, graph_data):
    if root:
        root = int(root)
    
    # convert graph data to AL
    # { start => { end => weight } }
    graph_AL = {}

    for vertex in graph_data["vertices"]:
        graph_AL[int(vertex)] = {}

    for edge in graph_data["edges"]:
        node_A = int(edge[0])
        node_B = int(edge[1])
        weight = float(edge[2])

        graph_AL[node_A][node_B] = weight
        graph_AL[node_B][node_A] = weight

    match type:
        case "is_connected":
            # select an arbitrary vertex as the root
            root = int(graph_data["vertices"][0])

            # track reachable nodes
            reachable = set([root])

            # track nodes to visit next
            queue = [root]

            # perform BFS to find reachable nodes
            while len(queue) > 0:
                cur = queue.pop(0)

                # all neighbors are reachable
                for destination in graph_AL[cur]:
                    if destination in reachable:
                        continue

                    reachable.add(destination)
                    queue.append(destination)

            # connected implies all nodes are reachable
            for vertex in graph_data["vertices"]:
                if int(vertex) not in reachable:
                    return {"type": "is_connected", "data": False}

            return {"type": "is_connected", "data": True}

        case "has_cycle":
            # track vertices seen by DFS
            seen = set()

            # perform cycle detection on all connected components
            for root in graph_data["vertices"]:
                root = int(root)

                # vertex already seen? connected component already checked
                if root in seen:
                    continue

                # perform DFS for cycle detection
                def dfs(cur, ancestry):
                    seen.add(cur)

                    # check for cycle
                    if cur in ancestry:
                        cycle = ancestry[0:ancestry.index(cur) + 1]
                        cycle.insert(0, cur)
                        return {"type": "has_cycle", "data": cycle}

                    # new ancestry
                    copy = [cur]
                    for node in ancestry:
                        copy.append(node)

                    # check all neighbors
                    for destination in graph_AL[cur]:
                        if len(ancestry) == 0 or destination != ancestry[0]:
                            # propogate result
                            res = dfs(destination, copy)
                            if res:
                                return res

                    return None

                # trigger DFS with root
                res = dfs(root, [])
                if res:
                    return res

            return {"type": "has_cycle", "data": False}

        case "shortest_paths":
            # track pred for each node
            pred = {root: None}

            # track shortest dist for each node
            dist = {root: 0}

            # track visited vertices
            visited = set()

            pq = [(0, root)]

            # find shortest paths with Dijkstra's algorithm
            while len(pq) > 0:
                _, cur = heapq.heappop(pq)

                # skip visited nodes
                if cur in visited:
                    continue

                # mark as visited
                visited.add(cur)

                # relax all neighbors
                for destination in graph_AL[cur]:
                    if destination not in dist or graph_AL[cur][destination] + dist[cur] < dist[destination]:
                        dist[destination] = graph_AL[cur][destination] + dist[cur]
                        pred[destination] = cur
                        heapq.heappush(pq, (dist[destination], destination))

            # collect shortest paths
            shortest_paths = {}
            for vertex in graph_data["vertices"]:
                vertex = int(vertex)

                shortest_paths[vertex] = []

                # node is not reachable
                if vertex not in pred:
                    continue

                # backtrace shortest path
                cur = vertex
                while cur != None:
                    shortest_paths[vertex].insert(0, cur)
                    cur = pred[cur]

            # collect shortest paths with distances
            for vertex in graph_data["vertices"]:
                vertex = int(vertex)

                if vertex in dist:
                    shortest_paths[vertex] = [dist[vertex], shortest_paths[vertex]]
                else:
                    shortest_paths[vertex] = [-1, None]

            return {
                "type": "shortest_paths",
                "data": {"root": root, "paths": shortest_paths},
            }

        case "reachable_nodes":
            # track reachable nodes
            reachable = set([root])

            # track nodes to visit next
            queue = [root]

            # perform BFS to find reachable nodes
            while len(queue) > 0:
                cur = queue.pop(0)

                # all neighbors are reachable
                for destination in graph_AL[cur]:
                    if destination in reachable:
                        continue

                    reachable.add(destination)
                    queue.append(destination)

            return {
                "type": "reachable_nodes",
                "data": {"root": root, "reachable": list(reachable)},
            }

        case "mst":
            # select an arbitrary vertex as the root
            root = int(graph_data["vertices"][0])

            # track reachable nodes
            reachable = set([root])

            # track nodes to visit next
            queue = [root]

            # perform BFS to find reachable nodes
            while len(queue) > 0:
                cur = queue.pop(0)

                # all neighbors are reachable
                for destination in graph_AL[cur]:
                    if destination in reachable:
                        continue
                    
                    reachable.add(destination)
                    queue.append(destination)

            # disconnect implies no MST
            for vertex in graph_data["vertices"]:
                if int(vertex) not in reachable:
                    return {"type": "mst", "data": False}

            # graph is connected, we can find a MST
            pq = []

            # start with outgoing edges from root
            for destination in graph_AL[root]:
                heapq.heappush(pq, (graph_AL[root][destination], root, destination))

            # track edges in MST
            mst_edges = []

            # track vertices in MST
            mst_vertices = [root]

            # find MST with Prim's algorithm
            while len(pq) > 0:
                cur = heapq.heappop(pq)

                # edge no longer on fringe
                if cur[1] in mst_vertices and cur[2] in mst_vertices:
                    continue

                # new edge
                mst_edges.append(cur)

                if cur[1] in mst_vertices:
                    # second edge is new
                    mst_vertices.append(cur[2])

                    # add all fringe vertices
                    for destination in graph_AL[cur[2]]:
                        if destination not in mst_vertices:
                            heapq.heappush(pq, (graph_AL[cur[2]][destination], cur[2], destination))

                else:
                    # first edge is new
                    mst_vertices.append(cur[1])

                    # add all fringe vertices
                    for destination in graph_AL[cur[1]]:
                        if destination not in mst_vertices:
                            heapq.heappush(pq, (graph_AL[cur[1]][destination], cur[1], destination))

            # collect edges in [start, end, weight]
            true_edges = []
            for edge in mst_edges:
                true_edges.append([edge[1], edge[2], edge[0]])

            return {"type": "mst", "data": true_edges}

        case _:
            raise Exception(f"analysis type {type} is invalid")

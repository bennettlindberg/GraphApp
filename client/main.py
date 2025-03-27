#
# Client-side python app for graph app, which is calling
# a set of lambda functions in AWS through API Gateway.
# The overall purpose of the app is to enable quick
# generation, analysis, and visualization of graphs.
#
# Authors:
#   Bennett Lindberg
#
#   Prof. Joe Hummel (initial template, from project03)
#   Northwestern University
#   CS 310
#

import requests
import json

import uuid
import pathlib
import logging
import sys
import base64
import time

from configparser import ConfigParser


############################################################
#
# classes
#
class Graph:

    def __init__(self, row):
        self.graphid = row["graphid"]
        self.datafilekey = row["datafilekey"]
        self.visualfilekey = row["visualfilekey"]


class Job:

    def __init__(self, row):
        self.jobid = row["jobid"]
        self.graphid = row["graphid"]
        self.status = row["status"]
        self.resultsfilekey = row["resultsfilekey"]


###################################################################
#
# web_service_req
#
# When calling servers on a network, calls can randomly fail.
# The better approach is to repeat at least N times (typically
# N=3), and then give up after N tries.
#
def web_service_req(url, action, body=None):
    """
    Submits an HTTP request to a web service at most 3 times, since
    web services can fail to respond e.g. to heavy user or internet
    traffic. If the web service responds with status code 200, 400
    or 500, we consider this a valid response and return the response.
    Otherwise we try again, at most 3 times. After 3 attempts the
    function returns with the last response.

    Parameters
    ----------
    url: url for calling the web service
    action: the HTTP verb to use in the request
    body: body data to provide in the request

    Returns
    -------
    response received from web service
    """

    try:
        retries = 0

        while True:
            if action == "GET":
                response = requests.get(url)
            elif action == "DELETE":
                response = requests.delete(url)
            elif action == "POST":
                response = requests.post(url, json=body)
            else:
                raise Exception("HTTP verb must be in ['GET', 'POST', 'DELETE']")

            if response.status_code in [200, 400, 404, 481, 482, 500]:
                #
                # we consider this a successful call and response
                #
                break

            #
            # failed, try again?
            #
            retries = retries + 1
            if retries < 3:
                # try at most 3 times
                time.sleep(retries)
                continue

            #
            # if get here, we tried 3 times, we give up:
            #
            break

        return response

    except Exception as e:
        print("**ERROR**")
        logging.error("web_service_req() failed:")
        logging.error("url: " + url)
        logging.error(e)
        return None


############################################################
#
# prompt
#
def prompt():
    """
    Prompts the user and returns the command number

    Parameters
    ----------
    None

    Returns
    -------
    Command number entered by user (0, 1, 2, ...)
    """
    try:
        print()
        print(">> Enter a command:")
        print("   0 => end")
        print("   1 => make new graph")
        print("   2 => retrieve graph")
        print("   3 => delete graph")
        print("   4 => get all graph rows")
        print("   5 => get all job rows")
        print("   6 => delete all jobs")
        print("   7 => visualize graph")
        print("   8 => make random graph")
        print("   9 => start graph analysis")
        print("  10 => get analysis results")

        cmd = input()

        if cmd == "":
            cmd = -1
        elif not cmd.isnumeric():
            cmd = -1
        else:
            cmd = int(cmd)

        return cmd

    except Exception as e:
        print("**ERROR")
        print("**ERROR: invalid input")
        print("**ERROR")
        return -1


############################################################
#
# make_new_graph
#
def make_new_graph(baseurl):
    """
    Prompts the user for a local filename and uploads
    that graph data file to the application.

    Parameters
    ----------
    baseurl: baseurl for web service

    Returns
    -------
    nothing
    """

    try:
        print("Enter path to graph data file>")
        local_filename = input()

        #
        # check file extension
        #
        if len(local_filename) < 5 or local_filename[-5:] != ".json":
            print(f"Graph data file {local_filename} does not end in '.json'...")
            return

        #
        # check that file exists
        #
        if not pathlib.Path(local_filename).is_file():
            print(f"Graph data file {local_filename} does not exist...")
            return

        #
        # build the data packet. First step is read the graph data file
        # as raw bytes:
        #
        infile = open(local_filename, "rb")
        bytes = infile.read()
        infile.close()

        #
        # now encode the file as base64. Note b64encode returns
        # a bytes object, not a string. So then we have to convert
        # (decode) the bytes -> string, and then we can serialize
        # the string as JSON for upload to server:
        #
        datastr = base64.b64encode(bytes).decode()
        data = {"data": datastr}

        #
        # call the web service:
        #
        api = "/graph"
        url = baseurl + api

        res = web_service_req(url, "POST", data)

        #
        # let's look at what we got back:
        #
        if res.status_code == 200:  # success
            pass
        elif res.status_code == 400:  # various errors
            body = res.json()
            print("Bad request...")
            print("Error message:", body["message"])
            return
        else:
            # failed:
            print("Failed with status code:", res.status_code)
            print("url: " + url)
            if res.status_code == 500:
                # we'll have an error message
                body = res.json()
                print("Error message:", body["message"])
            #
            return

        #
        # success, extract graphid:
        #
        body = res.json()
        graphid = body["graphid"]

        print("Graph successfully uploaded, graph id =", graphid)
        return

    except Exception as e:
        logging.error("**ERROR: make_new_graph() failed:")
        logging.error("url: " + url)
        logging.error(e)
        return


############################################################
#
# retrieve_graph
#
def retrieve_graph(baseurl):
    """
    Prompts the user for the graph id, then retrieves
    the graph data file for that graph.

    Parameters
    ----------
    baseurl: baseurl for web service

    Returns
    -------
    nothing
    """

    try:
        print("Enter graph id>")
        graphid = input()

        #
        # call the web service:
        #
        api = "/graph/"
        url = baseurl + api + graphid

        res = web_service_req(url, "GET")

        #
        # let's look at what we got back:
        #
        if res.status_code == 200:  # success
            pass
        elif res.status_code == 404:  # no such graph
            print(f"Graph {graphid} does not exist in the database...")
            return
        else:
            # failed:
            print("Failed with status code:", res.status_code)
            print("url: " + url)
            if res.status_code == 500:
                # we'll have an error message
                body = res.json()
                print("Error message:", body["message"])
            return

        #
        # if we get here, status code was 200, so we
        # have results to deserialize and save:
        #
        body = res.json()
        datastr = body["data"]

        #
        # encode the data string to obtain the raw bytes in base64,
        # then call b64decode to obtain the original raw bytes.
        # Finally, decode() the bytes to obtain the results as a
        # printable string.
        #
        base64_bytes = datastr.encode()
        bytes = base64.b64decode(base64_bytes)
        json_data = json.loads(bytes.decode())

        #
        # generate a unique filename for saving
        #
        new_file_name = "graph_data_" + str(graphid) + "_" + str(uuid.uuid4()) + ".json"

        #
        # write graph data file to local file
        #
        with open(new_file_name, "w") as file:
            json.dump(json_data, file, indent=4)

        print(f"Saved graph {graphid} data file to {new_file_name}...")
        return

    except Exception as e:
        logging.error("**ERROR: retrieve_graph() failed:")
        logging.error("url: " + url)
        logging.error(e)
        return


############################################################
#
# delete_graph
#
def delete_graph(baseurl):
    """
    Prompts the user for the graph id, then deletes
    that graph.

    Parameters
    ----------
    baseurl: baseurl for web service

    Returns
    -------
    nothing
    """

    try:
        print("Enter graph id>")
        graphid = input()

        #
        # call the web service:
        #
        api = "/graph/"
        url = baseurl + api + graphid

        res = web_service_req(url, "DELETE")

        #
        # let's look at what we got back:
        #
        if res.status_code == 200:  # success
            print(f"Successfully deleted graph {graphid}...")
        elif res.status_code == 404:  # no such graph
            print(f"Graph {graphid} does not exist in the database...")
        else:
            # failed:
            print("Failed with status code:", res.status_code)
            print("url: " + url)
            if res.status_code == 500:
                # we'll have an error message
                body = res.json()
                print("Error message:", body["message"])

    except Exception as e:
        logging.error("**ERROR: delete_graph() failed:")
        logging.error("url: " + url)
        logging.error(e)
        return


############################################################
#
# get_all_graph_rows
#
def get_all_graph_rows(baseurl):
    """
    Prints out all the graphs in the database

    Parameters
    ----------
    baseurl: baseurl for web service

    Returns
    -------
    nothing
    """

    try:
        #
        # call the web service:
        #
        api = "/graphs"
        url = baseurl + api

        res = web_service_req(url, "GET")

        #
        # let's look at what we got back:
        #
        if res.status_code == 200:  # success
            pass
        else:
            # failed:
            print("Failed with status code:", res.status_code)
            print("url: " + url)
            if res.status_code == 500:
                # we'll have an error message
                body = res.json()
                print("Error message:", body["message"])
            #
            return

        #
        # deserialize and extract graphs:
        #
        body = res.json()

        #
        # let's map each row into a Graph object:
        #
        graphs = []
        for row in body["data"]:
            graph = Graph(row)
            graphs.append(graph)

        #
        # Now we can think OOP:
        #
        if len(graphs) == 0:
            print("No graphs found in the database...")
            return

        for graph in graphs:
            print(
                f"{graph.graphid}:\n\tData file key: {graph.datafilekey}\n\tVisual file key: {graph.visualfilekey}"
            )

        return

    except Exception as e:
        logging.error("**ERROR: get_all_graph_rows() failed:")
        logging.error("url: " + url)
        logging.error(e)
        return


############################################################
#
# get_all_job_rows
#
def get_all_job_rows(baseurl):
    """
    Prints out all the jobs in the database

    Parameters
    ----------
    baseurl: baseurl for web service

    Returns
    -------
    nothing
    """

    try:
        #
        # call the web service:
        #
        api = "/jobs"
        url = baseurl + api

        res = web_service_req(url, "GET")

        #
        # let's look at what we got back:
        #
        if res.status_code == 200:  # success
            pass
        else:
            # failed:
            print("Failed with status code:", res.status_code)
            print("url: " + url)
            if res.status_code == 500:
                # we'll have an error message
                body = res.json()
                print("Error message:", body["message"])
            #
            return

        #
        # deserialize and extract graphs:
        #
        body = res.json()

        #
        # let's map each row into a Graph object:
        #
        jobs = []
        for row in body["data"]:
            job = Job(row)
            jobs.append(job)

        #
        # Now we can think OOP:
        #
        if len(jobs) == 0:
            print("No jobs found in the database...")
            return

        for job in jobs:
            print(
                f"{job.jobid}:\n\tGraph ID: {job.graphid}\n\tStatus: {job.status}\n\tResults file key: {job.resultsfilekey}"
            )

        return

    except Exception as e:
        logging.error("**ERROR: get_all_job_rows() failed:")
        logging.error("url: " + url)
        logging.error(e)
        return


############################################################
#
# get_all_job_rows
#
def delete_all_jobs(baseurl):
    """
    Deletes all the jobs in the database and corresponding
    results files from the S3 bucket

    Parameters
    ----------
    baseurl: baseurl for web service

    Returns
    -------
    nothing
    """

    try:
        #
        # call the web service:
        #
        api = "/jobs"
        url = baseurl + api

        res = web_service_req(url, "DELETE")

        #
        # let's look at what we got back:
        #
        if res.status_code == 200:  # success
            print("Successfully deleted all jobs...")
        else:
            # failed:
            print("Failed with status code:", res.status_code)
            print("url: " + url)
            if res.status_code == 500:
                # we'll have an error message
                body = res.json()
                print("Error message:", body["message"])

    except Exception as e:
        logging.error("**ERROR: delete_all_jobs() failed:")
        logging.error("url: " + url)
        logging.error(e)
        return


############################################################
#
# visualize_graph
#
def visualize_graph(baseurl):
    """
    Prompts the user for the graph id, then retrieves
    the graph visualization file for that graph.

    Parameters
    ----------
    baseurl: baseurl for web service

    Returns
    -------
    nothing
    """

    try:
        print("Enter graph id>")
        graphid = input()

        #
        # call the web service:
        #
        api = "/visual/"
        url = baseurl + api + graphid

        res = web_service_req(url, "GET")

        #
        # let's look at what we got back:
        #
        if res.status_code == 200:  # success
            pass
        elif res.status_code == 404:  # no such graph
            print(f"Graph {graphid} does not exist in the database...")
            return
        else:
            # failed:
            print("Failed with status code:", res.status_code)
            print("url: " + url)
            if res.status_code == 500:
                # we'll have an error message
                body = res.json()
                print("Error message:", body["message"])
            return

        #
        # if we get here, status code was 200, so we
        # have results to deserialize and save:
        #
        body = res.json()
        datastr = body["data"]

        #
        # encode the data string to obtain the raw bytes in base64,
        # then call b64decode to obtain the original raw bytes.
        #
        base64_bytes = datastr.encode()
        bytes = base64.b64decode(base64_bytes)

        #
        # generate a unique filename for saving
        #
        new_file_name = (
            "graph_visual_" + str(graphid) + "_" + str(uuid.uuid4()) + ".png"
        )

        #
        # write graph data file to local file
        #
        with open(new_file_name, "wb") as file:
            file.write(bytes)

        print(f"Saved graph {graphid} visualization file to {new_file_name}...")
        return

    except Exception as e:
        logging.error("**ERROR: visualize_graph() failed:")
        logging.error("url: " + url)
        logging.error(e)
        return


############################################################
#
# make_random_graph
#
def make_random_graph(baseurl):
    """
    Prompts the user for a type of graph, then
    generates and returns a graph data file containing
    the randomly-generated graph.

    Parameters
    ----------
    baseurl: baseurl for web service

    Returns
    -------
    nothing
    """

    try:
        print("Supported graph types: any, connected, complete, acyclic, tree, bipartite")
        print("Enter graph type>")
        graph_type = input()

        #
        # check graph type
        #
        if graph_type not in [
            "any",
            "connected",
            "complete",
            "acyclic",
            "tree",
            "bipartite",
        ]:
            print(f"Type {graph_type} is not a valid graph type")
            return

        print("Enter number of vertices (-1 to skip)>")
        num_vertices = input()

        #
        # check number of vertices
        #
        if num_vertices == "-1":
            pass
        elif (
            not num_vertices.isnumeric()
            or int(num_vertices) != float(num_vertices)
            or int(num_vertices) < 0
        ):
            print(f"Number {num_vertices} is not a valid number of vertices")
            return

        print("Enter number of edges (-1 to skip)>")
        num_edges = input()

        #
        # check number of vertices
        #
        if num_edges == "-1":
            pass
        elif (
            not num_edges.isnumeric()
            or int(num_edges) != float(num_edges)
            or int(num_edges) < 0
        ):
            print(f"Number {num_edges} is not a valid number of edges")
            return

        #
        # call the web service:
        #
        api = "/random/"
        qp_list = []
        if num_vertices != "-1":
            qp_list.append(f"vertices={num_vertices}")
        if num_edges != "-1":
            qp_list.append(f"edges={num_edges}")
        qp = "?" + "&".join(qp_list)
        url = baseurl + api + graph_type + qp

        res = web_service_req(url, "GET")

        #
        # let's look at what we got back:
        #
        if res.status_code == 200:  # success
            pass
        elif res.status_code == 400:  # various errors
            body = res.json()
            print(f"Random graph could not be generated...")
            print("Error message:", body["message"])
            return
        else:
            # failed:
            print("Failed with status code:", res.status_code)
            print("url: " + url)
            if res.status_code == 500:
                # we'll have an error message
                body = res.json()
                print("Error message:", body["message"])
            return

        #
        # if we get here, status code was 200, so we
        # have results to deserialize and save:
        #
        body = res.json()
        datastr = body["data"]

        #
        # encode the data string to obtain the raw bytes in base64,
        # then call b64decode to obtain the original raw bytes.
        # Finally, decode() the bytes to obtain the results as a
        # printable string.
        #
        base64_bytes = datastr.encode()
        bytes = base64.b64decode(base64_bytes)
        json_data = json.loads(bytes.decode())

        #
        # generate a unique filename for saving
        #
        new_file_name = "graph_data_random_" + str(uuid.uuid4()) + ".json"

        #
        # write graph data file to local file
        #
        with open(new_file_name, "w") as file:
            json.dump(json_data, file, indent=4)

        print(f"Saved random graph {body["graphid"]} data file to {new_file_name}...")
        return

    except Exception as e:
        logging.error("**ERROR: make_random_graph() failed:")
        logging.error("url: " + url)
        logging.error(e)
        return


############################################################
#
# start_graph_analysis
#
def start_graph_analysis(baseurl):
    """
    Starts a asynchronous job to analyze the
    specified graph in the specified way.

    Parameters
    ----------
    baseurl: baseurl for web service

    Returns
    -------
    nothing
    """

    try:
        print("Enter graph id>")
        graphid = input()

        print("Supported analysis types: is_connected, has_cycle, shortest_paths, reachable_nodes, mst")
        print("Enter analysis type>")
        analysis_type = input()

        #
        # check analysis type
        #
        if analysis_type not in [
            "is_connected",
            "has_cycle",
            "shortest_paths",
            "reachable_nodes",
            "mst",
        ]:
            print(f"Type {analysis_type} is not a valid analysis type")
            return

        #
        # get root identifier for some types of analysis
        #
        if analysis_type in ["shortest_paths", "reachable_nodes"]:
            print("Enter root vertex identifier>")
            root_id = input()

            #
            # check root identifier
            #
            if not root_id.isnumeric() or int(root_id) != float(root_id):
                print(f"Identifier {root_id} is not a valid root vertex identifier")
                return

            #
            # call the web service:
            #
            api = "/analysis/"
            url = baseurl + api + graphid + "/" + analysis_type + f"?root={root_id}"

        else:
            #
            # call the web service:
            #
            api = "/analysis/"
            url = baseurl + api + graphid + "/" + analysis_type

        res = web_service_req(url, "GET")

        #
        # let's look at what we got back:
        #
        if res.status_code == 200:  # success
            pass
        elif res.status_code == 404:  # no such graph
            print(f"Graph {graphid} does not exist in the database...")
            return
        elif res.status_code == 400:  # various errors
            body = res.json()
            print(f"Could not start a graph analysis job...")
            print("Error message:", body["message"])
            return
        else:
            # failed:
            print("Failed with status code:", res.status_code)
            print("url: " + url)
            if res.status_code == 500:
                # we'll have an error message
                body = res.json()
                print("Error message:", body["message"])
            return

        #
        # if we get here, status code was 200, so we
        # have a jobid for the analysis job
        #
        body = res.json()
        jobid = body["jobid"]

        print(f"Successfully started a graph analysis job, job id = {jobid}")
        return

    except Exception as e:
        logging.error("**ERROR: start_graph_analysis() failed:")
        logging.error("url: " + url)
        logging.error(e)
        return


############################################################
#
# get_analysis_results
#
def get_analysis_results(baseurl):
    """
    Retrieve the status and results (if available) of
    the specified analysis job.

    Parameters
    ----------
    baseurl: baseurl for web service

    Returns
    -------
    nothing
    """

    try:
        print("Enter job id>")
        jobid = input()

        #
        # call the web service:
        #
        api = "/results/"
        url = baseurl + api + jobid

        res = web_service_req(url, "GET")

        #
        # let's look at what we got back:
        #
        if res.status_code == 200:  # success
            pass
        elif res.status_code == 481:  # not ready
            print(f"Job {jobid} is still processing...")
            return
        elif res.status_code == 482:  # unknown error
            print(f"Job {jobid} terminated with an unknown error...")
            return
        elif res.status_code == 404:  # no such job
            print(f"Job {jobid} does not exist in the database...")
            return
        else:
            # failed:
            print("Failed with status code:", res.status_code)
            print("url: " + url)
            if res.status_code == 500:
                # we'll have an error message
                body = res.json()
                print("Error message:", body["message"])
            return

        #
        # if we get here, status code was 200, so we
        # have results to deserialize and save:
        #
        body = res.json()
        datastr = body["data"]

        #
        # encode the data string to obtain the raw bytes in base64,
        # then call b64decode to obtain the original raw bytes.
        # Finally, decode() the bytes to obtain the results as a
        # printable string.
        #
        base64_bytes = datastr.encode()
        bytes = base64.b64decode(base64_bytes)
        json_data = json.loads(bytes.decode())

        #
        # generate a unique filename for saving
        #
        new_file_name = "job_analysis_" + str(jobid) + "_" + str(uuid.uuid4()) + ".json"

        #
        # write graph analysis file to local file
        #
        with open(new_file_name, "w") as file:
            json.dump(json_data, file, indent=4)

        print(f"Saved job {jobid} analysis results to {new_file_name}...")
        return

    except Exception as e:
        logging.error("**ERROR: get_analysis_results() failed:")
        logging.error("url: " + url)
        logging.error(e)
        return


############################################################
# main
#
try:
    print("** Welcome to GraphApp **")
    print()

    # eliminate traceback so we just get error message:
    sys.tracebacklimit = 0

    #
    # what config file should we use for this session?
    #
    config_file = "graphapp-client-config.ini"

    print("Config file to use for this session?")
    print("Press ENTER to use default, or")
    print("enter config file name>")
    s = input()

    if s == "":  # use default
        pass  # already set
    else:
        config_file = s

    #
    # does config file exist?
    #
    if not pathlib.Path(config_file).is_file():
        print("**ERROR: config file '", config_file, "' does not exist, exiting")
        sys.exit(0)

    #
    # setup base URL to web service:
    #
    configur = ConfigParser()
    configur.read(config_file)
    baseurl = configur.get("client", "webservice")

    #
    # make sure baseurl does not end with /, if so remove:
    #
    if len(baseurl) < 16:
        print("**ERROR: baseurl '", baseurl, "' is not nearly long enough...")
        sys.exit(0)

    if baseurl == "https://YOUR_GATEWAY_API.amazonaws.com":
        print("**ERROR: update config file with your gateway endpoint")
        sys.exit(0)

    if baseurl.startswith("http:"):
        print("**ERROR: your URL starts with 'http', it should start with 'https'")
        sys.exit(0)

    lastchar = baseurl[len(baseurl) - 1]
    if lastchar == "/":
        baseurl = baseurl[:-1]

    #
    # main processing loop:
    #
    cmd = prompt()

    while cmd != 0:
        #
        if cmd == 1:
            make_new_graph(baseurl)
        elif cmd == 2:
            retrieve_graph(baseurl)
        elif cmd == 3:
            delete_graph(baseurl)
        elif cmd == 4:
            get_all_graph_rows(baseurl)
        elif cmd == 5:
            get_all_job_rows(baseurl)
        elif cmd == 6:
            delete_all_jobs(baseurl)
        elif cmd == 7:
            visualize_graph(baseurl)
        elif cmd == 8:
            make_random_graph(baseurl)
        elif cmd == 9:
            start_graph_analysis(baseurl)
        elif cmd == 10:
            get_analysis_results(baseurl)
        else:
            print("** Unknown command, try again...")
        #
        cmd = prompt()

    #
    # done
    #
    print()
    print("** done **")
    sys.exit(0)

except Exception as e:
    logging.error("**ERROR: main() failed:")
    logging.error(e)
    sys.exit(0)

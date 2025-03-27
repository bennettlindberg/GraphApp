#
# Lambda function that handles the retrieval of all
# graphs from the database by querying the database
# for all rows
#
# Authors:
#   Bennett Lindberg
#
#   Prof. Joe Hummel (initial template, from project03)
#   Northwestern University
#   CS 310
#

import json
import os
import datatier

from configparser import ConfigParser


def lambda_handler(event, context):
    try:
        print("**STARTING**")
        print("**lambda: final_get_all_graphs**")

        #
        # set up AWS based on config file
        #
        config_file = "graphapp-config.ini"
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = config_file

        configur = ConfigParser()
        configur.read(config_file)

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
        # get all graph rows from database
        #
        formatted_rows = []

        print("**Retrieving all graph rows from database**")

        sql = "SELECT * FROM graphs;"

        rows = datatier.retrieve_all_rows(dbConn, sql)

        for row in rows:
            formatted_rows.append(
                {"graphid": row[0], "datafilekey": row[1], "visualfilekey": row[2]}
            )

        #
        # success: 200 OK
        #
        print("**DONE, returning success**")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "success", "data": formatted_rows}),
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

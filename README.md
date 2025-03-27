# Graph App: Graph Analysis and Visualization Suite

- Author: Bennett Lindberg
- Date: March 21, 2025

## Contents

All code and configuration files needed to set up the project are provided in this directory. The primary components are located as follows:

- Client: client/
    - Dockerfile: client/docker/Dockerfile
    - REPL code: client/main.py
    - Test data files: client/test-graphs/
- Server: server/
    - Custom Lambda layer: server/layers/final-project-layer-bin.zip
    - Lambda functions code: server/functions/
- Database: database/
    - Schema file: database/graphapp-database.sql

## Installation

The following sections provide high-level instructions for installing and running Graph App. Four major components must be set up: a client front-end, an API Gateway-built REST API, an AWS Lambda-based compute tier, and an RDS database. Once all configuration is set up, the client front-end should be able to contact the server via the REST API.

### RDS Database

1. Navigate to the "database" directory, which contains the SQL script to set up the necessary database and database users.

2. Create a new (or reuse a previously-created) RDS MySQL database on AWS.

3. Open a connection to your RDS MySQL database (such as in VSCode using the MySQL extension).

4. Run the "client/graphapp-database.sql" script to create a new database, tables, and users in the RDS database.

### Compute Tier (AWS Lambda)

1. Navigate to the "server/layers" directory, which contains dependency files and a AWS Lambda layer .zip file.

2. Graph App requires four Python packages not directly supported by AWS: pymysql, numpy, networkx, and matplotlib. Locate the file "server/layers/final-project-layer-bin.zip", which contains these dependencies in a format accepted by AWS.

3. Upload the "server/layers/final-project-layer-bin.zip" to any AWS S3 bucket.

4. Create a new Lambda layer based on the "server/layers/final-project-layer-bin.zip" file by importing from your S3 bucket.

5. If for any reason the "server/layers/final-project-layer-bin.zip" was not accepted by AWS (you'll be able to tell when import errors occur in your Lambda functions), you can build the layer yourself. To do so, create a new Python virtual environment (venv) and install the requirements in "server/layers/requirements.txt", which references the dependency files in the same directory. Then, use the following commands to create a "layer_content.zip" layer file:

```bash
mkdir python
cp -r [PATH_TO_VENV]/lib python/
zip -r layer_content.zip python
```

6. With the lambda layer created, you can create the actual lambda functions. There are 11 lambda functions in total, all listed in the "server/functions" directory.

7. Update the "server/functions/graphapp-config.ini" file with your S3 bucket information, RDS endpoint, user role access keys. Copy the updated file over to each lambda function's individual folder.

8. Compress each lambda function's individual folder into a .zip file.

9. Create the 11 lambda functions on AWS Lambda with names corresponding to the folder names in "server/functions". For each lambda function, set the timeout to 5 minutes, add the layer you created earlier, and upload the .zip file for the lambda function as the code source.

10. For the lambda function "final_perform_analysis", you may want to increase the timeout to 10 minutes to accommodate the analysis of large graphs.

11. For the lambda function "final_start_analysis", you will need to add the permission policy "AWSLambdaRole" to the lambda function's execution role. This is because this lambda function invokes another lambda function. If you still get a invocation permissions error when running the lambda function, try adding "AWSLambda_FullAccess" to the S3 profile used in the lambda function's "lambda_function.py" file (currently "s3readwrite").

12. Create a folder named "graphapp" in the S3 bucket you referred to in the configuration file in step 7. Many of the lambda functions assume the existence of this folder.

### REST API (API Gateway)

1. Create a new REST API in API Gateway.

2. Navigate to the "server/api" directory, which contains a standard API specification file.

3. Use the API import feature to automatically generate a replica of the API described in the "server/api/GraphAPI-prod-oas30-apigateway.json" specification file.

4. You may need to manually change the specific Lambda functions being called by each endpoint. There are 10 endpoints in total, and each one has a corresponding lambda function. The names of the lambda functions should sufficiently indicate which endpoint to which they correspond.

5. If the API import did not succeed, you will need to manually set up the API. Start by recreating the endpoint tree visualized in the "server/api/GraphAPI_visual.png" file.

6. Next, if the API import did not succeed, specify the query string parameters expected by each endpoint. Only two endpoints expect query string parameters: "GET /random/:type", which optionally takes "vertices" and "edges" as query parameters, and "GET /analysis/:graphid/:type", which optionally takes "root" as a query parameter.

7. Finally, if the API import did not succeed, select the corresponding lambda function for each endpoint to run when the endpoint is called (as described by step 4). 

### Client Front-End

1. Navigate to the "client" directory, which contains the Dockerfile, image build script, container run script, and client code (main.py).

2. Update the "client/graphapp-client-config.ini" file to refer to your own API Gateway endpoint.

3. Inspect the "client/_readme.txt" file and follow the directions to build and run the necessary docker container.

4. When inside of the docker container (running interactively), enter the command "python3 main.py" to run the client.

5. Follow the prompts on the screen to select the correct configuration file and subsequently interact with the server.

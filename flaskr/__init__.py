import os
import json

from flask import Flask, request
from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode, TableName
from tableauhyperapi import escape_string_literal
import tableauserverclient as TSC
import uuid

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    @app.route('/private/tableau/insert', methods=["POST"])
    def insertRowsIntoTableau():
        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, 'datamin' ) as hyper:
            input = json.loads(request.get_data())
            columns = []
            for col in input["table"]["columns"]:
                columns.append(TableDefinition.Column(col["name"], sqlTypeFromString(col["type"])))


            PATH_TO_HYPER = '/tmp/'+str(uuid.uuid4())+'.hyper'

            # Step 1: Start a new private local Hyper instance
            

            # Step 2:  Create the the .hyper file, replace it if it already exists
            with Connection(endpoint=hyper.endpoint, 
                            create_mode=CreateMode.CREATE_AND_REPLACE,
                            database=PATH_TO_HYPER) as connection:

            # Step 3: Create the schema
                    connection.catalog.create_schema('Extract')

            # Step 4: Create the table definition
                    schema = TableDefinition(table_name=TableName('Extract', 'Extract'),
                        columns=columns)
                
            # Step 5: Create the table in the connection catalog
                    connection.catalog.create_table(schema)
                
                    with Inserter(connection, schema) as inserter:
                        inserter.add_rows(input["table"]["rows"])
                        inserter.execute()


            server_address = input["connection"]["server"]
            username = input["connection"]["username"]
            password = input["connection"]["password"]
            sitename = input["connection"]["site_name"]
            project_name = input["connection"]["project_name"]
            datasource_name = input["connection"]["datasource_name"]

            tableau_auth = TSC.TableauAuth(username, password, sitename)
            server = TSC.Server(server_address)

            with server.auth.sign_in(tableau_auth):
                all_projects, pagination_item = server.projects.get()
                found = False
                for project in all_projects:
                    if project.name == project_name:
                        found = True
                        break

                if not found:
                    return "Project not found", 404

                all_datasources, pagination_item = server.datasources.get()
                found = False
                for datasource in all_datasources:
                    if datasource.name == datasource_name and datasource.project_id == project.id:
                        found = True
                        break

                if found:
                    server.datasources.publish(datasource, PATH_TO_HYPER, tableauMode(input["mode"]))
                    return "", 200
                else:
                    ds = TSC.DatasourceItem(project.id)
                    ds.name = datasource_name
                    server.datasources.publish(ds, PATH_TO_HYPER, "CreateNew")
                    return "", 200
            return

    return app

def sqlTypeFromString(typeStr):
    types = {
        "int": SqlType.int(),
        "varchar": SqlType.varchar(255),
        "double": SqlType.double(),
        "timestamp": SqlType.timestamp(),
    }

    return types[typeStr]

def tableauMode(mode):
    types = {
        "append": "Append",
        "overwrite": "Overwrite",
        "create_new": "CreateNew",
    }

    return types[mode]
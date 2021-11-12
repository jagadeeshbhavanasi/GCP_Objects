# [START imports]
import json
from collections import OrderedDict
from copy import deepcopy
from google.cloud import bigquery


project_id = "gcp-project-314410"
objects_details = "BigQuery/objects_details.json"

## Creating a native table in BigQuery and creating schema from a json file.
def native_table_creation(project_id, dataset_name, table_name, json_schema_uri, labels):
    try:
        # Construct a BigQuery client object.
        client = bigquery.Client(project = project_id)
    except:
        return f"WARNING: Unable to access the project {project_id}."

    #Definig SchemaField for table creation
    bigquerySchema = []
    with open(json_schema_uri) as f:
        bigqueryColumns = json.load(f)
        for col in bigqueryColumns:
            bigquerySchema.append(bigquery.SchemaField(col['name'], col['type'], col['mode']))

    #Definig table structure for table creation
    tableRef = f"{project_id}.{dataset_name}.{table_name}"
    table = bigquery.Table(tableRef, schema=bigquerySchema)

    try:
        # Raises:- google.cloud.exceptions.Conflict – If the table already exists.
        table = client.create_table(table)

        # Adding labels to the table
        table.labels = labels
        table = client.update_table(table, ["labels"])  # API request

        return table
    except Exception as e:
        print(e)
        return e


## Creating and external_table with schema from json file
def external_table_creation(project_id, dataset_name, ext_table_name, json_schema_uri, source_format, source_uris, labels):

    try:
        # Construct a BigQuery client object.
        client = bigquery.Client(project = project_id)
    except:
        return f"WARNING: Unable to access the project {project_id}."

    #Definig SchemaField for table creation
    bigquerySchema = []
    with open(json_schema_uri) as f:
        bigqueryColumns = json.load(f)
        for col in bigqueryColumns:
            bigquerySchema.append(bigquery.SchemaField(col['name'], col['type'], col['mode']))

    #Definig table structure for table creation
    tableRef = f"{project_id}.{dataset_name}.{ext_table_name}"
    table = bigquery.Table(tableRef, schema=bigquerySchema)

    # Definig external table
    external_config = bigquery.ExternalConfig(source_format)
    external_config.autodetect = False
    external_config.schema = bigquerySchema
    external_config.source_uris = source_uris
    table.external_data_configuration = external_config

    # For more details on parameters and attributes visit on the link
    # https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.external_config.ExternalConfig.html#google.cloud.bigquery.external_config.ExternalConfig

    try:
        # Raises:- google.cloud.exceptions.Conflict – If the table already exists.
        table = client.create_table(table)

        # Adding labels to the table
        table.labels = labels
        table = client.update_table(table, ["labels"])  # API request

        return table
    except Exception as e:
        print(e)
        return e


#Create dataset in Project at default dataset location
def create_dataset(project_id, dataset_name, dataset_location):
    # Set dataset_id to the ID of the dataset to create.
    # dataset_id = "{}.your_dataset".format(client.project)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(f'{project_id}.{dataset_name}')

    # Specify the geographic location where the dataset should reside.
    dataset.location = dataset_location

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    try:
        dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
        return dataset
    except Exception as e:
        return e


# Getting the table schema from BigQuery
def get_table_schema(project_id, dataset_name, table_name):

    bq_table_schema = []

    client = bigquery.Client()
    table = client.get_table(f"{project_id}.{dataset_name}.{table_name}")  # Make an API request.

    # View table properties
    print("Got table schema for '{}.{}.{}'.".format(table.project, table.dataset_id, table.table_id))

    for schema in table.schema:
        bq_table_schema.append({"name":schema.name,"type":schema.field_type,"mode":schema.mode})

    return bq_table_schema


# Returning the changed_columns in the current schema of the table
def changed_columns(cur_schema,updated_schema):

    #Comparing both schemas and removing the same Columns
    #At last only the extra column will be left which will removed or added based on condition
    temp1 = deepcopy(cur_schema)
    temp2 = deepcopy(updated_schema)

    if len(updated_schema)>len(cur_schema):
        for col1 in cur_schema:
            for col2 in updated_schema:
                if col1['name']==col2['name']:
                    if col1['type']==col2['type']:
                        temp1.remove(col1)
                        temp2.remove(col2)
        return temp2

    else:
        for col1 in updated_schema:
            for col2 in cur_schema:
                if col1['name']==col2['name']:
                    if col1['type']==col2['type']:
                        temp1.remove(col2)
                        temp2.remove(col1)
        return temp1


# Checking if the labels for the object have been changed
def changed_lables(cur_lables, updated_lables):

    dict1 = OrderedDict(sorted(cur_lables.items()))
    dict2 = OrderedDict(sorted(updated_lables.items()))

    if dict1 == dict2:
        return
    else:
        return updated_lables


# Checking and applying the changes for the native table
def native_table_changes(project_id, dataset_name, table_name, json_schema_uri, labels):
    with open(json_schema_uri) as file:
        updated_schema = json.load(file)
    cur_schema = get_table_schema(project_id, dataset_name, table_name)

    # Construct a BigQuery client object.
    client = bigquery.Client()

    ## Check If the columns have been added or removed or the number of columns are same
    if len(updated_schema)!=len(cur_schema):

        #Additional Columns
        if len(updated_schema)>len(cur_schema):
            print(f"\n{updated_schema}\n{cur_schema}\n")
            print("\nAdding new column/s . . .\n")

            #to find the Additional Columns
            Columns = changed_columns(cur_schema,updated_schema)

            table = client.get_table(f"{project_id}.{dataset_name}.{table_name}")

            original_schema = table.schema
            try:
                new_schema = original_schema[:]  # Creates a copy of the schema.

                # Adding the Additional Columns
                for col in Columns:
                    if col['mode'].lower() == "required":
                        raise Exception(f"Cannot add mode='REQUIRED' for fields to an existing schema.\n{col['name']} {col['type']} {col['mode']}")
                    new_schema.append(bigquery.SchemaField(col['name'], col['type'], col['mode']))

                table.schema = new_schema
                table = client.update_table(table, ["schema"])  # Make an API request.

                if len(table.schema) >= len(original_schema):
                    print("A new column/s has been added to table {}.{}.{}\n".format(table.project, table.dataset_id, table.table_id))
                else:
                    print("The column/s has not been added table {}.{}.{}\n".format(table.project, table.dataset_id, table.table_id))

            except Exception as e:
                print(e)

        elif len(updated_schema)<len(cur_schema):
            print(f"\n{updated_schema}\n{cur_schema}\n")
            print("\nRemoving a column/s . . .\n")

            #to find the Columns to be deleted
            Columns = changed_columns(cur_schema,updated_schema)

            #Getting the list of column names to be removed
            col_names = []
            for col in Columns:
                col_names.append(col["name"])
            col_names = ", ".join(col_names)

            table = client.get_table(f"{project_id}.{dataset_name}.{table_name}")

            # keeping the copy of original schema to compare the changes after update
            original_schema = table.schema

            sql = f"""CREATE OR REPLACE TABLE {project_id}.{dataset_name}.{table_name} AS SELECT * EXCEPT ({col_names}) FROM {project_id}.{dataset_name}.{table_name}"""

            query_job = client.query(sql)  # Make an API request.
            query_job.result()  # Wait for the job to complete.

            table = client.get_table(f"{project_id}.{dataset_name}.{table_name}")

            if len(table.schema)<len(original_schema):
                print("A column/s has been deleted from table {}.{}.{}\n".format(table.project, table.dataset_id, table.table_id))
            else:
                print("The column/s has not been deleted from table {}.{}.{}\n".format(table.project, table.dataset_id, table.table_id))

    else:
        # Case where number of columns are same but have changes in schema.
        print(f"\n{updated_schema}\n{cur_schema}\n")

        if changed_columns(cur_schema,updated_schema):
            try:

                # Creting a new_schema to replace the previous table.
                col_details = []
                for col in updated_schema:
                    col_details.append(f"""{col['name']} {col['type']} {"NOT NULL" if col['mode']=="REQUIRED" else ""}""")
                col_details = ", ".join(col_details)

                sql = f"""CREATE OR REPLACE TABLE {project_id}.{dataset_name}.{table_name} ({col_details})"""

                query_job = client.query(sql)  # Make an API request.
                query_job.result()  # Wait for the job to complete.

                table = client.get_table(f"{project_id}.{dataset_name}.{table_name}")

                print("{}.{}.{} Table schema has been updated. \n".format(table.project, table.dataset_id, table.table_id))

            except Exception as e:
                print(e)
                return
        else:
            print("No changes have been made to schema.\n")

            ## After Checking for the schmea checking for changes in lables for the bq_table_schema
            ## If there is change updating the external_table_list
            table = client.get_table(f"{project_id}.{dataset_name}.{table_name}")

            updated_lables = labels
            cur_lables = table.labels
            OrderedDict(sorted(cur_lables.items()))
            print(f"\nLabels:- \nNew - {OrderedDict(sorted(updated_lables.items()))}\nCurrent - {OrderedDict(sorted(cur_lables.items()))}\n")
            if changed_lables(cur_lables, updated_lables):

                table.labels = updated_lables
                table = client.update_table(table, ["labels"])  # API request

            else:
                print("No changes have been made to lables.\n")
            return


# Checking and applying the changes for the external table
def external_table_changes(project_id, dataset_name, ext_table_name, json_schema_uri, source_format, source_uris, labels):

    with open(json_schema_uri) as file:
        updated_schema = json.load(file)
    cur_schema = get_table_schema(project_id, dataset_name, ext_table_name)

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # In case of external table we are dropping the table and creating a new one wit the new schema
    print(f"\n{updated_schema}\n{cur_schema}\n")

    if changed_columns(cur_schema,updated_schema):
        try:

            # Dropping the original external_table
            table_id = f"{project_id}.{dataset_name}.{ext_table_name}"

            # If the table does not exist, delete_table raises
            # google.api_core.exceptions.NotFound unless not_found_ok is True.
            client.delete_table(table_id, not_found_ok=True)  # Make an API request.
            print("Deleted old external table '{}'.".format(table_id))

            table = external_table_creation(project_id, dataset_name, ext_table_name, json_schema_uri, source_format, source_uris, labels)

            print("{}.{}.{} Table schema has been updated. \n".format(table.project, table.dataset_id, table.table_id))

        except Exception as e:
            print(e)
            return

    else:
        print("No changes have been made to schema.\n")

        ## After Checking for the schmea checking for changes in lables or Source details for the external table
        ## If there is change updating the external table details
        table_id = f"{project_id}.{dataset_name}.{ext_table_name}"
        table = client.get_table(table_id)

        # Getting the external table data configuration
        external_config = table.external_data_configuration

        updated_source_format = source_format
        current_source_format = external_config.source_format

        updated_source_uris = source_uris
        current_source_uris = external_config.source_uris

        # To sort the uris to compare
        updated_source_uris.sort()
        current_source_uris.sort()

        # Printing the Ordered labels
        print(f"Source Format:- \nNew - {updated_source_format}\nCurrent - {current_source_format}\n")
        print(f"Source uris:- \nNew - {updated_source_uris}\nCurrent - {current_source_uris}\n")

        if current_source_format!=updated_source_format or current_source_uris!=updated_source_uris:
            try:

                # Dropping the original external_table
                # If there is any change in external configuration we have to create the table again
                client.delete_table(table_id, not_found_ok=True)  # Make an API request.
                print("Deleted old external table '{}'.".format(table_id))

                table = external_table_creation(project_id, dataset_name, ext_table_name, json_schema_uri, source_format, source_uris, labels)

                print("{}.{}.{} Table data configuration has been updated. \n".format(table.project, table.dataset_id, table.table_id))

            except Exception as e:
                print(e)
                return

        else:
            print("No changes have been made to data configuration for external table.\n")

            updated_lables = labels
            cur_lables = table.labels

            print(f"\nLabels:- \nNew - {sorted(updated_lables.items())}\nCurrent - {sorted(cur_lables.items())}\n")
            if changed_lables(cur_lables, updated_lables):

                table.labels = updated_lables
                table = client.update_table(table, ["labels"])  # API request

            else:
                print("No changes have been made to lables.\n")

        return


## ================================================================================================================================
#Start of execution
try:
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_id)
except:
    print(f"WARNING: Unable to access the project {project_id}.")

#Lists all datasets.
datasets = client.list_datasets()  # Make an API request.

# To use it to perform check for if required dataset and table is present or not
bq_datasets = []
bq_tables = {}
if datasets:
    for dataset in datasets:
        bq_datasets.append(str(dataset.dataset_id))
        # In-case the dataset is empty set a empty list for it
        bq_tables.setdefault(str(dataset.dataset_id), [])

        tables = client.list_tables(f"{project_id}.{dataset.dataset_id}")
        if tables:
            for table in tables:
                bq_tables.setdefault(str(dataset.dataset_id), []).append(str(table.table_id))


## Going through all the objects one-by-one in objects_details
with open(objects_details) as file:
    details = json.load(file)

    if details:

        # This loop iterate over all the native_table in the list
        # len return number of tables in native_table_list
        for i in range(len(details['na_tables_list'])):

            # Table details
            dataset_name = details['na_tables_list'][i]['dataset_name']
            table_name = details['na_tables_list'][i]['table_name']
            json_schema_uri = details['na_tables_list'][i]['schema_json']
            labels = details['na_tables_list'][i]['labels']

            # To check if dataset is present in BigQuery
            if dataset_name in bq_datasets:
                # To check table is not already present in BigQuery dataset
                if table_name not in bq_tables[dataset_name]:
                    try:
                        table = native_table_creation(project_id, dataset_name, table_name, json_schema_uri, labels)
                        print("Created table {}.{}.{}\n".format(table.project, table.dataset_id, table.table_id))

                    except Exception as e:
                        print(f"WARNING: Unable to create Table {table_name} in dataset {dataset_name}\n",e)
                else:
                    # TODO:
                    print(f"\nWARNING: Table {table_name} Already EXISTS in dataset {dataset_name} ! ! !\n\n Looking for changes . . . \n\n")
                    native_table_changes(project_id, dataset_name, table_name, json_schema_uri, labels)
            else:
                # Creating a new dataset if not present in the project
                print(f"\nDataset {dataset_name} does not EXISTS ! ! !\n\nCreating new Dataset {dataset_name} in project {project_id}. . .")

                try:
                    # Create dataset and update the list of datasets in BigQuery
                    dataset = create_dataset(project_id, dataset_name, labels["location"])
                    print("Created dataset {}.{}\n".format(client.project, dataset.dataset_id))

                    bq_datasets.append(str(dataset.dataset_id))

                    try:
                        # Create dataset and update the list of table in dataset BigQuery
                        table = native_table_creation(project_id, dataset_name, table_name, json_schema_uri, labels)
                        print("Created table {}.{}.{}\n".format(table.project, table.dataset_id, table.table_id))

                        bq_tables.setdefault(str(dataset.dataset_id), []).append(str(table.table_id))

                    except Exception as e:
                        print(f"WARNING: Unable to create Table {table_name} in dataset {dataset_name}\n",e)

                except Exception as e:
                    print(f"WARNING: Unable to create a new dataset {dataset_name} in the project {project_id}.\n",e)


        # This loop iterate over all the external_table in the list
        # len return number of tables in external_table_list
        for i in range(len(details['ex_tables_list'])):

            # External table details
            dataset_name = details['ex_tables_list'][i]['dataset_name']
            ext_table_name = details['ex_tables_list'][i]['table_name']
            json_schema_uri = details['ex_tables_list'][i]['schema_json']
            source_format = details['ex_tables_list'][i]['source_format']
            source_uris = details['ex_tables_list'][i]['source_uris']
            labels = details['ex_tables_list'][i]['labels']


            # To check if dataset is present in BigQuery
            if dataset_name in bq_datasets:
                # To check if external table is not already present in BigQuery dataset
                if ext_table_name not in bq_tables[dataset_name]:
                    try:

                        table = external_table_creation(project_id, dataset_name, ext_table_name, json_schema_uri, source_format, source_uris, labels)
                        print("Created external table {}.{}.{}\n".format(table.project, table.dataset_id, table.table_id))

                    except Exception as e:
                        print(f"WARNING: Unable to create external Table {ext_table_name} in dataset {dataset_name}\n",e)
                else:
                    # TODO:
                    print(f"\nWARNING: External table {ext_table_name} Already EXISTS in dataset {dataset_name} ! ! !\n\n Looking for changes . . . \n\n")
                    external_table_changes(project_id, dataset_name, ext_table_name, json_schema_uri, source_format, source_uris, labels)
            else:
                # Creating a new dataset if not present in the project
                print(f"\nDataset {dataset_name} does not EXISTS ! ! !\n\nCreating new Dataset {dataset_name} in project {project_id}. . .")

                try:
                    # Create dataset and update the list of datasets in BigQuery
                    dataset = create_dataset(project_id, dataset_name, labels["location"])
                    print("Created dataset {}.{}\n".format(client.project, dataset.dataset_id))

                    bq_datasets.append(str(dataset.dataset_id))

                    try:
                        # Create dataset and update the list of table in dataset BigQuery
                        table = external_table_creation(project_id, dataset_name, ext_table_name, json_schema_uri, source_format, source_uris, labels)
                        print("Created External table {}.{}.{}\n".format(table.project, table.dataset_id, table.table_id))

                        bq_tables.setdefault(str(dataset.dataset_id), []).append(str(table.table_id))

                    except Exception as e:
                        print(f"WARNING: Unable to create external table {ext_table_name} in dataset {dataset_name}\n",e)

                except Exception as e:
                    print(f"WARNING: Unable to create a new dataset {dataset_name} in the project {project_id}.\n",e)


    else:
        print("WARNING: No Objects founds.")

import stripe
import json
import pandas as pd
from bson import json_util
import ast
from pandas import json_normalize
from bson import json_util
import dateutil.parser
import configparser
from datetime import date, datetime
from google.cloud import bigquery
import google.auth
import os
from google.cloud.exceptions import NotFound


def confReader():
    try:
        print("confReader() -- Started")
        print("confReader() -- Intializing config parser")
        config = configparser.ConfigParser()
        print("confReader() -- Reading properties.conf file")
        config.read(os.path.dirname(__file__) + '/properties.conf')
        print("confReader() -- Identifying Sections")
        configs=config._sections
        print("confReader() -- Ended")
        print("confReader() -- Returning configs")
        return configs
    except Exception as e:
        print("confReader Error: "+str(e) )

def StripeAPI_CustomersList(API_KEY):
    try:
        print("StripeAPI_CustomersList() -- Started")
        
        print("StripeAPI_CustomersList() -- Initializing stripe.api_key")
        stripe.api_key = API_KEY

        print("StripeAPI_CustomersList() -- Call Stripe API SDK to List Customers")
        api_data = stripe.Customer.list(limit=100) 
        
        print("StripeAPI_CustomersList() -- Initializing empty obj_id & df")
        obj_id = None
        df = pd.DataFrame()
        while_counter=1

        print("StripeAPI_CustomersList() -- Starting While True")
        while 1:
            print("StripeAPI_CustomersList() -- While True - Iter "+str(while_counter)+" Started")
            
            print("StripeAPI_CustomersList() -- While True - Check if API Data = 0")
            if len(api_data["data"]) == 0:
                print("StripeAPI_CustomersList() -- While True - API Data = 0")
                print("StripeAPI_CustomersList() -- While True - Break While True")
                break
            else:
                while_counter=while_counter+1
                print("StripeAPI_CustomersList() -- While True - API Data NOT = 0")
                print("StripeAPI_CustomersList() -- While True - Identify last customer id in the page")
                print("DEBUG -- StripeAPI_CustomersList() -- While True - CustomerID= "+str(api_data["data"][-1]["id"]))
                obj_id = api_data["data"][-1]["id"]
            
            print("StripeAPI_CustomersList() -- While True - Convert API Data into JSON")
            data_json=json.dumps(api_data["data"], default=json_util.default)

            print("StripeAPI_CustomersList() -- While True - Normalizing API Data")
            json_norm_df = pd.json_normalize(json.loads(data_json))
            
            print("StripeAPI_CustomersList() -- While True - Add API Data into DataFrame")
            df=df.append(json_norm_df)

            print("StripeAPI_CustomersList() -- While True - Call Stripe API SDK to List Customers on Next Page")                
            api_data = stripe.Customer.list(limit=100,starting_after=obj_id)
            
            print("StripeAPI_CustomersList() -- While True - Iter "+str(while_counter-1)+" Ended")
            
        print("StripeAPI_CustomersList() -- While True Ended")
        print("StripeAPI_CustomersList() -- "+str(while_counter-1)+" Pages of Customers Collected")    
        
        print("StripeAPI_CustomersList() -- Ended")
        print("StripeAPI_CustomersList() -- Returning DataFrame")
        return df        
    except Exception as e:
        print("StripeAPI_CustomersList Error: "+str(e))          

def dataPrep(df):
    try:
        print("dataPrep() -- Started")
        ##----- Clean-up Field Names -----##               
        df.columns = df.columns.str.replace(r'.', '_',regex = True)

        ##----- Datetime Fiedlds Type -----##
        print("dataPrep() -- Datetime Fields Type Section")

        print("dataPrep() -- Converting [ created ] Column to datetime")
        df["created"]=pd.to_datetime(df["created"],unit='s')
        #df["created"] = df["created"].dt.strftime('%d-%m-%Y %I:%M:%S')

        print("dataPrep() -- Creating [ collected_date ] Column")        
        df["collect_date"] = pd.to_datetime(datetime.now())
        #df["collect_date"] = df["collect_date"].dt.strftime('%d-%m-%Y %I:%M:%S')
        
        print("dataPrep() -- Ended")
        print("dataPrep() -- Returning DataFrame")
        return df
    except Exception as e:
        print("dataPrep Error: "+str(e))    
def BQTable_Exists(tbl):
    try:
        print("BQTable_Exists() -- Started")
        bq_client.get_table(tbl)
        print("BQTable_Exists() -- Table {} already exists.".format(tbl))
        print("BQTable_Exists() -- Ended")
        return True        
    except NotFound:
        print("BQTable_Exists() --Table {} is not found.".format(tbl))
        return False
    
def BQTable_Create(conf,schema):
    try:
        print("BQTable_Create() -- Started")
        table = bigquery.Table(conf["BQ"]["table_id"], schema=schema)
        table = bq_client.create_table(table)  # Make an API request.
        print("BQTable_Create() -- Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
        print("BQTable_Create() -- Ended")
    except Exception as e:
        print("BQTable_Create Error: "+str(e))
    
def BQTable_Insert(data,conf,ExistsBool):
    try:
        print("BQTable_Insert() -- Started")

        ##--------------Instert in ODS DATASET----------------##
        print("BQTable_Insert() -- Setting Table ID")
        table_id = conf["BQ"]["tmp_table_id"]

        print("BQTable_Insert() -- Setting Job Configurtion")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

        print("BQTable_Insert() -- Submitting Insert Job API")
        job = bq_client.load_table_from_dataframe(data, table_id, job_config=job_config)  # Make an API request.
        job.result()  # Wait for the job to complete.

        ##--------------Table Check in DWH DATASET----------------##
        print("BQTable_Insert() -- Check if DWH Table Exists")
        if ExistsBool:
            print("BQTable_Insert() -- DWH Table Exists")
            pass
        else:
            print("BQTable_Insert() -- DWH Table Not Exists")
            
            print("BQTable_Insert() -- Retrieve ODS_Table Schema")
            tmp_table_schema = bq_client.get_table(conf["BQ"]["tmp_table_id"]).schema

            print("BQTable_Insert() -- Schema Build")
            schemaList=[]
            for field in tmp_table_schema:
                schemaList.append(bigquery.SchemaField(field.name, field.field_type, mode=field.mode))
            print("BQTable_Insert() -- Call BQTable_Create")
            BQTable_Create(conf,schemaList)
            
        ##--------------MERGE WITH DWH DATASET----------------##
        print("BQTable_Insert() -- Formatting DML Query")
        InsertValue_string="("
        Update_string="UPDATE SET "
        lncols=len(list(data.columns))
        counter=1
        for col in list(data.columns):
            if counter < lncols:
                Update_string=str(Update_string)+str(col)+" = source."+str(col)+"\n, "
                InsertValue_string=str(InsertValue_string)+str(col)+", "
                counter+=1
            else:
                Update_string=str(Update_string)+str(col)+" = source."+str(col)+"\n"
                InsertValue_string=str(InsertValue_string)+str(col)+")"
                counter+=1
        print("BQTable_Insert() -- Declaring DML Query")
        dml_statement = f"""MERGE {conf["BQ"]["table_id"]} target 
            USING {conf["BQ"]["tmp_table_id"]} source
            ON target.id = source.id
            WHEN MATCHED THEN
                {Update_string}
            WHEN NOT MATCHED THEN
                INSERT {InsertValue_string}
                VALUES {InsertValue_string}
                """            
        print("BQTable_Insert() -- Submitting DML Merge Job")
        query_job = bq_client.query(dml_statement)  
        query_job.result()

        print("BQTable_Insert() -- Ended")
    except Exception as e:
        print("BQTable_Insert Error: "+str(e))     

def MAIN(event,context):
    try:
        print("MAIN() -- Started")
        
        print("MAIN() -- Calling confReader()")
        conf=confReader()
        
        print("MAIN() -- Calling StripeAPI_CustomersList()")
        df=StripeAPI_CustomersList(conf["STRIPE"]["api_key"])
        print("MAIN() -- Data Extracted Empty Check")
        if df.empty:
            print("MAIN() -- No Data Captured, Exiting... .")
            return
        else:
            print("MAIN() -- Data Found")

            print("MAIN() -- Calling dataPrep()")
            df=dataPrep(df)
            
            print("MAIN() -- Calling BQTable_Exists()")
            ExistsBool = BQTable_Exists(conf["BQ"]["table_id"])

            print("MAIN() -- Calling BQTable_Insert()")
            BQTable_Insert(df,conf,ExistsBool)
                
        ##-----Debug Group-----#
        print("DEBUG -- MAIN() -- Debug Group Started")
        print("DEBUG -- MAIN() -- Save to CSV")
        df.to_csv('stripe_api_customers.csv')
        print("DEBUG -- MAIN() -- Debug Group Ended")
        ##---------------------#
        print("MAIN() -- Ended")
    except Exception as e:
        print("MAIN Error: "+str(e))  

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "<Service Account Key File>"

bq_client = bigquery.Client()
        
MAIN("event","context")


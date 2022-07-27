# StripeAPI to BQ - CFunc ETL
Google Cloud Function built to perform an ETL Job to Collect StripeAPI Data and Transform it to be able to Import it to Bigquery.

Google CFun is Connecting to StripeAPI using Stripe Python Library and Extracts API Data with Iterating on all Pages Mechanism. As well it gets only newly created refrenced objects by applying delta update modeling. And having two layers of modeling on BQ as the ODS to stage the delta only and then merge it to the data warehouse layer.

Cloud Funtion has been written in python with 5-main functions:

1- MAIN() -- Orchestrate the rest of the functions

2- confReader() -- The full function is built on a separate propertes.conf file. Which has to be read at the early stages to enable the connections and properties needed.

3- StripeAPI_CustomersList() -- Responsible to connect to StripeAPI using API_Key within the configuration and grap the refernced api objects delta.

4- dataPrep() -- It performs the data & schema validations to fit within the BQ Insert.

5- BQTable_Insert() -- It Inserts the Table to the ODS Dataset as the delta only (Contains new/updated records) & then checks is the DWH Table Created or not. If yes it merge that delta data to the DWH Layer to have a consalidated table at the end. If no, then it extracts the ODS Table Schema, Create DWH Table with that schema and then do the merge. 

Script procedures are being logged step by step.

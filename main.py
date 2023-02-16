def hello_world(request):
    #import section
    import fnmatch
    import pymysql
    import base64 
    import os
    import io
    import time 
    import multiprocessing
    import json
    import re
    import ast
    import datetime
    import pathlib
    import imp
    import fastavro 
    import xlrd
    import pyorc
    import lxml
    from lxml import etree 
    import xmltodict
    from ftplib import FTP
    from sqlite3 import connect
    import pandas as pd
    import decimal
    import mysql.connector
    from google.cloud import bigquery
    from google.cloud import storage
    from google.cloud.exceptions import NotFound
    import sqlalchemy
    import os
    #section for log_file generation
    import logging
    logger=logging.getLogger(__name__)
    print(logger)
    logger.setLevel(logging.DEBUG)
    f=logging.Formatter('%(asctime)s-%(levelname)s-%(message)s')
    print(f)
    fh=logging.FileHandler('/tmp/mysql_to_bq.log','w')
    print(fh)
    fh.setFormatter(f)
    logger.addHandler(fh)

    decimal.getcontext().prec = 18

    #variables for accessing variables_dictionary_file from GCS 
    bucket_name = 'exporttogcs11'
    variables_file_name = 'variables_dictionary_file1.txt'

    #Establishing the connection to different GCP resources---Accessing variables_dictionary_file and extracting dictionary of variables from it 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(variables_file_name)
    b=blob.download_as_string()
    # Now we have a bytes string, convert that into a stream
    dict_str = b.decode("UTF-8")
    Variables_dict = ast.literal_eval(dict_str)
    
    #Accessing Variables from the file
    #variables related to mysql fullload job
    DB_USER=Variables_dict['DB_USER']
    print(Variables_dict['DB_USER'])
    CONNECTION_NAME=Variables_dict['CONNECTION_NAME']
    print(Variables_dict['CONNECTION_NAME'])
    DB_PASSWORD=Variables_dict['DB_PASSWORD']
    print(Variables_dict['DB_PASSWORD'])
    DB_NAME=Variables_dict['DB_NAME']
    print(Variables_dict['DB_NAME'])
    load_job_type=Variables_dict['load_job_type']
    print(Variables_dict['load_job_type'])
    gcpprojectname=Variables_dict['gcpprojectname']
    print(Variables_dict['gcpprojectname'])
    mysql_to_bq_datasetname=Variables_dict['mysql_to_bq_datasetname']
    print(Variables_dict['mysql_to_bq_datasetname'])
    mysql_to_gcs_bucket_name=Variables_dict['mysql_to_gcs_bucket_name']
    print(Variables_dict['mysql_to_gcs_bucket_name'])
    mysql_to_gcs_subfolder=Variables_dict['mysql_to_gcs_subfolder']
    print(Variables_dict['mysql_to_gcs_subfolder'])
    mysql_bq_backup_bucket_name=Variables_dict['mysql_bq_backup_bucket_name']
    print(Variables_dict['mysql_bq_backup_bucket_name'])
    source_tables_list=Variables_dict['source_tables_list']
    print(Variables_dict['source_tables_list'])
    mysql_table_files_backup_bucket=Variables_dict['mysql_table_files_backup_bucket']
    print(Variables_dict['mysql_table_files_backup_bucket'])
    validation_nullvalues_dataset=Variables_dict['validation_nullvalues_dataset']
    print(Variables_dict['validation_nullvalues_dataset'])
    bq_validation_table=Variables_dict['bq_validation_table']
    print(Variables_dict['bq_validation_table'])
    sum_check_validation_table=Variables_dict['sum_check_validation_table']
    print(Variables_dict['sum_check_validation_table'])
    bq_logging_info_table=Variables_dict['bq_logging_info_table']
    print(Variables_dict['bq_logging_info_table'])
    SLAtimeinseconds = Variables_dict['SLAtimeinseconds']
    print(Variables_dict['SLAtimeinseconds'])
    source_type=Variables_dict['source_type']
    print(Variables_dict['source_type'])
    mysql_logfile_path=Variables_dict['mysql_logfile_path']
    print(Variables_dict['mysql_logfile_path'])
    duplicates_table=Variables_dict['duplicates_table']   
    print(Variables_dict['duplicates_table']) 
    mask_check_validation_table=Variables_dict['mask_check_validation_table']
    print(Variables_dict['mask_check_validation_table'])
    mysql_count_of_source_tables=[]
    mysql_destination_tables_list=[]
    mysql_count_of_destination_tables=[]
    mysql_distinct_count_of_destination_tables=[]
    dataset_id=f'{gcpprojectname}.{mysql_to_bq_datasetname}'
    print(dataset_id)
    unix_socket= f'/cloudsql/{CONNECTION_NAME}'
    print(unix_socket)

    #variables related to FTP server connections
    FTP_HOST = Variables_dict["FTP_HOST"]
    print(Variables_dict["FTP_HOST"])
    FTP_USER = Variables_dict["FTP_USER"]
    print(Variables_dict["FTP_USER"])
    FTP_PASS = Variables_dict["FTP_PASS"]
    print(Variables_dict["FTP_PASS"])
    src_fold = Variables_dict["src_fold"]
    print(Variables_dict["src_fold"])

    #variables related to FTP_csv_files fullload job 
    FTP_csv_to_GCS_bucket=Variables_dict['FTP_csv_to_GCS_bucket']
    FTP_csv_gcs_destination_folder = Variables_dict["FTP_csv_gcs_destination_folder"]
    FTP_csv_source_files_backup_bucket=Variables_dict['FTP_csv_source_files_backup_bucket']
    FTP_csv_bq_tables_backup_bucket=Variables_dict['FTP_csv_bq_tables_backup_bucket']
    FTP_to_BQ_csv_dataset=Variables_dict['FTP_to_BQ_csv_dataset']
    FTP_csv_source_files_list=[]
    FTP_csv_count_of_source_files=[]
    FTP_csv_destination_tables_list=[]
    FTP_csv_count_of_destination_tables=[]
    FTP_csv_distinct_count_of_destination_tables=[]


    #variables related to FTP_json_files fullload job
    FTP_json_to_GCS_bucket=Variables_dict['FTP_json_to_GCS_bucket']
    FTP_json_gcs_destination_folder = Variables_dict["FTP_json_gcs_destination_folder"]
    FTP_json_source_files_backup_bucket=Variables_dict['FTP_json_source_files_backup_bucket']
    FTP_json_bq_tables_backup_bucket=Variables_dict['FTP_json_bq_tables_backup_bucket']
    FTP_to_BQ_json_dataset=Variables_dict['FTP_to_BQ_json_dataset']
    FTP_json_source_files_list=[]
    FTP_json_count_of_source_files=[]
    FTP_json_destination_tables_list=[]
    FTP_json_count_of_destination_tables=[]
    FTP_json_distinct_count_of_destination_tables=[]

    #variables related to FTP_avro_files fullload job
    FTP_avro_to_GCS_bucket=Variables_dict['FTP_avro_to_GCS_bucket']
    FTP_avro_gcs_destination_folder = Variables_dict["FTP_avro_gcs_destination_folder"]
    FTP_avro_source_files_backup_bucket=Variables_dict['FTP_avro_source_files_backup_bucket']
    FTP_avro_bq_tables_backup_bucket=Variables_dict['FTP_avro_bq_tables_backup_bucket']
    FTP_to_BQ_avro_dataset=Variables_dict['FTP_to_BQ_avro_dataset']
    FTP_avro_source_files_list=[]
    FTP_avro_count_of_source_files=[]
    FTP_avro_destination_tables_list=[]
    FTP_avro_count_of_destination_tables=[]
    FTP_avro_distinct_count_of_destination_tables=[]

    #variables related to FTP_tsv_files fullload job
    FTP_tsv_to_GCS_bucket=Variables_dict['FTP_tsv_to_GCS_bucket']
    FTP_tsv_gcs_destination_folder = Variables_dict["FTP_tsv_gcs_destination_folder"]
    FTP_tsv_source_files_backup_bucket=Variables_dict['FTP_tsv_source_files_backup_bucket']
    FTP_tsv_bq_tables_backup_bucket=Variables_dict['FTP_tsv_bq_tables_backup_bucket']
    FTP_to_BQ_tsv_dataset=Variables_dict['FTP_to_BQ_tsv_dataset']
    FTP_tsv_source_files_list=[]
    FTP_tsv_count_of_source_files=[]
    FTP_tsv_destination_tables_list=[]
    FTP_tsv_count_of_destination_tables=[]
    FTP_tsv_distinct_count_of_destination_tables=[]

    #variables related to FTP_psv_files fullload job
    FTP_psv_to_GCS_bucket=Variables_dict['FTP_psv_to_GCS_bucket']
    FTP_psv_gcs_destination_folder = Variables_dict["FTP_psv_gcs_destination_folder"]
    FTP_psv_source_files_backup_bucket=Variables_dict['FTP_psv_source_files_backup_bucket']
    FTP_psv_bq_tables_backup_bucket=Variables_dict['FTP_psv_bq_tables_backup_bucket']
    FTP_to_BQ_psv_dataset=Variables_dict['FTP_to_BQ_psv_dataset']
    FTP_psv_source_files_list=[]
    FTP_psv_count_of_source_files=[]
    FTP_psv_destination_tables_list=[]
    FTP_psv_count_of_destination_tables=[]
    FTP_psv_distinct_count_of_destination_tables=[]

    #variables related to FTP_xlsx_files fullload job
    FTP_xlsx_to_GCS_bucket=Variables_dict['FTP_xlsx_to_GCS_bucket']
    FTP_xlsx_gcs_destination_folder = Variables_dict["FTP_xlsx_gcs_destination_folder"]
    FTP_xlsx_source_files_backup_bucket=Variables_dict['FTP_xlsx_source_files_backup_bucket']
    FTP_xlsx_bq_tables_backup_bucket=Variables_dict['FTP_xlsx_bq_tables_backup_bucket']
    FTP_to_BQ_xlsx_dataset=Variables_dict['FTP_to_BQ_xlsx_dataset']
    FTP_xlsx_source_files_list=[]
    FTP_xlsx_count_of_source_files=[]
    FTP_xlsx_destination_tables_list=[]
    FTP_xlsx_count_of_destination_tables=[]
    FTP_xlsx_distinct_count_of_destination_tables=[]

    #variables related to FTP_xls_files fullload job
    FTP_xls_to_GCS_bucket=Variables_dict['FTP_xls_to_GCS_bucket']
    FTP_xls_gcs_destination_folder = Variables_dict["FTP_xls_gcs_destination_folder"]
    FTP_xls_source_files_backup_bucket=Variables_dict['FTP_xls_source_files_backup_bucket']
    FTP_xls_bq_tables_backup_bucket=Variables_dict['FTP_xls_bq_tables_backup_bucket']
    FTP_to_BQ_xls_dataset=Variables_dict['FTP_to_BQ_xls_dataset']
    FTP_xls_source_files_list=[]
    FTP_xls_count_of_source_files=[]
    FTP_xls_destination_tables_list=[]
    FTP_xls_count_of_destination_tables=[]
    FTP_xls_distinct_count_of_destination_tables=[]

    #variables related to FTP_xls_files fullload job
    FTP_xml_to_GCS_bucket=Variables_dict['FTP_xml_to_GCS_bucket']
    FTP_xml_gcs_destination_folder = Variables_dict["FTP_xml_gcs_destination_folder"]
    FTP_xml_source_files_backup_bucket=Variables_dict['FTP_xml_source_files_backup_bucket']
    FTP_xml_bq_tables_backup_bucket=Variables_dict['FTP_xml_bq_tables_backup_bucket']
    FTP_to_BQ_xml_dataset=Variables_dict['FTP_to_BQ_xml_dataset']
    FTP_xml_source_files_list=[]
    FTP_xml_count_of_source_files=[]
    FTP_xml_destination_tables_list=[]
    FTP_xml_count_of_destination_tables=[]
    FTP_xml_distinct_count_of_destination_tables=[]

    #variables related to FTP_csv_files fullload job 
    FTP_orc_to_GCS_bucket=Variables_dict['FTP_orc_to_GCS_bucket']
    FTP_orc_gcs_destination_folder = Variables_dict["FTP_orc_gcs_destination_folder"]
    FTP_orc_source_files_backup_bucket=Variables_dict['FTP_orc_source_files_backup_bucket']
    FTP_orc_bq_tables_backup_bucket=Variables_dict['FTP_orc_bq_tables_backup_bucket']
    FTP_to_BQ_orc_dataset=Variables_dict['FTP_to_BQ_orc_dataset']
    FTP_orc_source_files_list=[]
    FTP_orc_count_of_source_files=[]
    FTP_orc_destination_tables_list=[]
    FTP_orc_count_of_destination_tables=[]
    FTP_orc_distinct_count_of_destination_tables=[]

    #variables related to FTP_parquet_files fullload job 
    FTP_parquet_to_GCS_bucket=Variables_dict['FTP_parquet_to_GCS_bucket']
    FTP_parquet_gcs_destination_folder = Variables_dict["FTP_parquet_gcs_destination_folder"]
    FTP_parquet_source_files_backup_bucket=Variables_dict['FTP_parquet_source_files_backup_bucket']
    FTP_parquet_bq_tables_backup_bucket=Variables_dict['FTP_parquet_bq_tables_backup_bucket']
    FTP_to_BQ_parquet_dataset=Variables_dict['FTP_to_BQ_parquet_dataset']
    FTP_parquet_source_files_list=[]
    FTP_parquet_count_of_source_files=[]
    FTP_parquet_destination_tables_list=[]
    FTP_parquet_count_of_destination_tables=[]
    FTP_parquet_distinct_count_of_destination_tables=[]

    #variables related to FTP_log_files fullload job 
    FTP_log_to_GCS_bucket=Variables_dict['FTP_log_to_GCS_bucket']
    FTP_log_gcs_destination_folder = Variables_dict["FTP_log_gcs_destination_folder"]
    FTP_log_source_files_backup_bucket=Variables_dict['FTP_log_source_files_backup_bucket']
    FTP_log_bq_tables_backup_bucket=Variables_dict['FTP_log_bq_tables_backup_bucket']
    FTP_to_BQ_log_dataset=Variables_dict['FTP_to_BQ_log_dataset']
    FTP_log_source_files_list=[]
    FTP_log_count_of_source_files=[]
    FTP_log_destination_tables_list=[]
    FTP_log_count_of_destination_tables=[]
    FTP_log_distinct_count_of_destination_tables=[]

    #Establishing the connection to different GCP resources
    storage_client = storage.Client()
    bigquery_client = bigquery.Client(project=gcpprojectname)
    
    #generating TIMESTAMP as required
    data = [{
            "mask_name":"YYYY",
            "mask_value":"datetime.datetime.now().year",
            "mask_type":1},
        {
            "mask_name":"MM",
            "mask_value":"datetime.datetime.now().month",
            "mask_type":1
        },
        {
            "mask_name":"DD",
            "mask_value":"datetime.datetime.now().day",
            "mask_type":1
        },
        {
            "mask_name":"HH",
            "mask_value":"datetime.datetime.now().hour",
            "mask_type":1
        },
        {
            "mask_name":"MIN",
            "mask_value":"datetime.datetime.now().minute",
            "mask_type":1
        },
        {
            "mask_name":"SS",
            "mask_value":"datetime.datetime.now().second",
            "mask_type":1
        }]
    filename = "YYYY-MM-DD-MM-SS"
    for row, item in enumerate(data):
        if item.get("mask_name") in filename: 
            filename = filename.strip().replace(" ", "")  
            if item.get("mask_type") == 1 :
                filename = filename.replace(
                    item.get("mask_name").strip(),
                    str(eval(item.get("mask_value"))))
            if item.get("mask_type") ==2 :
                filename = filename.replace(item.get("mask_name").strip(),
                                            item.get("mask_value"))
    print('successful upto timestamp generation')

    #connecting to cloud SQL Instance
    pool = sqlalchemy.create_engine(
        # Equivalent URL:
        # mysql+pymysql://<db_user>:<db_pass>@/<db_name>?unix_socket=<socket_path>/<cloud_sql_instance_name>
        sqlalchemy.engine.url.URL.create(
            drivername="mysql+pymysql",
            username=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            query={"unix_socket": unix_socket},
        ),
        # ...
    )
    print('successfully connected!!')

    #function to connect to FTP SERVER and load files to /tmp/ folder in cloud functions and from there to GCS  
    def get_ftp(FTP_HOST, FTP_USER, FTP_PASS,source_file_type,gcs_bucket_name,gcs_folder_name,FTP_source_files_list,FTP_count_of_source_files):
     # initialize FTP session
        ftp = FTP(FTP_HOST, FTP_USER, FTP_PASS)
        # force UTF-8 encoding
        ftp.encoding = "utf-8"
        # switch to the directory we want
        ftp.cwd(src_fold)
        filenames = ftp.nlst()
        for filename in filenames:
            if((pathlib.Path(filename).suffix)==source_file_type):
                print(filename)
                with open("/tmp/"+filename, 'wb' ) as file :
                    ftp.retrbinary('RETR %s' % filename, file.write)
                    file.close()
                print("/tmp/"+filename)
                if source_file_type=='.csv':
                    a = pd.read_csv("/tmp/"+filename)
                    FTP_csv_source_files_list.append(filename)
                    FTP_csv_count_of_source_files.append(len(a))
                    print(len(a))
                if source_file_type=='.json':
                    b = pd.read_json("/tmp/"+filename)
                    FTP_json_source_files_list.append(filename)
                    FTP_json_count_of_source_files.append(len(b))
                    print(len(b))
                if source_file_type=='.avro':
                    with open("/tmp/"+filename, "rb") as fp:
                        reader = fastavro.reader(fp)
                        records = [r for r in reader]
                        fp.close()
                    c = pd.DataFrame.from_records(records)
                    FTP_avro_source_files_list.append(filename)
                    FTP_avro_count_of_source_files.append(len(c))
                    print(len(c))
                if source_file_type=='.psv':
                    d = pd.read_csv("/tmp/"+filename,sep='|')
                    FTP_psv_source_files_list.append(filename)
                    FTP_psv_count_of_source_files.append(len(d))
                    print(len(d))
                if source_file_type=='.tsv':
                    e = pd.read_csv("/tmp/"+filename,sep='\t')
                    FTP_tsv_source_files_list.append(filename)
                    FTP_tsv_count_of_source_files.append(len(e))
                    print(len(e))
                if source_file_type=='.xlsx':
                    f=pd.read_excel("/tmp/"+filename, index_col=0)
                    FTP_xlsx_source_files_list.append(filename) 
                    FTP_xlsx_count_of_source_files.append(len(f))
                    print(len(f))
                if source_file_type=='.xls':
                    g=pd.read_excel("/tmp/"+filename, index_col=0)
                    FTP_xls_source_files_list.append(filename) 
                    FTP_xls_count_of_source_files.append(len(g))
                    print(len(g))
                if source_file_type=='.xml':
                    h=pd.read_xml("/tmp/"+filename)
                    FTP_xml_source_files_list.append(filename) 
                    FTP_xml_count_of_source_files.append(len(h))
                    print(len(h))
                if source_file_type=='.orc':
                    i=pd.read_orc("/tmp/"+filename)
                    FTP_orc_source_files_list.append(filename) 
                    FTP_orc_count_of_source_files.append(len(i))
                    print(len(i))
                if source_file_type=='.parquet':
                    j = pd.read_parquet("/tmp/"+filename)
                    FTP_parquet_source_files_list.append(filename)
                    FTP_parquet_count_of_source_files.append(len(j))
                    print(len(j))
                if source_file_type=='.log':
                    k = pd.read_table("/tmp/"+filename)
                    FTP_log_source_files_list.append(filename)
                    FTP_log_count_of_source_files.append(len(k))
                    print(len(k))
                bucket = storage_client.get_bucket(gcs_bucket_name)
                blob = bucket.blob("{}/{}".format(gcs_folder_name,filename))
                blob.upload_from_filename("/tmp/"+filename)
    
    #Function to move the source files to backup folder in GCS 
    def source_files_backup_to_gcs_folder(source_bucket,destination_bucket,folder_name,file_format):
        dest_bucket = storage_client.get_bucket(destination_bucket)
        source_bucket = storage_client.get_bucket(source_bucket)
        blobs = source_bucket.list_blobs(prefix=folder_name) #assuming this is tested
        for blob in blobs:
            if fnmatch.fnmatch(blob.name, f'*.{file_format}'): #assuming this is tested
                print(blob.name)
                temp=blob.name[:-4]
                temp_name=f'gcs-{temp}-{datetime.datetime.now()}'
                source_bucket.copy_blob(blob,dest_bucket,new_name = temp_name)
                source_bucket.delete_blob(blob.name)
        print('source files successfully moved to gcs backup folder')

    #Function to create a dataset in Bigquery
    def bq_create_dataset(bigquery_client, dataset):
        print(dataset)
        dataset_ref = bigquery_client.dataset(dataset)
        print(dataset_ref)
        try:
            dataset = bigquery_client.get_dataset(dataset_ref)
            print(type(dataset))
            print(dataset)
            print('Dataset {} already exists.'.format(dataset.dataset_id))
            return f'Dataset {dataset} already exists.'
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            print(dataset)
            dataset.location = 'us-central1'
            dataset = bigquery_client.create_dataset(dataset)
            print('Dataset {} created.'.format(dataset.dataset_id))
            return dataset

    #Function to move the BigQuery tables data to GCS before truncating for backup  
    def bqtables_backup_to_gcs_folder(project_name,dataset_name,bucket_name,destination_file_format): 
        tables = bigquery_client.list_tables(f'{project_name}.{dataset_name}')
        print("Tables contained in '{}':".format(dataset_id))
        for table in tables:
            print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))
            table_id = table.table_id
            destination_file_name=f"bq-{table_id}-{datetime.datetime.now()}"
            print(destination_file_name)
            destination_uri = "gs://{}/{}".format(bucket_name, destination_file_name)
            dataset_ref = bigquery.DatasetReference(project_name, dataset_name)
            table_ref = dataset_ref.table(table_id)
            job_config = bigquery.job.ExtractJobConfig()
            if destination_file_format=='NEWLINE_DELIMITED_JSON':
                job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
            if destination_file_format=='CSV':
                job_config.destination_format = bigquery.DestinationFormat.CSV
            if destination_file_format=='AVRO':
                job_config.destination_format = bigquery.DestinationFormat.AVRO 
            if destination_file_format=='TSV':
                job_config.destination_format = bigquery.DestinationFormat.CSV
            if destination_file_format=='PSV':
                job_config.destination_format = bigquery.DestinationFormat.CSV               
            if destination_file_format=='XLSX':
                job_config.destination_format = bigquery.DestinationFormat.CSV
            if destination_file_format=='XLS':
                job_config.destination_format = bigquery.DestinationFormat.CSV
            if destination_file_format=='XML':
                job_config.destination_format = bigquery.DestinationFormat.CSV
            if destination_file_format=='ORC':
                job_config.destination_format = bigquery.DestinationFormat.CSV
            if destination_file_format=='PARQUET':
                job_config.destination_format = bigquery.DestinationFormat.PARQUET
            if destination_file_format=='LOG':
                job_config.destination_format = bigquery.DestinationFormat.CSV
            extract_job = bigquery_client.extract_table(
                table_ref,
                destination_uri,
                job_config=job_config,
                # Location must match that of the source table.
                location="us-central1",
            )  # API request
            extract_job.result()  # Waits for job to complete.
            print(
                "Exported {}:{}.{} to {}".format(project_name, dataset_name, table_id, destination_uri)
            )

    #function to trim the data in pandas dataframe---to perform left trim and right trim(to remove all the spaces)
    def trim_all_columns(df):
        trim_strings = lambda x: x.strip() if isinstance(x, str) else x
        return df.applymap(trim_strings)
        
    #function to upload the gcs files to big query tables
    def gcs_to_bigquery(gcs_bucket_name,gcs_folder_name,project_name,dataset_name,destination_tables_list,count_of_destination_tables):
        bucket = storage_client.get_bucket(gcs_bucket_name)    #connects to gcs bucket
        print(bucket)
        print(gcs_folder_name)
        for blob in bucket.list_blobs(prefix=gcs_folder_name):
            print(blob.name)
            if blob.name.endswith('csv'):    #Checking for csv files in the gcs folder
                filename = re.findall(r".*/(.*).csv",blob.name) #Extracting file name for BQ's table id
                filenametest=filename[0].strip().replace(" ", "")
                print(filenametest)
                table_id = f'{project_name}.{dataset_name}.{filenametest}' # Determining table name
                print(table_id)
                temp=blob.name
                uri1= "gs://{}".format(gcs_bucket_name)
                uri =f'{uri1}/{temp}'
                print(uri)
                df=pd.read_csv(uri)
                df=trim_all_columns(df) 
                df1=df     #trimming the data
                df2=df1
            if blob.name.endswith('json'):  #Checking for json files in the gcs folder
                filename = re.findall(r".*/(.*).json",blob.name) #Extracting file name for BQ's table id
                filenametest=filename[0].strip().replace(" ", "")
                print(filenametest)
                table_id = f'{project_name}.{dataset_name}.{filenametest}' # Determining table name
                print(table_id)
                temp=blob.name
                uri1= "gs://{}".format(gcs_bucket_name)
                uri =f'{uri1}/{temp}'
                print(uri)
                df=pd.read_json(uri)
                df.to_csv(uri,index=False)
                df=trim_all_columns(df)
                df1=df 
                df2=df1
            if blob.name.endswith('avro'):    #Checking for avro files in the gcs folder
                filename = re.findall(r".*/(.*).avro",blob.name) #Extracting file name for BQ's table id
                filenametest=filename[0].strip().replace(" ", "")
                print(filenametest)
                table_id = f'{project_name}.{dataset_name}.{filenametest}' # Determining table name
                print(table_id)
                temp=blob.name
                uri1= "gs://{}".format(gcs_bucket_name)
                uri =f'{uri1}/{temp}'
                print(uri)
                my_temp="/tmp/{filenametest}.avro"
                blob.download_to_filename(my_temp)
                with open(my_temp, "rb") as fp:
                    reader = fastavro.reader(fp)
                    records = [r for r in reader]
                    df = pd.DataFrame.from_records(records)
                df=trim_all_columns(df)
                df1=df 
                df2=df1
            if blob.name.endswith('tsv'):    #Checking for tsv files in the gcs folder
                filename = re.findall(r".*/(.*).tsv",blob.name) #Extracting file name for BQ's table id
                filenametest=filename[0].strip().replace(" ", "")
                print(filenametest)
                table_id = f'{project_name}.{dataset_name}.{filenametest}' # Determining table name
                print(table_id)
                temp=blob.name
                uri1= "gs://{}".format(gcs_bucket_name)
                uri =f'{uri1}/{temp}'
                print(uri)
                df=pd.read_table(uri,sep='\t')
                print('a') 
                df.to_csv(uri,index=False)
                df=trim_all_columns(df)
                df1=df 
                df2=df1
            if blob.name.endswith('psv'):    #Checking for psv files in the gcs folder
                filename = re.findall(r".*/(.*).psv",blob.name) #Extracting file name for BQ's table id
                filenametest=filename[0].strip().replace(" ", "")
                print(filenametest)
                table_id = f'{project_name}.{dataset_name}.{filenametest}' # Determining table name
                print(table_id)
                temp=blob.name
                uri1= "gs://{}".format(gcs_bucket_name)
                uri =f'{uri1}/{temp}'
                print(uri)
                df=pd.read_csv(uri,sep='|')
                df.to_csv(uri,index=False)
                df=trim_all_columns(df)
                df1=df 
                df2=df1
            if blob.name.endswith('xlsx'):   #Checking for xlsx files in the gcs folder
                filename = re.findall(r".*/(.*).xlsx",blob.name) #Extracting file name for BQ's table id
                filenametest=filename[0].strip().replace(" ", "")
                print(filenametest)
                table_id = f'{project_name}.{dataset_name}.{filenametest}' # Determining table name
                print(table_id)
                temp=blob.name
                uri1= "gs://{}".format(gcs_bucket_name)
                uri =f'{uri1}/{temp}'
                print(uri)
                df=pd.read_excel(uri)
                df.to_csv(uri,index=False)
                df=trim_all_columns(df)
                df1=df 
                df2=df1
            if blob.name.endswith('xls'):   #Checking for xls files in the gcs folder
                filename = re.findall(r".*/(.*).xls",blob.name) #Extracting file name for BQ's table id
                filenametest=filename[0].strip().replace(" ", "")
                print(filenametest)
                table_id = f'{project_name}.{dataset_name}.{filenametest}' # Determining table name
                print(table_id)
                temp=blob.name
                uri1= "gs://{}".format(gcs_bucket_name)
                uri =f'{uri1}/{temp}'
                print(uri)
                df=pd.read_excel(uri)
                df.to_csv(uri,index=False)
                df=trim_all_columns(df)
                df1=df 
                df2=df1
            if blob.name.endswith('xml'):   #Checking for psv blobs as list_blobs also returns folder_name
                filename = re.findall(r".*/(.*).xml",blob.name) #Extracting file name for BQ's table id
                filenametest=filename[0].strip().replace(" ", "")
                print(filenametest)
                table_id = f'{project_name}.{dataset_name}.{filenametest}' # Determining table name
                print(table_id)
                temp=blob.name
                uri1= "gs://{}".format(gcs_bucket_name)
                uri =f'{uri1}/{temp}'
                print(uri)
                df=pd.read_xml(uri)
                #df.to_csv(uri,index=False)
                df=trim_all_columns(df)
                df1=df
                df2=df1
            if blob.name.endswith('orc'):    #Checking for csv files in the gcs folder
                filename = re.findall(r".*/(.*).orc",blob.name) #Extracting file name for BQ's table id
                filenametest=filename[0].strip().replace(" ", "")
                print(filenametest)
                table_id = f'{project_name}.{dataset_name}.{filenametest}' # Determining table name
                print(table_id)
                temp=blob.name
                uri1= "gs://{}".format(gcs_bucket_name)
                uri =f'{uri1}/{temp}'
                print(uri)
                df=pd.read_orc(uri)
                df=trim_all_columns(df) 
                df1=df     #trimming the data
                df2=df1
            if blob.name.endswith('parquet'):    #Checking for csv files in the gcs folder
                filename = re.findall(r".*/(.*).parquet",blob.name) #Extracting file name for BQ's table id
                filenametest=filename[0].strip().replace(" ", "")
                print(filenametest)
                table_id = f'{project_name}.{dataset_name}.{filenametest}' # Determining table name
                print(table_id)
                temp=blob.name
                uri1= "gs://{}".format(gcs_bucket_name)
                uri =f'{uri1}/{temp}'
                print(uri)
                df=pd.read_parquet(uri)
                df=trim_all_columns(df) 
                df1=df     #trimming the data
                df2=df1
            if blob.name.endswith('log'):    #Checking for psv files in the gcs folder
                filename = re.findall(r".*/(.*).log",blob.name) #Extracting file name for BQ's table id
                filenametest=filename[0].strip().replace(" ", "")
                print(filenametest)
                table_id = f'{project_name}.{dataset_name}.{filenametest}' # Determining table name
                print(table_id)
                temp=blob.name
                uri1= "gs://{}".format(gcs_bucket_name)
                uri =f'{uri1}/{temp}'
                print(uri)
                df=pd.read_table(uri,sep='\s\s+', engine='python')
                #df.to_csv(uri,index=False)
                df=trim_all_columns(df)
                df1=df 
                df2=df1
            destination_tables_list.append(filenametest)   #extracting the destination table names
            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )     
            load_job = bigquery_client.load_table_from_dataframe(
                df,
                table_id,
                location="us-central1",   # Must match the destination dataset location.
                job_config=job_config,
            )  # Make an API request.
            load_job.result()
            destination_table = bigquery_client.get_table(table_id)
            print("Loaded {} rows.".format(destination_table.num_rows))

            #Date conversion
            if Variables_dict[filenametest+'_dateconversion']!='None':
                print('entered another if')
                for i in Variables_dict[filenametest+'_dateconversion']:
                    print(i)
                    df[i] = pd.to_datetime(df[i])
                    print(df)    
            print('date conversion has done successfully')
            print(df)

            #extracting the count of destination tables
            query_check = f"SELECT count(*) FROM {dataset_name}.{filenametest}"
            print(query_check) 
            query_job_check = bigquery_client.query(query_check) 
            print("The data:")
            for row in query_job_check:
                print(row[0])
                count_of_destination_tables.append(row[0])
                print(count_of_destination_tables)

            #checking the duplicates and uploading them to bq duplicates tables
            print(Variables_dict[filenametest])
            if Variables_dict[filenametest+'_duplicates']!='None':
                print('entered if')
                for i in Variables_dict[filenametest+'_duplicates']:
                    print(i)
                    distinct_check_query=f"SELECT {i}, COUNT({i}) AS count FROM {dataset_name}.{filenametest} GROUP BY {i} HAVING COUNT({i})>1"
                    print(distinct_check_query)
                    query_job = bigquery_client.query(distinct_check_query)  # Make an API request
                    print(query_job)
                    df = query_job.to_dataframe()  # Stores your query results to dataframe
                    print(df)
                    print(type(df))
                    if df.empty:
                        print(f'No duplicates found for {filenametest}.{i}')
                    else:
                        df.to_gbq(
                        destination_table=f'{validation_nullvalues_dataset}.{filenametest}_{i}_duplicates',
                        project_id=gcpprojectname,
                        if_exists="append"  # 3 available methods: fail/replace/append
                        )
                print('duplicate rows successfully Loaded to bq duplicates table')
            
            print(Variables_dict[filenametest])
            if Variables_dict[filenametest+'_maskdata']!='None':
                print('entered if')
                for i in Variables_dict[filenametest+'_maskdata']:
                    print(i)
                    distinct_check_query=f"SELECT {i} FROM {dataset_name}.{filenametest} limit 10"
                    print(distinct_check_query)
                    query_job = bigquery_client.query(distinct_check_query)  # Make an API request
                    print(query_job)
                    df = query_job.to_dataframe()  # Stores your query results to dataframe
                    print(df)
                    print(type(df))
                    if df.empty:
                        print(f'No data found for {filenametest}.{i}')
                    else:
                        df.to_gbq(
                        destination_table=f'{validation_nullvalues_dataset}.{filenametest}_{i}_maskdata',
                        project_id=gcpprojectname,
                        if_exists="append"  # 3 available methods: fail/replace/append
                        )
                print('maskdata successfully Loaded to bq maskdata table')

        

            #checking the nullvalues and uploading them to bq nullvalues tables
            if Variables_dict[filenametest+'_nullvalues']!='None':
                print('entered another if')
                for i in Variables_dict[filenametest+'_nullvalues']:
                    print(i)
                    nullvalues_check_query=f"SELECT * FROM {dataset_name}.{filenametest} WHERE {i} IS NULL"
                    print(nullvalues_check_query)
                    query_job = bigquery_client.query(nullvalues_check_query)  # Make an API request
                    print(query_job)
                    df = query_job.to_dataframe()  # Stores your query results to dataframe
                    print(df)
                    print(type(df))
                    if df.empty:
                        print(f'No nullvalues found for {filenametest}.{i}')
                    else:
                        df.to_gbq(
                        destination_table=f'{validation_nullvalues_dataset}.{filenametest}_{i}_nullvalues',
                        project_id=gcpprojectname,
                        if_exists="append"  # 3 available methods: fail/replace/append
                        )
                print('null values successfully Loaded to bq nullvalues table')
            
            #appending null values with 'zero'
            if Variables_dict[filenametest]!='None':
                print('entered another if')
                for i in Variables_dict[filenametest]:
                    print(i)
                    append_zero=f"UPDATE {dataset_name}.{filenametest} SET {i}=0 WHERE {i} IS NULL"
                    print(append_zero)
                    query_job = bigquery_client.query(append_zero)  # Make an API request
                    print(query_job)
                print('zero successfully appended to null values')

            if Variables_dict[filenametest+'_columnsum']!='None':
                print('entered another if')
                for i in Variables_dict[filenametest+'_columnsum']:
                    print(i)
                    
                    csvsum=df1[i].sum()
                    print(csvsum)
                    csvsumdec="%.2f"%csvsum
                    print(csvsum)
                    print(csvsumdec)
                    
                    #extracting the count of destination tables
                    dest_sum_query = f"SELECT sum({i}) FROM {dataset_name}.{filenametest}"
                    print(dest_sum_query)
                    query_job_check = bigquery_client.query(dest_sum_query) 
                    print("The data:")
                    for row in query_job_check:
                        dest_sum=print(row[0])
                        z=row[0]
                        y="%.2f"%z
                        print(y)

                    if csvsumdec==y:
                        check_result='sum count matched'
                        print('sum count matched')        
                        query = f"INSERT INTO {gcpprojectname}.{validation_nullvalues_dataset}.{sum_check_validation_table} VALUES (@table_name,@col_name,@src_cnt,@dest_cnt,@result)"
                    else:
                        check_result='sum count mismatched'
                        print('sum count mismatched')
                        query = f"INSERT INTO {gcpprojectname}.{validation_nullvalues_dataset}.{sum_check_validation_table} VALUES (@table_name,@col_name,@src_cnt,@dest_cnt,@result)"
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("table_name", "STRING", filenametest),
                            bigquery.ScalarQueryParameter("col_name", "STRING", i),
                            bigquery.ScalarQueryParameter("src_cnt", "FLOAT", csvsumdec),
                            bigquery.ScalarQueryParameter("dest_cnt", "FLOAT", y),
                            bigquery.ScalarQueryParameter("result", "STRING", check_result)
                        ]
                    )
                    bigquery_client.query(query, job_config=job_config) 
            print('sum_comparison data successfully loaded to sum_check_table')        
        print('data successfully loaded from gcs to bigquery')

    #Function to validate source(MYSQL or FTP) and destination(BigQuery) tables count and insert the validation data to validation table in BigQuery
    def source_destination_count_validation_function(source_tables_list,destination_tables_list,count_of_source_tables,count_of_destination_tables):
        print(source_tables_list)
        print(destination_tables_list)
        print(count_of_source_tables)
        print(count_of_destination_tables) 
        for i in range(len(source_tables_list)):
            print(i)
            print(source_tables_list[i])
            print(destination_tables_list[i])
            print(count_of_source_tables[i])
            print(count_of_destination_tables[i])
            if count_of_source_tables[i]==count_of_destination_tables[i]:
                result='count matched'
                print('count matched')    
            else:
                result='count mismatched'
                print('count mismatched')
            query = f"INSERT INTO {gcpprojectname}.{validation_nullvalues_dataset}.{bq_validation_table} VALUES (@sourcetableslist,@destinationtableslist,@countofsourcetables,@countofdestinationtables,@ts_value)"
            print(query)
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("sourcetableslist", "STRING", source_tables_list[i]),
                    bigquery.ScalarQueryParameter("destinationtableslist", "STRING",destination_tables_list[i]),
                    bigquery.ScalarQueryParameter("countofsourcetables", "INT64", count_of_source_tables[i]),
                    bigquery.ScalarQueryParameter("countofdestinationtables", "INT64", count_of_destination_tables[i]),
                    bigquery.ScalarQueryParameter("ts_value","STRING",result)
                ]
            )
            print(query)
            bigquery_client.query(query, job_config=job_config)  
            # Make an API request.
        print('count data successfully loaded to validation_table')

    #Function to read the logging data from /tmp/mysql_to_bq.log file and load it to bigquery table
    def insert_logging_info_to_bqtable(log_file_path,project_name,dataset_name,log_table): 
        f = open(log_file_path, "r")
        h=f.readlines()
        print(h[0])
        linelist = h[0].strip().split(",")
        timestamp=linelist[0]
        starttime = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')  #start time of process
        print(starttime)
        print(h[-1])
        linelist = h[-1].strip().split(",")
        timestamp1=linelist[0]
        endtime = datetime.datetime.strptime(timestamp1, '%Y-%m-%d %H:%M:%S')  #end time of process
        print(endtime)
        duration_time=endtime-starttime   
        print(SLAtimeinseconds)       #variable declared in parameter file
        durationtimeinseconds=duration_time.seconds
        print(durationtimeinseconds)  #total time taken 
        f.close()
        if(durationtimeinseconds<=SLAtimeinseconds):
            checkresult='within range'
            print('within range')
        else:
            checkresult='out of range'
            print('out of range')
        query = f"INSERT INTO {project_name}.{dataset_name}.{bq_logging_info_table} VALUES (@start_time,@end_time,@duration_time,@SLA_time,@check_result,@source_type)"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", starttime),
                bigquery.ScalarQueryParameter("end_time", "TIMESTAMP",endtime),
                bigquery.ScalarQueryParameter("duration_time", "INT64", durationtimeinseconds),
                bigquery.ScalarQueryParameter("SLA_time", "INT64", SLAtimeinseconds),
                bigquery.ScalarQueryParameter("check_result","STRING",checkresult),
                bigquery.ScalarQueryParameter("source_type","STRING",source_type)
            ]
        )
        bigquery_client.query(query, job_config=job_config)   
            # Make an API request.
        print('logging data successfully loaded to validation')


    #1
    #function to load the data from MYSQL to GCS to BIGQUERY
    def mysql_to_bq_full_load():
        logger.debug('MySQL Fullload job started')

        source_files_backup_to_gcs_folder(mysql_to_gcs_bucket_name,mysql_table_files_backup_bucket,mysql_to_gcs_subfolder,'csv') #function call to backup the source files from mysql database 
        print('mysql source_files_backup method success')
        
        #code to load the mysql tables data to google cloud storage bucket
        for tablename in source_tables_list:
            #extracting source tables count
            source_count_query=f'SELECT count(*) FROM {tablename}'
            print(source_count_query)
            ls=pd.read_sql_query(source_count_query,pool)
            print(type(ls))
            print(ls)
            #Extracting source tables count---converting pandas Dataframe to temporary mysql table i.e,employeedata and store the count of tables
            ls.to_sql('employeedata',
                con = pool,
                if_exists = 'replace')
            # run a sql query
            temp=pool.execute("SELECT * FROM employeedata").fetchall()
            source_count=temp[0][1]
            mysql_count_of_source_tables.append(source_count)    #appending the count to source count list(count_of_source_tables)
            query2=f"SELECT * FROM {tablename}"
            df = pd.read_sql_query(query2, pool)    #extract the data from mysql table and load to pandas framework
            remote_path = f'{mysql_to_gcs_subfolder}/{tablename}.csv'
            mysqltogcsbucket = storage_client.get_bucket(mysql_to_gcs_bucket_name)
            blob = mysqltogcsbucket.blob(remote_path)
            blob.upload_from_string(df.to_csv(index=False), 'text/csv') 
        print('data successfully loaded from cloud sql mysql database to google cloud storage')

        bq_create_dataset(bigquery_client,mysql_to_bq_datasetname) #function call---to create the dataset dynamically
        print('mysql create_dataset method success')

        bqtables_backup_to_gcs_folder(gcpprojectname,mysql_to_bq_datasetname,mysql_bq_backup_bucket_name,'CSV') #function call---to backup the bigquery tables to gcs folder before truncating
        print('bq_tables_backup method success')

        gcs_to_bigquery(mysql_to_gcs_bucket_name,mysql_to_gcs_subfolder,gcpprojectname,mysql_to_bq_datasetname,mysql_destination_tables_list,mysql_count_of_destination_tables)  #function call---to upload the files from gcs bucket to bigquery tables 
        print('mysql-gcs_to_bq method success')

        source_destination_count_validation_function(source_tables_list,mysql_destination_tables_list,mysql_count_of_source_tables,mysql_count_of_destination_tables)#function call--to validate source and destination count and upload related data to bq validation table
        print('mysql-count method success')

        logger.debug('Mysql Fullload job successfully completed')  

        insert_logging_info_to_bqtable(mysql_logfile_path,gcpprojectname,validation_nullvalues_dataset,bq_logging_info_table) #function call--to insert the log data to bigquery logging_info_table
        print('logging method success')

        print('data successfully loaded from mysql database to bq')


    #2
    #function to move files from FTP_csv to GCS to bigquery
    def ftp_csv_to_bq_full_load():
        logger.debug('FTP_CSV Fullload job started')
        bucket = storage_client.get_bucket(FTP_csv_to_GCS_bucket)

        source_files_backup_to_gcs_folder(FTP_csv_to_GCS_bucket,FTP_csv_source_files_backup_bucket,FTP_csv_gcs_destination_folder,'csv')    #function call to backup the source files from ftp server(csv files) 
        print('ftp_csv source_files_backup method success')

        get_ftp(FTP_HOST, FTP_USER, FTP_PASS,'.csv',FTP_csv_to_GCS_bucket,FTP_csv_gcs_destination_folder,FTP_csv_source_files_list,FTP_csv_count_of_source_files)  #function to load the files(csv) from ftp server to gcs bucket
        print('data successfully loaded from ftp_csv to gcs')

        bq_create_dataset(bigquery_client,FTP_to_BQ_csv_dataset)  #function call---to create the dataset dynamically
        print('ftp_csv create_dataset method success')

        bqtables_backup_to_gcs_folder(gcpprojectname,FTP_to_BQ_csv_dataset,FTP_csv_bq_tables_backup_bucket,'CSV') #function call---to backup the bigquery tables to gcs folder before truncating
        print('ftp_csv-bq_tables_backup method success')

        gcs_to_bigquery(FTP_csv_to_GCS_bucket,FTP_csv_gcs_destination_folder,gcpprojectname,FTP_to_BQ_csv_dataset,FTP_csv_destination_tables_list,FTP_csv_count_of_destination_tables)  #function call---to upload the files from gcs bucket to bigquery tables 
        print('ftp_csv-gcs_to_bq method success')

        source_destination_count_validation_function(FTP_csv_source_files_list,FTP_csv_destination_tables_list,FTP_csv_count_of_source_files,FTP_csv_count_of_destination_tables) #function call--to validate source and destination count and upload related data to bq validation table
        print('ftp_csv-count method success')

        logger.debug('FTP_CSV Fullload job successfully completed')

        insert_logging_info_to_bqtable(mysql_logfile_path,gcpprojectname,validation_nullvalues_dataset,bq_logging_info_table) #function call--to insert the log data to bigquery logging_info_table
        print('ftp_csv logging method success')

        print("data successfully loaded from FTP_csv to bq")


    #3
    #function to move files from FTP_json to GCS to bigquery
    def ftp_json_to_bq_full_load():
        logger.debug('FTP_JSON Fullload job started')

        bucket = storage_client.get_bucket(FTP_json_to_GCS_bucket)

        source_files_backup_to_gcs_folder(FTP_json_to_GCS_bucket,FTP_json_source_files_backup_bucket,FTP_json_gcs_destination_folder,'json')    #function call to backup the source files from ftp server(json files)
        print('ftp_json source_files_backup method success')

        get_ftp(FTP_HOST, FTP_USER, FTP_PASS,'.json',FTP_json_to_GCS_bucket,FTP_json_gcs_destination_folder,FTP_json_source_files_list,FTP_json_count_of_source_files) #function call to load the files(json) from ftp server to gcs bucket
        print('data successfully loaded from ftp_json to gcs')

        bq_create_dataset(bigquery_client,FTP_to_BQ_json_dataset)  #function call---to create the dataset dynamically
        print('ftp_json create_dataset method success')

        bqtables_backup_to_gcs_folder(gcpprojectname,FTP_to_BQ_json_dataset,FTP_json_bq_tables_backup_bucket,'NEWLINE_DELIMITED_JSON') #function call---to backup the bigquery tables to gcs folder before truncating
        print('ftp_json-bq_tables_backup method success')

        gcs_to_bigquery(FTP_json_to_GCS_bucket,FTP_json_gcs_destination_folder,gcpprojectname,FTP_to_BQ_json_dataset,FTP_json_destination_tables_list,FTP_json_count_of_destination_tables)   #function call---to upload the files from gcs bucket to bigquery tables
        print('ftp_json-gcs_to_bq method success')

        source_destination_count_validation_function(FTP_json_source_files_list,FTP_json_destination_tables_list,FTP_json_count_of_source_files,FTP_json_count_of_destination_tables) #function call--to validate source and destination count and upload related data to bq validation table
        print('ftp_json-count method success')

        logger.debug('FTP_JSON Fullload job successfully completed')

        insert_logging_info_to_bqtable(mysql_logfile_path,gcpprojectname,validation_nullvalues_dataset,bq_logging_info_table) #function call--to insert the log data to bigquery logging_info_table
        print('ftp_json logging method success')

        print("data successfully loaded from FTP_json to bq")


    #4
    #function to move files from FTP_avro to GCS to bigquery
    def ftp_avro_to_bq_full_load():
        logger.debug('FTP_AVRO Fullload job started')

        bucket = storage_client.get_bucket(FTP_avro_to_GCS_bucket)

        source_files_backup_to_gcs_folder(FTP_avro_to_GCS_bucket,FTP_avro_source_files_backup_bucket,FTP_avro_gcs_destination_folder,'avro')    #function call to backup the source files from ftp server(avro files)
        print('ftp_avro source_files_backup method success')

        get_ftp(FTP_HOST, FTP_USER, FTP_PASS,'.avro',FTP_avro_to_GCS_bucket,FTP_avro_gcs_destination_folder,FTP_avro_source_files_list,FTP_avro_count_of_source_files) #function to load the files(avro) from ftp server to gcs bucket
        print('data successfully loaded from ftp_avro to gcs')

        bq_create_dataset(bigquery_client,FTP_to_BQ_avro_dataset)  #function call---to create the dataset dynamically
        print('ftp_csv create_dataset method success')

        bqtables_backup_to_gcs_folder(gcpprojectname,FTP_to_BQ_avro_dataset,FTP_avro_bq_tables_backup_bucket,'AVRO') #function call---to backup the bigquery tables to gcs folder before truncating
        print('ftp_avro-bq_tables_backup method success')

        gcs_to_bigquery(FTP_avro_to_GCS_bucket,FTP_avro_gcs_destination_folder,gcpprojectname,FTP_to_BQ_avro_dataset,FTP_avro_destination_tables_list,FTP_avro_count_of_destination_tables)   #function call---to upload the files from gcs bucket to bigquery tables
        print('ftp_avro-gcs_to_bq method success')

        source_destination_count_validation_function(FTP_avro_source_files_list,FTP_avro_destination_tables_list,FTP_avro_count_of_source_files,FTP_avro_count_of_destination_tables) #function call--to validate source and destination count and upload related data to bq validation table
        print('ftp_avro-count method success')

        logger.debug('FTP_AVRO Fullload job successfully completed')

        insert_logging_info_to_bqtable(mysql_logfile_path,gcpprojectname,validation_nullvalues_dataset,bq_logging_info_table) #function call--to insert the log data to bigquery logging_info_table
        print('ftp_avro logging method success')

        print("data successfully loaded from FTP_avro to bq")


    #5
    #function to move files from FTP_tsv to GCS to bigquery
    def ftp_tsv_to_bq_full_load():
        logger.debug('FTP_TSV Fullload job started')

        bucket = storage_client.get_bucket(FTP_tsv_to_GCS_bucket)

        source_files_backup_to_gcs_folder(FTP_tsv_to_GCS_bucket,FTP_tsv_source_files_backup_bucket,FTP_tsv_gcs_destination_folder,'tsv')    #function call to backup the source files from ftp server(tsv files)
        print('ftp_tsv source_files_backup method success')

        get_ftp(FTP_HOST, FTP_USER, FTP_PASS,'.tsv',FTP_tsv_to_GCS_bucket,FTP_tsv_gcs_destination_folder,FTP_tsv_source_files_list,FTP_tsv_count_of_source_files)  #function to load the files(tsv) from ftp server to gcs bucket
        print('data successfully loaded from ftp_tsv to gcs')

        bq_create_dataset(bigquery_client,FTP_to_BQ_tsv_dataset)  #function call---to create the dataset dynamically
        print('ftp_tsv create_dataset method success')

        bqtables_backup_to_gcs_folder(gcpprojectname,FTP_to_BQ_tsv_dataset,FTP_tsv_bq_tables_backup_bucket,'TSV') #function call---to backup the bigquery tables to gcs folder before truncating
        print('ftp_tsv-bq_tables_backup method success')

        gcs_to_bigquery(FTP_tsv_to_GCS_bucket,FTP_tsv_gcs_destination_folder,gcpprojectname,FTP_to_BQ_tsv_dataset,FTP_tsv_destination_tables_list,FTP_tsv_count_of_destination_tables)   #function call---to upload the files from gcs bucket to bigquery tables
        print('ftp_csv-gcs_to_bq method success')

        source_destination_count_validation_function(FTP_tsv_source_files_list,FTP_tsv_destination_tables_list,FTP_tsv_count_of_source_files,FTP_tsv_count_of_destination_tables)  #function call--to validate source and destination count and upload related data to bq validation table
        print('ftp_tsv-count method success')

        logger.debug('FTP_TSV Fullload job successfully completed')

        insert_logging_info_to_bqtable(mysql_logfile_path,gcpprojectname,validation_nullvalues_dataset,bq_logging_info_table) #function call--to insert the log data to bigquery logging_info_table
        print('ftp_tsv logging method success')

        print("data successfully loaded from FTP_tsv to bq")


    #6
    #function to move files from FTP_psv to GCS to bigquery
    def ftp_psv_to_bq_full_load():
        logger.debug('FTP_PSV Fullload job started')

        bucket = storage_client.get_bucket(FTP_psv_to_GCS_bucket)

        source_files_backup_to_gcs_folder(FTP_psv_to_GCS_bucket,FTP_psv_source_files_backup_bucket,FTP_psv_gcs_destination_folder,'psv')    #function call to backup the source files from ftp server(psv files)
        print('ftp_psv source_files_backup method success')

        get_ftp(FTP_HOST, FTP_USER, FTP_PASS,'.psv',FTP_psv_to_GCS_bucket,FTP_psv_gcs_destination_folder,FTP_psv_source_files_list,FTP_psv_count_of_source_files)  #function to load the files(psv) from ftp server to gcs bucket
        print('data successfully loaded from ftp_psv to gcs')

        bq_create_dataset(bigquery_client,FTP_to_BQ_psv_dataset)  #function call---to create the dataset dynamically
        print('ftp_psv create_dataset method success')

        bqtables_backup_to_gcs_folder(gcpprojectname,FTP_to_BQ_psv_dataset,FTP_psv_bq_tables_backup_bucket,'PSV') #function call---to backup the bigquery tables to gcs folder before truncating
        print('ftp_csv-bq_tables_backup method success')

        gcs_to_bigquery(FTP_psv_to_GCS_bucket,FTP_psv_gcs_destination_folder,gcpprojectname,FTP_to_BQ_psv_dataset,FTP_psv_destination_tables_list,FTP_psv_count_of_destination_tables)   #function call---to upload the files from gcs bucket to bigquery tables
        print('ftp_csv-gcs_to_bq method success')

        source_destination_count_validation_function(FTP_psv_source_files_list,FTP_psv_destination_tables_list,FTP_psv_count_of_source_files,FTP_psv_count_of_destination_tables) #function call--to validate source and destination count and upload related data to bq validation table
        print('ftp_psv-count method success')

        logger.debug('FTP_PSV Fullload job successfully completed')

        insert_logging_info_to_bqtable(mysql_logfile_path,gcpprojectname,validation_nullvalues_dataset,bq_logging_info_table) #function call--to insert the log data to bigquery logging_info_table
        print('ftp_psv logging method success')

        print("data successfully loaded from FTP_psv to bq")


    #7
    #function to move files from FTP_xlsx to GCS to bigquery
    def ftp_xlsx_to_bq_full_load():
        logger.debug('FTP_XLSX Fullload job started')

        bucket = storage_client.get_bucket(FTP_xlsx_to_GCS_bucket)

        source_files_backup_to_gcs_folder(FTP_xlsx_to_GCS_bucket,FTP_xlsx_source_files_backup_bucket,FTP_xlsx_gcs_destination_folder,'xlsx')    #function call to backup the source files from ftp server(xlsx files)
        print('ftp_xlsx source_files_backup method success')

        get_ftp(FTP_HOST, FTP_USER, FTP_PASS,'.xlsx',FTP_xlsx_to_GCS_bucket,FTP_xlsx_gcs_destination_folder,FTP_xlsx_source_files_list,FTP_xlsx_count_of_source_files)  #function to load the files(xlsx) from ftp server to gcs bucket
        print('data successfully loaded from ftp_xlsx to gcs')

        bq_create_dataset(bigquery_client,FTP_to_BQ_xlsx_dataset)  #function call---to create the dataset dynamically
        print('ftp_xlsx create_dataset method success')

        bqtables_backup_to_gcs_folder(gcpprojectname,FTP_to_BQ_xlsx_dataset,FTP_xlsx_bq_tables_backup_bucket,'XLSX') #function call---to backup the bigquery tables to gcs folder before truncating
        print('ftp_xlsx-bq_tables_backup method success')

        gcs_to_bigquery(FTP_xlsx_to_GCS_bucket,FTP_xlsx_gcs_destination_folder,gcpprojectname,FTP_to_BQ_xlsx_dataset,FTP_xlsx_destination_tables_list,FTP_xlsx_count_of_destination_tables)   #function call---to upload the files from gcs bucket to bigquery tables
        print('ftp_xlsx-gcs_to_bq method success')

        source_destination_count_validation_function(FTP_xlsx_source_files_list,FTP_xlsx_destination_tables_list,FTP_xlsx_count_of_source_files,FTP_xlsx_count_of_destination_tables)  #function call--to validate source and destination count and upload related data to bq validation table
        print('ftp_xlsx-count method success')

        logger.debug('FTP_XLSX Fullload job successfully completed')

        insert_logging_info_to_bqtable(mysql_logfile_path,gcpprojectname,validation_nullvalues_dataset,bq_logging_info_table) #function call--to insert the log data to bigquery logging_info_table
        print('ftp_xlsx logging method success')

        print("data successfully loaded from FTP_xlsx to bq")


    #8
    #function to move files from FTP_xls to GCS to bigquery
    def ftp_xls_to_bq_full_load():
        logger.debug('FTP_XLS Fullload job started')

        bucket = storage_client.get_bucket(FTP_xls_to_GCS_bucket)

        source_files_backup_to_gcs_folder(FTP_xls_to_GCS_bucket,FTP_xls_source_files_backup_bucket,FTP_xls_gcs_destination_folder,'xls')    #function call to backup the source files from ftp server(xls files)
        print('ftp_xls source_files_backup method success')

        get_ftp(FTP_HOST, FTP_USER, FTP_PASS,'.xls',FTP_xls_to_GCS_bucket,FTP_xls_gcs_destination_folder,FTP_xls_source_files_list,FTP_xls_count_of_source_files)  #function to load the files(xls) from ftp server to gcs bucket
        print('data successfully loaded from ftp_xls to gcs')

        bq_create_dataset(bigquery_client,FTP_to_BQ_xls_dataset)  #function call---to create the dataset dynamically
        print('ftp_xls create_dataset method success')

        bqtables_backup_to_gcs_folder(gcpprojectname,FTP_to_BQ_xls_dataset,FTP_xls_bq_tables_backup_bucket,'XLS') #function call---to backup the bigquery tables to gcs folder before truncating
        print('ftp_xls-bq_tables_backup method success')

        gcs_to_bigquery(FTP_xls_to_GCS_bucket,FTP_xls_gcs_destination_folder,gcpprojectname,FTP_to_BQ_xls_dataset,FTP_xls_destination_tables_list,FTP_xls_count_of_destination_tables)   #function call---to upload the files from gcs bucket to bigquery tables
        print('ftp_xls-gcs_to_bq method success')

        source_destination_count_validation_function(FTP_xls_source_files_list,FTP_xls_destination_tables_list,FTP_xls_count_of_source_files,FTP_xls_count_of_destination_tables) #function call--to validate source and destination count and upload related data to bq validation table
        print('ftp_xls-count method success')

        logger.debug('FTP_XLS Fullload job successfully completed')

        insert_logging_info_to_bqtable(mysql_logfile_path,gcpprojectname,validation_nullvalues_dataset,bq_logging_info_table) #function call--to insert the log data to bigquery logging_info_table
        print('ftp_xls logging method success')

        print("data successfully loaded from FTP_xls to bq")

    
    #9
    #function to move files from FTP_xml to GCS to bigquery
    def ftp_xml_to_bq_full_load():
        logger.debug('FTP_XML Fullload job started')

        bucket = storage_client.get_bucket(FTP_xml_to_GCS_bucket)

        source_files_backup_to_gcs_folder(FTP_xml_to_GCS_bucket,FTP_xml_source_files_backup_bucket,FTP_xml_gcs_destination_folder,'xml')    #function call to backup the source files from ftp server(xls files)
        print('ftp_xml source_files_backup method success')

        get_ftp(FTP_HOST, FTP_USER, FTP_PASS,'.xml',FTP_xml_to_GCS_bucket,FTP_xml_gcs_destination_folder,FTP_xml_source_files_list,FTP_xml_count_of_source_files)  #function to load the files(xls) from ftp server to gcs bucket
        print('data successfully loaded from ftp_xml to gcs')

        bq_create_dataset(bigquery_client,FTP_to_BQ_xml_dataset)  #function call---to create the dataset dynamically
        print('ftp_xml create_dataset method success')

        bqtables_backup_to_gcs_folder(gcpprojectname,FTP_to_BQ_xml_dataset,FTP_xml_bq_tables_backup_bucket,'XML') #function call---to backup the bigquery tables to gcs folder before truncating
        print('ftp_xml-bq_tables_backup method success')

        gcs_to_bigquery(FTP_xml_to_GCS_bucket,FTP_xml_gcs_destination_folder,gcpprojectname,FTP_to_BQ_xml_dataset,FTP_xml_destination_tables_list,FTP_xml_count_of_destination_tables)   #function call---to upload the files from gcs bucket to bigquery tables
        print('ftp_xml-gcs_to_bq method success')

        source_destination_count_validation_function(FTP_xml_source_files_list,FTP_xml_destination_tables_list,FTP_xml_count_of_source_files,FTP_xml_count_of_destination_tables) #function call--to validate source and destination count and upload related data to bq validation table
        print('ftp_xml-count method success')

        logger.debug('FTP_XML Fullload job successfully completed')

        insert_logging_info_to_bqtable(mysql_logfile_path,gcpprojectname,validation_nullvalues_dataset,bq_logging_info_table) #function call--to insert the log data to bigquery logging_info_table
        print('ftp_xml logging method success')

        print("data successfully loaded from FTP_xml to bq")

    #10
    #function to move files from FTP_orc to GCS to bigquery
    def ftp_orc_to_bq_full_load():
        logger.debug('FTP_ORC Fullload job started')
        bucket = storage_client.get_bucket(FTP_orc_to_GCS_bucket)

        source_files_backup_to_gcs_folder(FTP_orc_to_GCS_bucket,FTP_orc_source_files_backup_bucket,FTP_orc_gcs_destination_folder,'orc')    #function call to backup the source files from ftp server(csv files) 
        print('ftp_orc source_files_backup method success')

        get_ftp(FTP_HOST, FTP_USER, FTP_PASS,'.orc',FTP_orc_to_GCS_bucket,FTP_orc_gcs_destination_folder,FTP_orc_source_files_list,FTP_orc_count_of_source_files)  #function to load the files(csv) from ftp server to gcs bucket
        print('data successfully loaded from ftp_orc to gcs')

        bq_create_dataset(bigquery_client,FTP_to_BQ_orc_dataset)  #function call---to create the dataset dynamically
        print('ftp_orc create_dataset method success')

        bqtables_backup_to_gcs_folder(gcpprojectname,FTP_to_BQ_orc_dataset,FTP_orc_bq_tables_backup_bucket,'ORC') #function call---to backup the bigquery tables to gcs folder before truncating
        print('ftp_orc-bq_tables_backup method success')

        gcs_to_bigquery(FTP_orc_to_GCS_bucket,FTP_orc_gcs_destination_folder,gcpprojectname,FTP_to_BQ_orc_dataset,FTP_orc_destination_tables_list,FTP_orc_count_of_destination_tables)  #function call---to upload the files from gcs bucket to bigquery tables 
        print('ftp_orc-gcs_to_bq method success')

        source_destination_count_validation_function(FTP_orc_source_files_list,FTP_orc_destination_tables_list,FTP_orc_count_of_source_files,FTP_orc_count_of_destination_tables) #function call--to validate source and destination count and upload related data to bq validation table
        print('ftp_orc-count method success')

        logger.debug('FTP_ORC Fullload job successfully completed')

        insert_logging_info_to_bqtable(mysql_logfile_path,gcpprojectname,validation_nullvalues_dataset,bq_logging_info_table) #function call--to insert the log data to bigquery logging_info_table
        print('ftp_orc logging method success')

        print("data successfully loaded from FTP_orc to bq")

    #11
    #function to move files from FTP_parquet to GCS to bigquery
    def ftp_parquet_to_bq_full_load():
        logger.debug('FTP_parquet Fullload job started')
        bucket = storage_client.get_bucket(FTP_parquet_to_GCS_bucket)

        source_files_backup_to_gcs_folder(FTP_parquet_to_GCS_bucket,FTP_parquet_source_files_backup_bucket,FTP_parquet_gcs_destination_folder,'parquet')    #function call to backup the source files from ftp server(csv files) 
        print('ftp_parquet source_files_backup method success')

        get_ftp(FTP_HOST, FTP_USER, FTP_PASS,'.parquet',FTP_parquet_to_GCS_bucket,FTP_parquet_gcs_destination_folder,FTP_parquet_source_files_list,FTP_parquet_count_of_source_files)  #function to load the files(csv) from ftp server to gcs bucket
        print('data successfully loaded from ftp_parquet to gcs')

        bq_create_dataset(bigquery_client,FTP_to_BQ_parquet_dataset)  #function call---to create the dataset dynamically
        print('ftp_parquet create_dataset method success')

        bqtables_backup_to_gcs_folder(gcpprojectname,FTP_to_BQ_parquet_dataset,FTP_parquet_bq_tables_backup_bucket,'PARQUET') #function call---to backup the bigquery tables to gcs folder before truncating
        print('ftp_parquet-bq_tables_backup method success')

        gcs_to_bigquery(FTP_parquet_to_GCS_bucket,FTP_parquet_gcs_destination_folder,gcpprojectname,FTP_to_BQ_parquet_dataset,FTP_parquet_destination_tables_list,FTP_parquet_count_of_destination_tables)  #function call---to upload the files from gcs bucket to bigquery tables 
        print('ftp_parquet-gcs_to_bq method success')

        source_destination_count_validation_function(FTP_parquet_source_files_list,FTP_parquet_destination_tables_list,FTP_parquet_count_of_source_files,FTP_parquet_count_of_destination_tables) #function call--to validate source and destination count and upload related data to bq validation table
        print('ftp_parquet-count method success')

        logger.debug('FTP_PARQUET Fullload job successfully completed')

        insert_logging_info_to_bqtable(mysql_logfile_path,gcpprojectname,validation_nullvalues_dataset,bq_logging_info_table) #function call--to insert the log data to bigquery logging_info_table
        print('ftp_parquet logging method success')

        print("data successfully loaded from FTP_parquet to bq")

    #12
    #function to move files from FTP_log to GCS to bigquery
    def ftp_log_to_bq_full_load():
        logger.debug('FTP_log Fullload job started')
        bucket = storage_client.get_bucket(FTP_log_to_GCS_bucket)

        source_files_backup_to_gcs_folder(FTP_log_to_GCS_bucket,FTP_log_source_files_backup_bucket,FTP_log_gcs_destination_folder,'log')    #function call to backup the source files from ftp server(csv files) 
        print('ftp_log source_files_backup method success')

        get_ftp(FTP_HOST, FTP_USER, FTP_PASS,'.log',FTP_log_to_GCS_bucket,FTP_log_gcs_destination_folder,FTP_log_source_files_list,FTP_log_count_of_source_files)  #function to load the files(csv) from ftp server to gcs bucket
        print('data successfully loaded from ftp_log to gcs')

        bq_create_dataset(bigquery_client,FTP_to_BQ_log_dataset)  #function call---to create the dataset dynamically
        print('ftp_log create_dataset method success')

        bqtables_backup_to_gcs_folder(gcpprojectname,FTP_to_BQ_log_dataset,FTP_log_bq_tables_backup_bucket,'log') #function call---to backup the bigquery tables to gcs folder before truncating
        print('ftp_log-bq_tables_backup method success')

        gcs_to_bigquery(FTP_log_to_GCS_bucket,FTP_log_gcs_destination_folder,gcpprojectname,FTP_to_BQ_log_dataset,FTP_log_destination_tables_list,FTP_log_count_of_destination_tables)  #function call---to upload the files from gcs bucket to bigquery tables 
        print('ftp_log-gcs_to_bq method success')

        source_destination_count_validation_function(FTP_log_source_files_list,FTP_log_destination_tables_list,FTP_log_count_of_source_files,FTP_log_count_of_destination_tables) #function call--to validate source and destination count and upload related data to bq validation table
        print('ftp_log-count method success')

        logger.debug('FTP_log Fullload job successfully completed')

        insert_logging_info_to_bqtable(mysql_logfile_path,gcpprojectname,validation_nullvalues_dataset,bq_logging_info_table) #function call--to insert the log data to bigquery logging_info_table
        print('ftp_log logging method success')

        print("data successfully loaded from FTP_log to bq")

    mysql_to_bq_full_load()  
    ftp_csv_to_bq_full_load()
    ftp_json_to_bq_full_load()
    ftp_avro_to_bq_full_load()
    ftp_tsv_to_bq_full_load()
    ftp_psv_to_bq_full_load()
    ftp_xlsx_to_bq_full_load()
    ftp_xls_to_bq_full_load()
    ftp_xml_to_bq_full_load()
    ftp_orc_to_bq_full_load()
    ftp_parquet_to_bq_full_load()
    ftp_log_to_bq_full_load()

    return 'success'
    #multiprocessing code
    '''p1=multiprocessing.Process(target=mysql_to_bq_full_load,args=[])
    p2=multiprocessing.Process(target=ftp_csv_to_bq_full_load,args=[])
    p3=multiprocessing.Process(target=ftp_json_to_bq_full_load,args=[])
    p4=multiprocessing.Process(target=ftp_avro_to_bq_full_load,args=[])
    p5=multiprocessing.Process(target=ftp_tsv_to_bq_full_load,args=[])
    p6=multiprocessing.Process(target=ftp_psv_to_bq_full_load,args=[])
    p7=multiprocessing.Process(target=ftp_xlsx_to_bq_full_load,args=[])
    p8=multiprocessing.Process(target=ftp_xls_to_bq_full_load,args=[])
    if __name__=='__main__':
        p1.start()
        p2.start()
        p3.start()
        p4.start()
        p5.start()
        p6.start()
        p7.start()
        p8.start()      
        p1.join()
        p2.join()
        p3.join()
        p4.join()
        p5.join()
        p6.join()
        p7.join()
        p8.join()
    finish=time.perf_counter()
    print("finished running after seconds:",finish)
    print('Fullload job done successfully')'''






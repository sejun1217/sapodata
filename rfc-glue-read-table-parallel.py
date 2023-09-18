import os, sys, pyrfc

os.environ['LD_LIBRARY_PATH'] = os.path.dirname(pyrfc.__file__)

os.execv('/usr/bin/python3', ['/usr/bin/python3', '-c', """

import json
import boto3
from pyrfc import Connection
import pandas as pd
import io
import pyarrow as pa
import pyarrow.parquet as pq

# Input Your Environment
ASHOST='' #modify
CLIENT='' #modify
SYSNR=''  #modify
USER=''   #modify
PASSWD ='' #modify
dataS3Bucket = "" #modify - Speficy your bucket name to store the data e.g. <AccountID>-saprfcglue

# Input Table Info
READ_Table = "" #modify 
Table_Rowcount = 100000 #modify - Get Row count
maxEntities = 100000000 #modify - Max Row count

# RFC & Table settiong
RFC_Function = "/SAPDS/RFC_READ_TABLE2"
#TABLE_Fields = ["MANDT", "VBELN", "ERDAT", "ERZET"]
Table_Delimiter = '`'

# S3 Info
file_format = "parquet"
dataS3Folder = "glue/parallel/"+file_format+"/"+READ_Table+"/"
global dataS3file; dataS3file = READ_Table+"."+file_format
dataS3Folder_err = "glue/parallel-err/"+file_format+"/"+READ_Table+"/"
global dataS3file_err; dataS3file_err = READ_Table+"-err"+"."+file_format

# global value
global Result_Rowcount

# ------------------------
# Send JSON to Amazon S3
# ------------------------

def _send_to_s3(out_buffer,out_buffer_err=None):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(dataS3Bucket)
    my_bucket.put_object(Key=dataS3Folder+dataS3file, Body=out_buffer.getvalue())
    # error
    if out_buffer_err != None:
        my_bucket.put_object(Key=dataS3Folder_err+dataS3file_err, Body=out_buffer_err.getvalue())
        

# ----------------------------
# Change RFC results to output
# ----------------------------

def _result_to_output(result):
    #find results list
    out_table = result["OUT_TABLE"]
    print("Result Count: "+str(len(result[out_table])))
    global Result_Rowcount; Result_Rowcount = len(result[out_table])
    
    # get filed
    data_field = []
    for line in result["FIELDS"]:
        data_field.append(line["FIELDNAME"])
    print("Field Count: "+str(len(data_field)))
  
    # body
    data = []
    data_err = []
    data_count = 0
    err_count = 0
    for line in result[out_table]:
        raw_data = line["WA"].strip().split(Table_Delimiter)
        if len(raw_data) == len(data_field):
            data_count += 1
            data.append(raw_data)
        else:
            err_count += 1
            data_err.append(raw_data)
            
    print("Data Count: "+str(data_count))             
    print("Error Count: "+str(err_count))
    print("Total Count: "+str(data_count+err_count))
    pd_result = pd.DataFrame(data,columns=data_field)

    # pandas to json or parquet
    if len(data_err) == 0:
        if file_format == 'json':
            out_buffer = io.StringIO()
            pd_result.to_json(out_buffer,orient = 'records')
        elif file_format == 'parquet':
            pd_table = pa.Table.from_pandas(pd_result)
            out_buffer = io.BytesIO()
            pq.write_table(pd_table, out_buffer)
        _send_to_s3(out_buffer)
    else:
        pd_result_err = pd.DataFrame(data_err)
        if file_format == 'json':
            out_buffer = io.StringIO()
            pd_result.to_json(out_buffer,orient = 'records')
            # error
            out_buffer_err = io.StringIO()
            pd_result_err.to_json(out_buffer,orient = 'records')
        elif file_format == 'parquet':
            pd_table = pa.Table.from_pandas(pd_result)
            out_buffer = io.BytesIO()
            pq.write_table(pd_table, out_buffer)
            # error
            pd_table_err = pa.Table.from_pandas(pd_result_err)
            out_buffer_err = io.BytesIO()
            pq.write_table(pd_table_err, out_buffer_err)
        _send_to_s3(out_buffer, out_buffer_err)
        
# --------------------------
# Call RFC Function
# --------------------------

def _call_rfc_function():
    conn = Connection(ashost=ASHOST, sysnr=SYSNR, client=CLIENT, user=USER, passwd=PASSWD)

    print("----Begin of RFC---")
    x = 0
    global Result_Rowcount;Result_Rowcount = Table_Rowcount
    while (x < maxEntities and Result_Rowcount == Table_Rowcount ):
        result = conn.call(RFC_Function, QUERY_TABLE = READ_Table, DELIMITER = Table_Delimiter, ROWSKIPS=x, ROWCOUNT=Table_Rowcount)
        global dataS3file; dataS3file = READ_Table+str(x)+"."+file_format
        _result_to_output(result)
        # print(Result_Rowcount)
        x = x + Table_Rowcount
    
# ------------------------
# Start of Program
# ------------------------  

_call_rfc_function()
print("----End of RFC---")

"""])

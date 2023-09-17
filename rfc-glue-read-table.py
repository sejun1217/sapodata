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

# System info
ASHOST='' #Adjust
CLIENT='' #Adjust
SYSNR=''  #Adjust
USER=''   #Adjust
PASSWD ='' #Adjust

# RFC & Table settiong
RFC_Function = "/SAPDS/RFC_READ_TABLE2"
READ_Table = "VBAP"
# TABLE_Fields = ["MANDT", "VBELN", "ERDAT", "ERZET"]
# Table_Rowcount = 5

# S3 Info
file_format = ".parquet"
dataS3Bucket = "" #Adjust - Speficy your bucket name to store the data e.g. <AccountID>-saprfcglue
dataS3Folder = "glue/"
dataS3file = READ_Table+file_format

# ------------------------
# Send JSON to Amazon S3
# ------------------------

def _send_to_s3(out_buffer):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(dataS3Bucket)
    my_bucket.put_object(Key=dataS3Folder+dataS3file, Body=out_buffer.getvalue())
    

# ----------------------------
# Change RFC results to output
# ----------------------------

def _result_to_output(result):
    #find results list"
    out_table = result["OUT_TABLE"]
    print(len(result[out_table]))
    
    # get filed
    data_field = []
    for line in result["FIELDS"]:
        data_field.append(line["FIELDNAME"])
    print(len(data_field))
  
    # body
    data = []
    for line in result[out_table]:
        raw_data = line["WA"].strip().split(';')
        data.append(raw_data)
    
   
    pd_result = pd.DataFrame(data,columns=data_field)

    # pandas to json or parquet
    if file_format == '.json':
       out_buffer = io.StringIO()
       pd_result.to_json(out_buffer,orient = 'records')
    elif file_format == '.parquet':
       pd_table = pa.Table.from_pandas(pd_result)
       out_buffer = io.BytesIO()
       pq.write_table(pd_table, out_buffer)
    
    _send_to_s3(out_buffer)
    
# --------------------------
# Call RFC Function
# --------------------------

def _call_rfc_function():
    conn = Connection(ashost=ASHOST, sysnr=SYSNR, client=CLIENT, user=USER, passwd=PASSWD)
    print("----Begin of RFC---")
    result = conn.call(RFC_Function, QUERY_TABLE = READ_Table, DELIMITER = ";")
    # result = conn.call("RFC_READ_TABLE", QUERY_TABLE="READ_Table", FIELDS = fields, DELIMITER = ";", ROWCOUNT=Table_Rowcount)
    _result_to_output(result)
    

# ------------------------
# Start of Program
# ------------------------  

_call_rfc_function()


"""])

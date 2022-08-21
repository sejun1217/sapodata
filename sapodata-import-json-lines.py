import boto3
import requests
import json
from requests.auth import HTTPBasicAuth
import base64
from botocore.exceptions import ClientError
import os


# SAP System Info
sapHostName = "<ELB URL or SAP Applications URL>"
sapPort = "<ELB Listener Port>"
odpServiceName = "<OData Service Name>"
odpEntitySetName = "<OData Entity Name>"
sapUser = '<SAP User>'
sapPassword = '<SAP User Password>'

# S3 Info
dataS3Bucket = "<Amazon S3 bucket name>"
dataS3Folder = "<Amazon S3 folder name>"
dataS3name = "<Amazon S3 object name>"
verify = False # verify HTTPS Certification = False
reLoad = False
isInit = True

# ------------------------------------
# delete local file
# ------------------------------------   

def _delete_dump_local_file(filename):
    if os.path.exists(filename):
        os.remove(filename)

# ------------------------------------
# Get base url for HTTP requests to SAP
# ------------------------------------
def _get_base_url():
    global sapPort
    if sapPort == "":
        sapPort = "443"
    return "https://" + sapHostName + ":" + sapPort + "/sap/opu/odata/sap/" + odpServiceName
    

# ------------------------------------
# request HTTPS to SAP
# ------------------------------------

def _http_request(line):
    
    # Retrieve the CSRF token first
    url = _get_base_url()
    session = requests.Session()
    response = session.head(url, auth=HTTPBasicAuth(sapUser,sapPassword), headers={'x-csrf-token': 'fetch'}, verify=verify)
    token = response.headers.get('x-csrf-token', '')
    print(response)
    
    # Execute Post request
    url = _get_base_url() + "/" + odpEntitySetName
    print(url)
    headers = { "Content-Type" : "application/json; charset=utf-8","X-CSRF-Token" : token }
    response =  session.post(url, auth=HTTPBasicAuth(sapUser,sapPassword), headers=headers, json=line, verify=verify)
    print(response)
    

# ------------------------
# Perform import
# ------------------------

def import_json_to_sap():
    
    # download a json from S3
    s3_filename = dataS3Folder + '/' + dataS3file
    s3 = boto3.client('s3')
    s3.download_file(dataS3Bucket,s3_filename,dataS3file)
    
    # read S3 file and request to import json to sap  
    with open(dataS3file,encoding= "utf-8") as f: 
	    for line in f: _http_request(json.loads(line))
	    
	# delete local json file
    _delete_dump_local_file(dataS3file)

 
# ------------------------
# Start of Program
# ------------------------  

import_json_to_sap()
 



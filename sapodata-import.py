import boto3
import requests
import json
from requests.auth import HTTPBasicAuth
import base64
from botocore.exceptions import ClientError

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
totalEntities = 100 #totalEntities = <Number of Entries>
reLoad = False
isInit = True

# ------------------------------------
# Get base url for HTTP calls to SAP
# ------------------------------------
def _get_base_url():
    global sapPort
    if sapPort == "":
        sapPort = "443"
    return "https://" + sapHostName + ":" + sapPort + "/sap/opu/odata/sap/" + odpServiceName
    

# ------------------------
# Perform import
# ------------------------

def _import_json():
    verify = False

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
    response =  session.post(url, auth=HTTPBasicAuth(sapUser,sapPassword), headers=headers, json=ijson, verify=verify)
    print(response)
    
# ------------------------
# Get data from Amazon S3
# ------------------------

def _get_data(x):
    # make samle json : totalEntities
    json_content = { "Vbeln" : str(x), "Vkorg" : "1710" }
    
    # json read from S3
    #file_to_read = dataS3Folder + '/' + dataS3name
    #s3 = boto3.resource('s3')
    #content_object = s3.Object(dataS3Bucket, file_to_read)
    #file_content = content_object.get()['Body'].read().decode('utf-8')
    #file_content = "[" + file_content + "]" 
    #file_content = file_content.replace('\n', ',')
    #file_content = file_content.replace(',]', ']')
    #json_content = json.loads(file_content)
    
    # make samle json : 1 entry
    #file_content = '''{
    #        "Vbeln": "3456",
    #        "Vkorg": "1710"
    #    }'''
    #json_content = json.loads(file_content)
    
    return(json_content)
 
# ------------------------
# Start of Program
# ------------------------  

x = 1
while (x <= totalEntities):
    ijson = _get_data(x)
    _import_json()
    x += 1

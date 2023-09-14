import boto3
import requests
from requests.auth import HTTPBasicAuth
import json
import datetime
import os
import traceback
import copy
import uuid
import urllib3

sapHostName = "<ELB URL or SAP Applications URL>"
sapPort = "<ELB Listener Port>"
odpServiceName = "<OData Service Name>"
odpEntitySetName = "<OData Entity Name>"
dataS3Bucket='<Amazon S3 bucket name>'
dataS3Folder='<Amazon S3 folder name>'
sapUser = '<SAP USER>'
sapPassword = '<SAP Password>'
dataChunkSize = 1000
totalEntities = 10000
_athenacompatiblejson = True

# ------------------------
# Initialize
# ------------------------
def _setResponse(success,message, data, numberofrecs):
    response = {
        'success'   : success,
        'message'   : message,
        'data'      : data,
        'numberofrecs' : numberofrecs
    }
    return response


# ------------------------
# Perform extract
# ------------------------
def _extract(skip_key):
    global response
    
    skip_key=str(skip_key)
    url = _get_base_url() + "/" + odpEntitySetName + "?$format=json&$top=1000&$skip=" + skip_key
    print(url)
    
    headers = {
        "prefer" : "odata.maxpagesize=" + str(dataChunkSize) + ",odata.track-changes"
    }
    sapresponse =  _make_http_call_to_sap(url,headers)
    sapresponsebody = json.loads(sapresponse.text)
    _response = copy.deepcopy(sapresponsebody)

    d = sapresponsebody.pop('d',None)
    results = d.pop('results',None)
    for result in results:
        _metadata = result.pop('__metadata',None)
        
    # ERDAT Format
    for i in range(len(results)):
        epoch_time = int(results[i]["ERDAT"][6:-2])/1000
        datetime_type_value = datetime.datetime.fromtimestamp(epoch_time)
        datetime_type_value = datetime_type_value.strftime("%F")
        results[i]["ERDAT"] =  str(datetime_type_value)
    # ERDAT Format
    
    if len(results)<=0:
        response = _setResponse(True,"No data available to extract from SAP", _response, 0)
    elif(dataS3Bucket != ""):
        s3 = boto3.resource('s3')
        fileName = ''.join([dataS3Folder,'/',str(uuid.uuid4().hex[:6]),odpServiceName, "_", odpEntitySetName,".json"]) 
        object = s3.Object(dataS3Bucket, fileName)
        if _athenacompatiblejson==True:
            object.put(Body=_athenaJson(results))
        else:    
            object.put(Body=json.dumps(results,sort_keys=True,indent=4))
            
        response = _setResponse(True,"Data successfully extracted and stored in S3 Bucket with key " + fileName, None, len(results))
    else:
        response = _setResponse(True,"Data successfully extracted from SAP", _response, len(results))
        

    
# ------------------------------------
# Get base url for HTTP calls to SAP
# ------------------------------------
def _get_base_url():
    global sapPort
    if sapPort == "":
        sapPort = "443"
    return "https://" + sapHostName + ":" + sapPort + "/sap/opu/odata/sap/" + odpServiceName
    
# ------------------------------------
# Call SAP HTTP endpoint
# ------------------------------------    
def _make_http_call_to_sap(url,headers):
    verify = False
    return requests.get( url, headers=headers, auth=HTTPBasicAuth(sapUser,sapPassword), verify=verify)
    
# ------------------------------------
# Conver JSON to athena format
# ------------------------------------
def _athenaJson(objects):
    return '\n'.join(json.dumps(obj) for obj in objects)
    
# ------------------------------------
# Run Script
# ------------------------------------

from pexecute.process import ProcessLoom
loom = ProcessLoom(max_runner_cap=9)

x = 0
while (x < totalEntities ):
    loom.add_function(_extract, [x], {})
    x += dataChunkSize

output = loom.execute()

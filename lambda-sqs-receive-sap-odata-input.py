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
sapUser = "<SAP User>"
sapPassword = "<SAP User Password>"

# S3 Info
verify = False # verify HTTPS Certification = False
reLoad = False
isInit = True
queue_url = 'https://sqs.us-east-1.amazonaws.com/xxxxxxxxxxxx/sap-odata-import-test'

sqs_client = boto3.client(
    service_name='sqs',
    region_name='us-east-1'
)

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
# Get data from Amazon SQS
# ------------------------

def _get_data():
    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        AttributeNames=[
            'All'
        ]
    )
    
    for message in response['Messages']:
        body = json.loads(message['Body'])

    return(body)


def lambda_handler(event, context):
    # TODO implement
    ijson = _get_data()
    _http_request(ijson)

    return {
        'statusCode': 200,
        'body': json.dumps('lambdasqsodata')
    }


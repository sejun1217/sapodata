import boto3, json

sqs_client = boto3.client(
    service_name='sqs',
    region_name='us-east-1'
)

inputJson = {'Vbeln':'22','Vkorg':'1710'}

response = sqs_client.send_message(
    QueueUrl='https://sqs.us-east-1.amazonaws.com/xxxxxxxx/sap-odata-import-test',
    MessageBody=json.dumps(inputJson)
)

print(json.dumps(response))
import boto3, json

queue_url = 'https://sqs.us-east-1.amazonaws.com/750051865020/sap-odata-import-test'

sqs_client = boto3.client(
    service_name='sqs',
    region_name='us-east-1'
)

# SQS 메시지 조회
response = sqs_client.receive_message(
    QueueUrl=queue_url,
    MaxNumberOfMessages=10,
    AttributeNames=[
        'All'
    ]
)

print(json.dumps(response))

for message in response['Messages']:
    body = json.loads(message['Body'])

print(body)
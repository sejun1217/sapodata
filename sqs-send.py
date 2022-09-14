import boto3, json

sqs_client = boto3.client(
    service_name='sqs',
    region_name='us-east-1'
)

response = sqs_client.send_message(
    QueueUrl='https://sqs.us-east-1.amazonaws.com/xxxxxxxxxxxxx/sqs-test-1',
    MessageBody={ "Vbeln" : "22", "Vkorg" : "1710" }
)

print(json.dumps(response))
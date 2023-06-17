import boto3

kms_client = boto3.client('kms')
key_id = "506f85cb-c3fa-4224-89b3-0a2928cd319a"
plaintext = "i am a boy"

response = kms_client.encrypt(
    KeyId=key_id,
    Plaintext=plaintext
)

ciphertext = response['CiphertextBlob']

print(ciphertext)

response = kms_client.decrypt(
    CiphertextBlob=ciphertext,
    KeyId=key_id
)

plaintext = response['Plaintext']

print(plaintext)




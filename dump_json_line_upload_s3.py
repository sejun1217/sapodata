import json
import boto3
import os

dataS3Bucket = "dump-json-for-odata"
tablename = "zdemoinput"
dataS3Folder = "s4h/zdemoinput/"
totalEntities = 200


# ------------------------------------
# delete local file
# ------------------------------------   

def _delete_dump_local_file(filename):
    if os.path.exists(filename):
        os.remove(filename)



# ------------------------------------
# upload_to_s3
# ------------------------------------   

def _upload_to_s3(filename):
    s3 = boto3.client('s3')
    s3_fileName = "".join([dataS3Folder,filename])
    s3.upload_file(filename,dataS3Bucket,s3_fileName)
    
    # After upload to S3, delete local file
    _delete_dump_local_file(filename)


# ------------------------------------
# create_dump_file
# ------------------------------------    

def create_dump_file():
    # create contents
    x = 1
    json_dump =[]
    while (x <= totalEntities):
        # change!! Table schema
        new_entry = { "vbeln" : str(x), "matnr" : "1710" }
        json_dump.append(new_entry)
        x = x + 1
    
    # create file name 
    filename = ''.join([tablename,"-",str(totalEntities),".json"]) 
    
    # write local file
    with open(filename,encoding= "utf-8",mode="w") as file: 
	    for i in json_dump: file.write(json.dumps(i) + "\n")
	    
	# S3 Upload
    _upload_to_s3(filename)


# ------------------------------------
# Run Script
# ------------------------------------

create_dump_file()

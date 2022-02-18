import boto3
import botocore
import sys
import os
import datetime

if len(sys.argv) != 4:
    print("This program only supports backup and restore.")
    print("Excution syntax for BACK UP: ​python3​ ​BackUp.py​ ​backup​ directory-name bucket-name::directory-name")
    print("Excution syntax for RESTORE: ​python3​ ​BackUp.py​ restore bucket-name::directory-name directory-name")
    quit()

# ------------------------------ BACK UP ------------------------------
if sys.argv[1] == "backup":

    # checking user's directory
    my_directory = sys.argv[2]
    if not os.path.exists(my_directory):
        print("directory-name:", my_directory)
        print("This directory does not exist")
        quit()

    # getting access to aws s3
    s3 = boto3.resource("s3")
    session = boto3.session.Session()
    region = session.region_name
    client = boto3.client('s3', region_name = region)

    # checking aws s3 directory
    s3_directory = sys.argv[3].split("::")
    bucket_name = s3_directory[0]
    bucket_directory = s3_directory[1]

    # checking if the bucket exsists or needs to be created
    try:
        client.head_bucket(Bucket = bucket_name)
        print("Backing Up to Bucket [", bucket_name, "], Directory in the Bucket:", bucket_directory)
    except botocore.exceptions.ClientError as e:
        # if e.response['Error']['Code'] == "400":
        #     print("Cannot call the Bucket.")
        #     quit()
        # if e.response['Error']['Code'] == "404":
        #     print("Cannot call the Bucket.")
        #     quit()
        try: 
            s3.create_bucket(Bucket = bucket_name, CreateBucketConfiguration = {"LocationConstraint":region})
            print("Backing Up to Bucket [", bucket_name, "] just created.")
        except s3.meta.client.exceptions.BucketAlreadyExists as e:
            print("Cannot Find Bucket [", bucket_name, "] and fail to create.")
            quit()

    # travalsing the user's directory to back up
    for path, dir, files in os.walk(my_directory):
        print("-------------------------")
        print("Path:", path)
        print("Directory:", dir)
        print("Files:", files)
    
        # setting the directory to back up
        last_path = my_directory.split("/")
        backup_path = bucket_directory + "/" + last_path[len(last_path)-1]
        current_path = path.replace(my_directory, backup_path)
        print("current path: ", current_path)
    
        # backing up files
        for file in files:
            file_path = os.path.join(path, file)
            bucket_path = current_path + "/" + file

            try: 
                # checking if the file already exists
                obj = s3.meta.client.head_object(Bucket = bucket_name, Key = bucket_path)

                # chekcing if the file needs to be updated
                latest_version = datetime.datetime.fromtimestamp(os.path.getmtime(file_path), tz=datetime.timezone.utc)
                if obj["LastModified"] < latest_version:
                    try:
                        # updating the file
                        s3.Object(bucket_name, bucket_path).put(Body=open(file_path, "rb"))
                        print("Updated:", bucket_path)
                    except botocore.exceptions.ClientError as e:
                        print("Fail to Back Up - file:", file)
    
            except botocore.exceptions.ClientError as e:
                try:
                    # uploading the file
                    s3.Object(bucket_name, bucket_path).put(Body=open(file_path, "rb"))
                    print("Uploaded:", bucket_path)
                except botocore.exceptions.ClientError as e:
                    print("Fail to Back Up - file:", file)

    print("-------------------------")
    print("BACK UP COMPLETED")

# ------------------------------ REESTORE ------------------------------
elif sys.argv[1] == "restore":
    
    # checking user's directory
    my_directory = sys.argv[3]
    if not os.path.exists(my_directory):
        print(my_directory)
        print("This directory does not exist")
        quit()

    # getting access to aws s3
    s3 = boto3.resource("s3")
    session = boto3.session.Session()
    region = session.region_name
    client = boto3.client('s3', region_name = region)

    # checking aws s3 directory
    s3_directory = sys.argv[2].split("::")
    bucket_name = s3_directory[0]
    bucket_directory = s3_directory[1]

    # checking if the bucket exsists or needs to be created
    try:
        client.head_bucket(Bucket = bucket_name)
        print("Restoring from Bucket [", bucket_name, "], Directory in the Bucket:", bucket_directory)
    except botocore.exceptions.ClientError as e:
        print("Cannot Find Bucket [", bucket_name, "] to Restore")
        quit()

    # Serching files from bucket_directory to restore
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix = bucket_directory):

        # setting the directory to back up
        current_path = obj.key.replace(bucket_directory, my_directory)  

        # making dir if it doesn't exist
        if not os.path.exists(os.path.dirname(current_path)):
            os.makedirs(os.path.dirname(current_path))

        # downloading file
        last = current_path[len(current_path)-1]
        if (last != '/'):
            bucket.download_file(obj.key, current_path)
              
    print("RESTORE COMPLETED")

# ------------------------------ ERROR ------------------------------
else: 
    print("This program only supports backup and restore.")
    print("Excution syntax for BACK UP: ​python3​ ​BackUp.py​ ​backup​ directory-name bucket-name::directory-name")
    print("Excution syntax for RESTORE: ​python3​ ​BackUp.py​ restore bucket-name::directory-name directory-name")
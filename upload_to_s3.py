import subprocess
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from getpass import getpass
import os
import sys
from tqdm import tqdm

# Ensure correct usage
if len(sys.argv) != 5:
    print("Usage: python script.py <bucket_name> <s3_directory_path> <local_download_path> <encrypted_credentials_path>")
    sys.exit(1)

bucket_name = sys.argv[1]
s3_directory_path = sys.argv[2].rstrip('/')  # Ensure no trailing slash for the S3 directory path
local_download_path = sys.argv[3]  # Local directory path to download the files
encrypted_credentials_path = sys.argv[4]

# Function to decrypt the credentials file and return AWS credentials
def decrypt_credentials(encrypted_file_path):
    passphrase = getpass("Enter your passphrase: ")
    decrypt_command = f'gpg --batch --yes --decrypt --passphrase-fd 0 {encrypted_file_path}'
    
    try:
        process = subprocess.Popen(decrypt_command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        output, error = process.communicate(input=passphrase)
        
        if process.returncode != 0:
            print(f"Failed to decrypt credentials: {error.strip()}")
            return None, None
        else:
            access_key, secret_key = output.strip().split('\n')
            return access_key, secret_key
    except Exception as e:
        print(f"An error occurred: {e}")
        return None, None

def upload_directory_to_s3(bucket_name, directory_path, encrypted_credentials_path):
    access_key, secret_key = decrypt_credentials(encrypted_credentials_path)
    if not access_key or not secret_key:
        print("Unable to obtain AWS credentials. Aborting upload.")
        return

    session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3_client = session.client('s3')

    total_files = sum([len(files) for r, d, files in os.walk(directory_path)])
    progress = tqdm(total=total_files, unit='file', desc='Checking and Uploading files')

    for subdir, dirs, files in os.walk(directory_path):
        for file in files:
            full_path = os.path.join(subdir, file)
            object_name = f"{os.path.relpath(full_path, start=directory_path)}"
            local_file_size = os.path.getsize(full_path)

            try:
                response = s3_client.head_object(Bucket=bucket_name, Key=object_name)
                s3_file_size = response['ContentLength']
                if local_file_size <= s3_file_size:
                    print(f"Skipping {object_name}, already uploaded with matching or larger size on S3.", flush=True)
                    progress.update(1)
                    continue
            except ClientError as e:
                # The file does not exist on S3, upload it
                if e.response['Error']['Code'] == "404":
                    pass  # Proceed to upload
                else:
                    print(f"Error checking {object_name} on S3: {e}")
                    continue

            # Upload the file since it's either not on S3 or is smaller than the local file
            s3_client.upload_file(Filename=full_path, Bucket=bucket_name, Key=object_name)
            progress.update(1)
            print(f"Uploaded {object_name}", flush=True)

    progress

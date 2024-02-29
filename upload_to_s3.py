import subprocess
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from getpass import getpass
import os
import sys
from tqdm import tqdm

# Ensure correct usage
if len(sys.argv) != 4:
    print("Usage: python script.py <bucket_name> <directory_path> <encrypted_credentials_path>")
    sys.exit(1)

bucket_name = sys.argv[1]
directory_path = sys.argv[2].rstrip('/')  # Ensure no trailing slash
encrypted_credentials_path = sys.argv[3]
directory_name = os.path.basename(directory_path)  # Get the name of the top-level directory

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

def get_total_size(directory_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(directory_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def upload_directory_to_s3(bucket_name, directory_path, encrypted_credentials_path):
    access_key, secret_key = decrypt_credentials(encrypted_credentials_path)
    if not access_key or not secret_key:
        print("Unable to obtain AWS credentials. Aborting upload.")
        return

    session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3 = session.resource('s3')
    s3_client = session.client('s3')
    bucket = s3.Bucket(bucket_name)

    total_size = get_total_size(directory_path)
    progress = tqdm(total=total_size, unit='B', unit_scale=True, desc='Uploading files', unit_divisor=1024)

    for subdir, dirs, files in os.walk(directory_path):
        for file in files:
            full_path = os.path.join(subdir, file)
            object_name = f"{directory_name}/{os.path.relpath(full_path, start=directory_path)}"
            local_file_size = os.path.getsize(full_path)

            try:
                response = s3_client.head_object(Bucket=bucket_name, Key=object_name)
                s3_file_size = response['ContentLength']

                if local_file_size > s3_file_size:
                    print(f"Local file '{object_name}' is larger than S3 version. Uploading...", flush=True)
                elif local_file_size == s3_file_size:
                    print(f"File '{object_name}' matches S3 size. Skipping...", flush=True)
                    progress.update(local_file_size)
                    continue
            except ClientError as e:
                if e.response['Error']['Code'] == "404":
                    # If the object does not exist on S3, proceed with the upload
                    pass
                else:
                    raise

            with open(full_path, 'rb') as data:
                bucket.put_object(Key=object_name, Body=data)
                progress.update(local_file_size)

    progress.close()

upload_directory_to_s3(bucket_name, directory_path, encrypted_credentials_path)


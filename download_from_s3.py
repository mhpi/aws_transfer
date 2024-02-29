import subprocess
import boto3
from botocore.exceptions import NoCredentialsError
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

# Function to download directory from S3
def download_from_s3(bucket_name, s3_directory_path, local_download_path, encrypted_credentials_path):
    access_key, secret_key = decrypt_credentials(encrypted_credentials_path)
    if not access_key or not secret_key:
        print("Unable to obtain AWS credentials. Aborting download.")
        return

    session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)

    # Ensure the local directory exists
    local_directory = os.path.join(local_download_path, os.path.basename(s3_directory_path) if s3_directory_path else '')
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    # Preparing for the progress bar
    objects = list(bucket.objects.filter(Prefix=s3_directory_path))
    progress = tqdm(objects, unit='file', desc='Downloading files')

    for obj in progress:
        if obj.key.endswith('/'):  # Skip directories
            continue
        target_path = os.path.join(local_directory, os.path.relpath(obj.key, start=s3_directory_path))
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        bucket.download_file(obj.key, target_path)
        progress.set_postfix(file=obj.key.split('/')[-1], refresh=True)

download_from_s3(bucket_name, s3_directory_path, local_download_path, encrypted_credentials_path)


# on suntzu:
#python /data/shared_data/aws/download_from_s3.py psu-data-transfer 1981 /data/shared_data/tmp /data/shared_data/aws/aws_transfer.gpg
# on wukong:
#python /data/shared_data/aws/download_from_s3.py psu-data-transfer 1981 /projects/mhpi/tmp /data/shared_data/aws/aws_transfer.gpg

import subprocess
import boto3
from botocore.exceptions import ClientError
from getpass import getpass
import os
import sys
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Ensure the correct usage
if len(sys.argv) < 4 or len(sys.argv) > 5:
    print("Usage: python script.py <bucket_name> <directory_path> <encrypted_credentials_path> [max_workers]")
    sys.exit(1)

bucket_name = sys.argv[1]
directory_path = sys.argv[2].rstrip('/')
encrypted_credentials_path = sys.argv[3]
max_workers = int(sys.argv[4]) if len(sys.argv) == 5 else 5

def decrypt_credentials(encrypted_file_path):
    passphrase = getpass("Enter your passphrase: ")
    decrypt_command = f'gpg --batch --yes --decrypt --passphrase "{passphrase}" {encrypted_file_path}'
    try:
        process = subprocess.Popen(decrypt_command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        output, error = process.communicate()
        if process.returncode != 0:
            print(f"Failed to decrypt credentials: {error.strip()}")
            return None, None
        else:
            access_key, secret_key = output.strip().split('\n')
            return access_key, secret_key
    except Exception as e:
        print(f"An error occurred: {e}")
        return None, None

def file_already_uploaded(s3_client, bucket_name, s3_object_name, local_file_path):
    try:
        s3_obj = s3_client.head_object(Bucket=bucket_name, Key=s3_object_name)
        local_file_size = os.path.getsize(local_file_path)
        return s3_obj['ContentLength'] == local_file_size
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise

def upload_file(s3_client, bucket_name, file_path, s3_object_name, progress, executor_id):
    if file_already_uploaded(s3_client, bucket_name, s3_object_name, file_path):
        progress.update(os.path.getsize(file_path))
        return
    try:
        s3_client.upload_file(file_path, bucket_name, s3_object_name)
        progress.update(os.path.getsize(file_path))
    except Exception as e:
        print(f"[{executor_id}] Error uploading {s3_object_name}: {str(e)}", flush=True)

def upload_directory_concurrently(bucket_name, directory_path, encrypted_credentials_path, max_workers):
    access_key, secret_key = decrypt_credentials(encrypted_credentials_path)
    if not access_key or not secret_key:
        print("Unable to obtain AWS credentials. Aborting upload.")
        return

    session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3_client = session.client('s3')

    files_to_upload = [(os.path.join(root, file), os.path.relpath(os.path.join(root, file), start=directory_path))
                       for root, _, files in os.walk(directory_path) for file in files if not file_already_uploaded(s3_client, bucket_name, os.path.relpath(os.path.join(root, file), start=directory_path), os.path.join(root, file))]

    if not files_to_upload:
        print(f"All files in {directory_path} are already uploaded and have the correct size, skipping.")
        return

    total_size = sum(os.path.getsize(f[0]) for f in files_to_upload)
    print(f"Uploading {len(files_to_upload)} files with a total size of {total_size} bytes using {max_workers} workers...")

    with tqdm(total=total_size, unit='B', unit_scale=True, desc='Uploading Files', miniters=1) as progress:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(upload_file, s3_client, bucket_name, *file_info, progress, idx): file_info[1]
                       for idx, file_info in enumerate(files_to_upload)}
            for future in as_completed(futures):
                pass  # Futures will automatically update progress

if __name__ == "__main__":
    upload_directory_concurrently(bucket_name, directory_path, encrypted_credentials_path, max_workers)

import os
import boto3
import dropbox
import paramiko
from typing import Dict, Tuple

class Upload:
    def __init__(self, backup_dir: str, services_config: Dict[str, Tuple]):
        self.backup_dir = backup_dir
        self.services_config = services_config

    def upload_to_services(self):
        for service, config in self.services_config.items():
            if service == "minio":
                self.upload_to_minio(config)
            elif service == "dropbox":
                self.upload_to_dropbox(config)
            elif service == "sftp":
                self.upload_to_sftp(config)

    def upload_to_minio(self, config: Tuple):
        if not all([config[0], config[1], config[2], config[3]]):
            raise ValueError("Minio server URL, access key, secret key, and bucket name must be provided to upload files to Minio.")

        s3_client = boto3.client(
            's3',
            endpoint_url=config[0],
            aws_access_key_id=config[1],
            aws_secret_access_key=config[2]
        )
        for filename in os.listdir(self.backup_dir):
            try:
                s3_client.upload_file(os.path.join(self.backup_dir, filename), config[3], filename)
                print(f"File uploaded to Minio: {filename}")
            except Exception as e:
                print(f"Failed to upload file to Minio: {filename} ({e})")

    def upload_to_dropbox(self, config: Tuple):
        if not all([config[0], config[1]]):
            raise ValueError("Dropbox access token and folder name must be provided to upload files to Dropbox.")

        dbx = dropbox.Dropbox(config[0])

        for filename in os.listdir(self.backup_dir):
            file_path = os.path.join(self.backup_dir, filename)
            try:
                dbx.files_upload(open(file_path, 'rb').read(), os.path.join(config[1], filename))
                print(f"File uploaded to Dropbox: {filename}")
            except Exception as e:
                print(f"Failed to upload file to Dropbox: {filename} ({e})")

    def upload_to_sftp(self, config: Tuple):

        if not all([config[0], config[1], config[2]]):
            raise ValueError("SFTP host, username, and password must be provided to upload files to an SFTP server.")

        sftp_client = paramiko.SSHClient()
        sftp_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            sftp_client.connect(config[0], username=config[1], password=config[2])
            sftp = sftp_client.open_sftp()

            for filename in os.listdir(self.backup_dir):
                sftp.put(os.path.join(self.backup_dir, filename), os.path.join('/path/to/remote/dir', filename))
                print(f"File uploaded to SFTP: {filename}")

            sftp_client.close()
        except Exception as e:
            print(f"Failed to upload files to SFTP server: {e}")

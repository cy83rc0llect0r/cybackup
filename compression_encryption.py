import os
import shutil
import gnupg
import tarfile
from datetime import datetime

class CompressionEncryption:
    def __init__(self, source_path, output_path, password, algorithm='gzip'):
        self.source_path = source_path
        self.output_path = output_path
        self.password = password
        self.algorithm = algorithm
        self.gpg = gnupg.GPG()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if os.path.dirname(output_path):
            os.makedirs(output_path)
        self.compressed_path = os.path.join(output_path, f'{os.path.basename(self.output_path)}_{timestamp}.tar.{algorithm}')
        self.encrypted_path = os.path.join(output_path, f'{os.path.basename(self.output_path)}_{timestamp}.tar.{algorithm}.gpg')

    def compress(self):
        mode = {
        'gzip': 'w:gz',
        'bzip2': 'w:bz2',
        'lzma': 'w:xz'
        }.get(self.algorithm, 'w:gz')

        with tarfile.open(self.compressed_path, mode) as tar:
            tar.add(self.source_path, arcname=os.path.basename(self.source_path))
            print(f'Compressed file created at {self.compressed_path}.')

    def encrypt(self):
        with open(self.compressed_path, 'rb') as f:
            status = self.gpg.encrypt(f, symmetric=True, passphrase=self.password, output=self.encrypted_path, encrypt=False)
            if status.ok:
                print(f'Encrypted file created at {self.encrypted_path}.')
            else:
                print(f'Error creating encrypted file: encrypted_data.status')

        os.remove(self.compressed_path)

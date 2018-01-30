import time
import packages.config as config
import packages.wrappers.s3_wrapper as s3_wrapper
import packages.file_handlers.add_columns as add_columns
import sys
import os

"""
    For Sales & Service Folders
        Find all in archive bucket that aren't in the processed bucket
        Download, add the two fields and then upload them to the processed bucket
"""

class Processor:

    def __init__(self):

        self.timestamp = int(time.time())
        self.config = config.Config()
        self.add_columns = add_columns.AddColumns(self.timestamp)

        self.archive_bucket = self.config.archive_bucket
        self.processed_archive_bucket = self.config.processed_archive_bucket

        self.s3_archive_wrapper = s3_wrapper.S3Wrapper()
        self.s3_archive_wrapper.setBucket(self.archive_bucket)

        self.s3_processed_wrapper = s3_wrapper.S3Wrapper()
        self.s3_processed_wrapper.setBucket(self.processed_archive_bucket)

        self.archive_filelist = []
        self.processed_filelist = []
        self.missing_files = []

    def run(self):
        print('Beginning processor...')

        for folder in self.config.file_patterns:

            pattern = self.config.file_patterns[folder]

            self.get_missing_files(folder)

            self.transfer_files(folder)

    def transfer_files(self, folder):
        for filename in self.missing_files:
            print('Transferring {}/{}'.format(folder, filename))

            print('Downloading...')
            keyname = "{}/{}".format(folder, filename)
            newname = "proc_{}".format(filename)
            self.s3_archive_wrapper.downloadFile(keyname, filename)

            print('Adding columns...')
            self.add_columns.process(filename, newname)

            print('Push new file up to processed_archive...')
            proc_keyname = "{}/{}".format(folder, newname)
            self.s3_processed_wrapper.copyFilesToS3(proc_keyname, newname)

            os.remove(filename)
            os.remove(newname)

    def get_missing_files(self, folder):
        self.get_archive_filelist(folder)
        self.get_processed_filelist(folder)
        self.missing_files = list(set(self.archive_filelist) - set(self.processed_filelist))

    def get_archive_filelist(self, folder):
        self.archive_filelist = self.s3_archive_wrapper.getBucketList(folder);

    def get_processed_filelist(self, folder):
        self.processed_filelist = self.s3_processed_wrapper.getBucketList(folder);
        # Ah, but remove the prefix proc_ from the filename
        self.processed_filelist = [name.replace('proc_', '') for name in self.processed_filelist]

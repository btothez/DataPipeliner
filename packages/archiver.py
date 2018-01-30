import time
import packages.config as config
import packages.wrappers.ftp_wrapper as ftp_wrapper
import packages.wrappers.s3_wrapper as s3_wrapper
import sys
import os


class Archiver:
    def __init__(self):
        self.config = config.Config()
        self.ftp_creds = self.config.ftp_creds
        self.archive_bucket = self.config.archive_bucket

        self.dealervault_filelist = []
        self.archive_filelist = []
        self.missing_files = {}

        self.ftp_wrapper = ftp_wrapper.FtpWrapper(
                self.ftp_creds
            )

        self.s3_wrapper = s3_wrapper.S3Wrapper()
        self.s3_wrapper.setBucket(self.archive_bucket)

    def run(self):
        print('Beginning archiver...')

        for folder in self.config.file_patterns:

            pattern = self.config.file_patterns[folder]

            print('Get dealervault list...')
            self.get_dealervault_filelist(pattern)

            print('Get current s3 archive list...')
            self.get_archive_filelist(folder)

            print('Find list of files')
            self.get_missing_files(folder)

            print('move the missings over')
            self.move_missing_files(folder)

    def get_dealervault_filelist(self, pattern):
        self.ftp_wrapper.getFileList()
        self.ftp_wrapper.filterFileListByPattern(pattern);
        self.dealervault_filelist = self.ftp_wrapper.dir_list

    def get_archive_filelist(self, folder):
        self.s3_filelist = self.s3_wrapper.getBucketList(folder);

    def get_missing_files(self, folder):
        self.missing_files[folder] = []
        for filename in self.dealervault_filelist:
            if filename not in self.s3_filelist:
                self.missing_files[folder].append(filename)

        # print(self.missing_files[folder])
        print(len(self.missing_files[folder]))
        print(len(self.dealervault_filelist))

    def move_missing_files(self, folder):
        for missing_file in self.missing_files[folder]:
            print(missing_file)
            # Download
            self.ftp_wrapper.downloadFile(missing_file)

            # Put on s3
            key_name = "{}/{}".format(folder, missing_file)
            self.s3_wrapper.copyFilesToS3(key_name, missing_file)

            os.remove(missing_file)

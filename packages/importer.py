import time
import packages.config as config
import packages.wrappers.s3_wrapper as s3_wrapper
import packages.file_handlers.add_columns as add_columns
import packages.redshift.uploaded_files as uploaded_files
import packages.redshift.redshift_copy as redshift_copy
import packages.redshift.redshift_insert as redshift_insert
import packages.redshift.all_timestamps as all_timestamps
import sys
import os

"""
    1. Get all files in that Redshift table
    2. Find all in missing from there that are in processed archive
    3. Copy the missing ones to dealervault-processed new imports
    4. RS COPY those into sales and services
    5. Empty out the new-imports bucket
    6. Get all timestamps that are NOW in sales/services that aren't in the 
        uploaded_files table, and add the files/timestamps to that table
"""

class Importer:

    def __init__(self):
        self.config = config.Config()
        self.processed_archive_bucket = self.config.processed_archive_bucket
        self.new_import_bucket = self.config.processed_new_imports

        self.s3_processed_wrapper = s3_wrapper.S3Wrapper()
        self.s3_processed_wrapper.setBucket(self.processed_archive_bucket)

        self.s3_newimport_wrapper = s3_wrapper.S3Wrapper()
        self.s3_newimport_wrapper.setBucket(self.new_import_bucket)

        self.uploaded_files = uploaded_files.UploadedFiles()

        self.redshift_copy = redshift_copy.RedshiftCopy()
        self.redshift_insert = redshift_insert.RedshiftInsert()
        self.all_timestamps = all_timestamps.AllTimestamps()

        self.processed_filelist = []
        self.previously_uploaded_files = []
        self.missing_files = []

        self.timestamp = 0


    def run(self):
        print('Beginning importer...')

        # First delete everything from new imports
        print('Clearing out new import bucket...')
        self.s3_newimport_wrapper.clearEntireBucket()

        for folder in self.config.file_patterns:

            print("Folder : {}".format(folder))

            print('Get list of all processed archive files')
            self.get_processed_archive_filelist(folder) 

            print('Get list of all imported files from table')
            self.get_previously_imported_files(folder)

            print('Copy missing files to new import bucket')
            self.copy_to_new_import(folder)

            if len(self.missing_files) > 0:
                print('There exist files for this folder in the processed-new-import bucket')
                print('Copy all files in bucket to raw table')
                self.copy_bucket_to_redshift(folder)

                print('Save previously missing files in table')
                self.save_missing_files(folder)

            else:
                print('No files exist for this folder, not doing redshift copy or save_missing_files')


    def save_missing_files(self, folder):
        if self.timestamp == 0:
            full_timestamps_list = self.all_timestamps.get_all_timestamps() 
            previous_uploaded_timestamps = self.uploaded_files.uploaded_import_times
            missing_timestamps = list(set(full_timestamps_list) - set(previous_uploaded_timestamps)) 
            if len(missing_timestamps) > 0:
                self.timestamp = missing_timestamps.pop()

        print('The Missing Timestamp : {}'.format(self.timestamp))

        rows_list = [(filename, folder, self.timestamp) 
                    for filename in self.missing_files]

        insert_str = """
            INSERT INTO {} ({}) VALUES {}
        """.format(
            self.config.uploaded_files_table, 
            'filename, type, import_time',
            ', '.join(['%s' for row in rows_list])            
        )
        self.redshift_insert.insert(insert_str, rows_list)

    def get_processed_archive_filelist(self, folder):
        self.processed_filelist = self.s3_processed_wrapper.getBucketList(folder);
        print(len(self.processed_filelist))
        print('( Not the files with APPT in them)')

        self.processed_filelist = [filename for filename in self.processed_filelist 
                                    if 'APPT.txt' not in filename]
        print(len(self.processed_filelist))
        print(self.processed_filelist)

    def get_previously_imported_files(self, folder):
        self.previously_uploaded_files = self.uploaded_files.get_uploaded_files_list()

    def copy_to_new_import(self, folder):
        self.missing_files = list(set(self.processed_filelist) 
                                    - set(self.previously_uploaded_files))
        print('Missing Files:')
        print(self.missing_files)

        for mf in self.missing_files:
            new_key_name = "{}/{}".format(folder, mf)
            self.s3_processed_wrapper.k.key = new_key_name
            print('Copying {} to new import bucket.'.format(new_key_name))

            self.s3_newimport_wrapper.bucket.copy_key( 
                new_key_name, 
                self.processed_archive_bucket,
                self.s3_processed_wrapper.k.key )

    def copy_bucket_to_redshift(self, folder):
        self.redshift_copy.copy(self.new_import_bucket, folder)



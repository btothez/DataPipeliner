import boto
from boto.s3.key import Key
import sys

class S3Wrapper:

    def __init__(self):
        self.s3 = boto.connect_s3()

    def setBucket(self, bucket_name):
        self.bucket_name = bucket_name
        self.bucket = self.s3.get_bucket(
            self.bucket_name
        )
        self.k = Key(self.bucket)
        

    def copyFilesToS3(self, keyname, filename): 
        self.k.key = keyname
        self.k.set_contents_from_filename(filename)

    def downloadFile(self, keyname, filename):
        self.k.key = keyname
        self.k.get_contents_to_filename(filename)

    def getBucketList(self, folder):
        return [fn.name.split('/')[1] 
                for fn in self.bucket.list()
                if folder in fn.name 
                and fn.name.split('/')[1]]

    def clearEntireBucket(self):
        for key in self.bucket.list():
            self.bucket.delete_key(key)
            print(' * deleted {} *'.format(key.name))

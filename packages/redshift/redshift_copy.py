import psycopg2
import psycopg2.extras
import packages.config as config

class RedshiftCopy:
    def __init__(self):
        self.config = config.Config()
        self.cursor_factory = psycopg2.extras.RealDictCursor

        self.delimiter = "\t"
        self.aws_access_key_id = self.config.aws_access_key_id
        self.aws_secret_access_key = self.config.aws_secret_access_key 
        self.ignore_header = " IGNOREHEADER 1 "

    def copy(self, bucket_name, table_name):
        copy_str = self.get_copy_str(bucket_name, table_name)

        # Connect 
        self.conn = psycopg2.connect(self.config.pg_conn_str)
        self.cur = self.conn.cursor(cursor_factory=self.cursor_factory)

        self.cur.execute(copy_str)
        self.conn.commit()

        self.cur.close()
        self.conn.close()

    def get_copy_str(self, bucket_name, table_name):
        s3_location = "s3://{}/{}".format(
            bucket_name,
            table_name 
        )

        copy_str = """ 
                copy {} from '{}' 
                credentials 'aws_access_key_id={};aws_secret_access_key={}' 
                delimiter '{}' TRUNCATECOLUMNS IGNOREBLANKLINES DATEFORMAT 'auto' 
                ACCEPTANYDATE {} MAXERROR AS 250;
            """.format(table_name, 
                        s3_location,                    
                        self.aws_access_key_id, 
                        self.aws_secret_access_key, 
                        self.delimiter,
                        self.ignore_header
                )

        print(copy_str)
        return copy_str

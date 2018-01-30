import psycopg2
import psycopg2.extras
import packages.config as config

class RedshiftInsert:
    def __init__(self):
        self.config = config.Config()
        self.aws_access_key_id = self.config.aws_access_key_id
        self.aws_secret_access_key = self.config.aws_secret_access_key 

    def insert(self, insert_str, args=[]):
        # Connect 
        self.conn = psycopg2.connect(self.config.pg_conn_str)
        self.cur = self.conn.cursor()

        self.cur.execute(insert_str, args)
        self.conn.commit()

        self.cur.close()
        self.conn.close()

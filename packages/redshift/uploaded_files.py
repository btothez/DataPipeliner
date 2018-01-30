import psycopg2
import psycopg2.extras
import packages.config as config

class UploadedFiles:
    def __init__(self):
        self.config = config.Config()
        self.cursor_factory = psycopg2.extras.RealDictCursor

    def get_uploaded_files_list(self):
        self.conn = psycopg2.connect(self.config.pg_conn_str)
        self.cur = self.conn.cursor(cursor_factory=self.cursor_factory)
        self.cur.execute("select * from {}".format(self.config.uploaded_files_table))

        results = self.cur.fetchall()

        uploaded_files =  [ result['filename'] for result in results ]
        self.uploaded_import_times = list(set([ result['import_time'] for result in results ]))

        self.cur.close()
        self.conn.close()
        return uploaded_files

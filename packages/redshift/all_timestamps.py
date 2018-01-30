import psycopg2
import psycopg2.extras
import packages.config as config

class AllTimestamps():
    def __init__(self):
        self.config = config.Config()
        self.cursor_factory = psycopg2.extras.RealDictCursor

    def get_all_timestamps(self):
        self.conn = psycopg2.connect(self.config.pg_conn_str)
        self.cur = self.conn.cursor(cursor_factory=self.cursor_factory)

        self.cur.execute("""
            select distinct import_time from (
            select distinct import_time from sales
            union
            select distinct import_time from service
            )""")

        all_timestamps =  [ result['import_time'] for result in self.cur.fetchall() ]

        self.cur.close()
        self.conn.close()
        return all_timestamps

import psycopg2
import psycopg2.extras
import packages.config as config
import logging

class GetAllRowsService():
    def __init__(self):
        self.config = config.Config()
        self.cursor_factory = psycopg2.extras.RealDictCursor

    def __call__(self, timestamp):
        self.offset = 0
        self.limit = 20000
        self.conn = psycopg2.connect(self.config.pg_conn_str)
        self.cur = self.conn.cursor(cursor_factory=self.cursor_factory)

        self.query(timestamp)
        all_rows = self.cur.fetchall()

        while len(all_rows) > 0:
            logging.info('yielding {} rows from service table'.format(len(all_rows)))

            yield all_rows
            self.offset += self.limit
            self.query(timestamp)
            all_rows = self.cur.fetchall()

        self.cur.close()
        self.conn.close()


    def query(self, timestamp):

        self.cur.execute("""
                        SELECT * from service WHERE first_name != ''
                        and last_name != '' and full_name != ''
                        and import_time={}
                        ORDER BY open_date, ro_number
                        OFFSET {} LIMIT {}
        """.format(timestamp,
            self.offset,
            self.limit))

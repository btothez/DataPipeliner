import psycopg2
import psycopg2.extras
import packages.config as config
import logging

class GetAllRowsSales():
    def __init__(self):
        self.config = config.Config()
        self.cursor_factory = psycopg2.extras.RealDictCursor

    def __call__(self, timestamp):
        self.conn = psycopg2.connect(self.config.pg_conn_str)
        self.cur = self.conn.cursor(cursor_factory=self.cursor_factory)
        self.limit = 20000
        self.offset = 0

        self.query(timestamp)
        all_rows = self.cur.fetchall()

        while len(all_rows) > 0:
            logging.info('yielding {} rows from sales table'.format(len(all_rows)))

            yield all_rows
            self.offset += self.limit
            self.query(timestamp)
            all_rows = self.cur.fetchall()

        self.cur.close()
        self.conn.close()

    def query(self, timestamp):
        x = """
                        SELECT * from sales WHERE first_name != ''
                        and last_name != '' and full_name != ''
                        and import_time={}
                        and deal_type in ('R', 'L', 'F', 'CRTSY')
                        ORDER BY contract_date, deal_number
                        OFFSET {} LIMIT {}
        """.format(timestamp,
            self.offset,
            self.limit)
        logging.info(x)

        self.cur.execute("""
                        SELECT * from sales WHERE first_name != ''
                        and last_name != '' and full_name != ''
                        and import_time={}
                        and deal_type in ('R', 'L', 'F', 'CRTSY')
                        ORDER BY contract_date, deal_number
                        OFFSET {} LIMIT {}
        """.format(timestamp,
            self.offset,
            self.limit))

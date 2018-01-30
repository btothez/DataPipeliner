import psycopg2
import psycopg2.extras
import packages.config as config

class GetAllVehicles():

    def __init__(self):
        self.config = config.Config()
        self.cursor_factory = psycopg2.extras.RealDictCursor

    def __call__(self, timestamp=0):
        self.conn = psycopg2.connect(self.config.pg_conn_str)
        self.cur = self.conn.cursor(cursor_factory=self.cursor_factory)
        self.cur.execute("""
            (
                SELECT vin, year, make, model, model_number from sales
                where sales.first_name != ''
                and sales.last_name != ''
                and sales.full_name != ''
                and sales.deal_type in ('R', 'L', 'F', 'CRTSY')
                and sales.import_time = '{0}'
            )
            union
            SELECT vin, year, make, model, model_number from service
            where service.first_name != ''
            and service.last_name != ''
            and service.full_name != ''
            and service.import_time = '{0}'
        """.format(timestamp))

        all_vehicles = self.cur.fetchall()

        self.cur.close()
        self.conn.close()

        return all_vehicles

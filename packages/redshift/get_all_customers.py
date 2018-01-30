import psycopg2
import psycopg2.extras
import packages.config as config

class GetAllCustomers():
    def __init__(self):
        self.config = config.Config()
        self.cursor_factory = psycopg2.extras.RealDictCursor

    def __call__(self, timestamp=0):
        self.conn = psycopg2.connect(self.config.pg_conn_str)
        self.cur = self.conn.cursor(cursor_factory=self.cursor_factory)
        self.cur.execute("""
            (
                select file_type, full_name, first_name, middle_name, last_name, suffix,
                birth_date, address_line_1, address_line_2, city, state, zip, county, cell_phone,
                home_phone, work_phone, work_extension, email_1, email_2, email_3, language, delegate_dealer_id, ro_number as specific_id

                from service
                where first_name != ''
                and last_name != ''
                and full_name != ''
                and import_time = '{0}'
            )

            union

            select file_type, full_name, first_name, middle_name, last_name, suffix,
            birth_date, address_line_1, address_line_2, city, state, zip, county, cell_phone,
            home_phone, work_phone, work_extension, email_1, email_2, email_3, language, delegate_dealer_id, deal_number as specific_id
            from sales
            where first_name != ''
            and last_name != ''
            and full_name != ''
            and sales.deal_type in ('R', 'L', 'F', 'CRTSY')
            and import_time = '{0}'
            """.format(timestamp))

        all_customers = self.cur.fetchall()

        self.cur.close()
        self.conn.close()

        return all_customers

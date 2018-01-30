import packages.mysql.mysql_connection as mysql_connection
import sys

class GrabCurrentCustomers:
    def __init__(self):
        self.chunk_size = 50000
        self.offset = 0

        self.query_str = """
            select *, ci.id as customer_info_id from customers c
            inner join customer_info ci
            on c.id = ci.customer_id
            limit {}
            offset {}
        """


    def run(self):
        # Connect
        self.mc = mysql_connection.MysqlConnection()
        rows = []
        for new_rows in self.generate_chunk():
            print('got new rows...{}'.format(len(new_rows)))
            rows.extend(new_rows)

        # Disconnect
        del(self.mc)
        return rows

    def generate_chunk(self):
        print('Generating Chunks for Current Customers')
        new_rows = self.run_query()
        while len(new_rows) > 0:
            yield new_rows
            self.increment_offset()
            new_rows = self.run_query()

    def run_query(self):
        return self.mc.run(
            self.query_str.format(
                self.chunk_size,
                self.offset
            )
        )

    def increment_offset(self):
        self.offset += self.chunk_size

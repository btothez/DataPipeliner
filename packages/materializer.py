import packages.mysql.mysql_connection as mysql_connection
import packages.config as config
import packages.redshift.get_all_rows_sales as get_all_rows_sales
import packages.redshift.get_all_rows_service as get_all_rows_service
import packages.import_handlers.missing_timestamps as missing_timestamps

import packages.substitutions.sub_dealers as sub_dealers
import packages.substitutions.sub_vehicles as sub_vehicles
import packages.substitutions.sub_customers as sub_customers

import packages.file_handlers.service_rows_to_csv as service_rows_to_csv
import packages.file_handlers.sales_rows_to_csv as sales_rows_to_csv

import logging
import os

"""
    1. for each ['sales', 'service']
    2. Get all the rows in that timestamp
    3. sub in customer_id, customer_info_id, vehicle_id, (good time to make the dealer_id sub thing)
    4. Write to gigantic csv
    5. Import that csv with mysqlimport
    6. Add those
"""

class Materializer:

    def __init__(self):
        logging.info('initializing materializer')
        self.config = config.Config()
        self.mysql_connection = mysql_connection.MysqlConnection()

        # Get Missing Timestamps (a callable class that is a generator)
        logging.info('getting all missing_timestamps')
        self.missing_timestamps = missing_timestamps.MissingTimestamps()

        logging.info('initializing table_queries (get_all_rows_sale, get_all_rows_service)')
        self.table_queries = {
            'sales': get_all_rows_sales.GetAllRowsSales(),
            'service': get_all_rows_service.GetAllRowsService()
        }

        logging.info('initializing subs')
        self.subs =[
            sub_dealers.SubDealers(),
            sub_vehicles.SubVehicles(),
            sub_customers.SubCustomers()
        ]


    def run(self):
        for ts in self.missing_timestamps():
            logging.info('Materializing for timestamp: {}'.format(ts))
            self.run_for_timestamp(ts)
            self.add_timestamp(ts)


    def run_for_timestamp(self, timestamp):

        for table in self.table_queries.keys():
            logging.info('Table {}'.format(table))
            self.query_and_write(timestamp, table)

    def query_and_write(self, timestamp, table):

        filename = self.config.materialized_filename.format(table)

        # Delete file
        try:
            os.remove(filename)
        except OSError:
            pass

        logging.info(filename)

        query_function = self.table_queries[table]
        logging.info(query_function)

        csv_writer = self.get_csv_writers(table)

        for row_chunk in query_function(timestamp):

            logging.info('queried a chunk from table'.format(table))

            for sub in self.subs:
                logging.info(sub)
                logging.info('subbing...')
                row_chunk = sub(row_chunk)

            logging.info('writing chunk to csv : {}'.format(filename))
            csv_writer(row_chunk, filename, timestamp)

        load_str = """
            LOAD DATA LOCAL INFILE '{}'
            INTO TABLE {}
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            IGNORE 1 LINES
        """.format(filename, table)

        logging.info(load_str)

        self.mysql_connection.cursor.execute(load_str)
        self.mysql_connection.db.commit()

    def get_csv_writers(self, table):
        logging.info('initializing csv_writer')

        if table == 'sales':
            csv_writer = sales_rows_to_csv.SalesRowsToCsv()
        else:
            csv_writer = service_rows_to_csv.ServiceRowsToCsv()

        return csv_writer


    def add_timestamp(self, timestamp):
        insert_str = """
            INSERT INTO imported_timestamps (timestamp) VALUES ({})
        """.format('%s')

        logging.info(insert_str)

        self.mysql_connection.insert(insert_str, [tuple([timestamp])])

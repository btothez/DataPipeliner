import packages.customers.objects.customer_dict as customer_dict
import packages.customers.methods.filter_only_new as filter_only_new
import packages.customers.methods.customer_dealer_insertion as customer_dealer_insertion
import packages.redshift.get_all_customers as get_all_customers
import packages.redshift.get_all_vehicles as get_all_vehicles

import packages.vehicles.vehicle_dict as vehicle_dict
import packages.vehicles.filter_only_new_vehicles as filter_only_new_vehicles
import packages.vehicles.dedupe_vehicles as dedupe_vehicles
import packages.vehicles.vehicle_insertion as vehicle_insertion

import packages.import_handlers.missing_timestamps as missing_timestamps
import packages.import_handlers.deduper as deduper
import packages.config as config
import sys
import logging
import time
import os
import gc

"""
    1. Get all current customers into customer dict
    2. Get list of missing timestamps
    3. For each of those missing timestamps, grab union of all rows from redshift
    4. Dedupe these rows
    5. Determine which of these rows are new
    6. Insert all new customers / custmer_info rows
    7. Extract all the Vehicles from those same rows
"""

class Constructor:

    def __init__(self):
        logging.info('initializing constructor')

        self.config = config.Config()

        self.get_all_customers = get_all_customers.GetAllCustomers()
        self.get_all_vehicles = get_all_vehicles.GetAllVehicles()

        self.deduper = deduper.Deduper()
        self.dedupe_vehicles = dedupe_vehicles.DedupeVehicles()

        self.customer_dealer_insertion = customer_dealer_insertion.CustomerDealerInsertion()
        self.vehicle_insertion = vehicle_insertion.VehicleInsertion()

        # Get Missing Timestamps (a callable class that is a generator)
        self.missing_timestamps = missing_timestamps.MissingTimestamps()

    def curr_sec(self):
        return int(time.time())

    def run(self):
        for ts in self.missing_timestamps():

            # Fork new process
            pid = os.fork()

            if pid == 0:
                # This is the child process, do the work
                logging.info('Missing Timestamp : {}'.format(ts))

                # Customers
                start_time = self.curr_sec()
                logging.info('CUSTOMERS')
                self.run_customers_for_timestamp(ts)
                logging.info('CUSTOMERS: TOOK {} SECONDS'.format(self.curr_sec() - start_time))

                # Vehicles
                start_time = self.curr_sec()
                logging.info('VEHICLES')
                self.run_vehicles_for_timestamp(ts)
                logging.info('VEHICLES: TOOK {} SECONDS'.format(self.curr_sec() - start_time))

                logging.info('Work thread done, exiting')
                sys.exit(0)

            else:
                # Parent thread, wait for kiddo to be done
                kid_process_id, status = os.waitpid(pid, 0)
                logging.info("kid-process-id {} || status{} Exiting loop".format(kid_process_id, status))

            # Try to free up some memory
            gc.collect()

    def run_customers_for_timestamp(self, ts):

        """ First get the new Customers """
        # Get union of all new timestamp rows
        logging.info('Getting union of all rows for timestamp = {}'.format(ts))
        self.all_customer_rows = self.get_all_customers(ts)
        logging.info(len(self.all_customer_rows))

        # Fill Customer Dict
        logging.info('Loading customer dictionary...')
        self.cd = customer_dict.CustomerDict()

        # Dedupe these rows
        logging.info('Deduping customer rows...')
        self.all_customer_rows = self.deduper(self.all_customer_rows)
        logging.info(len(self.all_customer_rows))

        logging.info('initializing filter_only_new')
        self.filter_only_new = filter_only_new.FilterOnlyNew(self.cd.customers_dict)

        # Filter only new rows
        logging.info('Filtering only new...')
        self.new_customer_rows = self.filter_only_new(self.all_customer_rows)
        logging.info(len(self.new_customer_rows))

        # Insert rows
        logging.info('Now inserting all customer/customer_info rows...')
        self.customer_dealer_insertion.insert(self.new_customer_rows, ts)
        logging.info('Done inserting customer/customer_info rows.')

        # Delete Stuff
        logging.info('Delete all customer stuff to save memory')
        del(self.all_customer_rows)
        del(self.new_customer_rows)
        del(self.filter_only_new)
        del(self.cd)

    def run_vehicles_for_timestamp(self, ts):

        """ Then get the new Vehicles """

        # Get all vehicles
        logging.info('Get all vehicle rows for timestamp = {}'.format(ts))
        self.all_vehicle_rows = self.get_all_vehicles(ts)
        logging.info(len(self.all_vehicle_rows))

        # Fill Vehicles Dict
        logging.info('Loading vehicle dictionary...')
        self.vd = vehicle_dict.VehicleDict()

        # Dedupe Vehicles
        logging.info('Dedupe Vehicles...')
        self.all_vehicle_rows = self.dedupe_vehicles(self.all_vehicle_rows)
        logging.info(len(self.all_vehicle_rows))

        # Init Filtering
        logging.info('initializing filter_only_new_vehicles')
        self.filter_only_new_vehicles = filter_only_new_vehicles.FilterOnlyNewVehicles(self.vd.vehicles_dict)

        # Filter only new rows
        logging.info('Filtering only new vehicles...')
        self.new_vehicle_rows = self.filter_only_new_vehicles(self.all_vehicle_rows)
        logging.info(len(self.new_vehicle_rows))

        logging.info('Insert new vehicles')
        self.vehicle_insertion.insert(self.new_vehicle_rows, ts)
        logging.info('Done inserting vehicle rows.')


        # Delete Stuff
        logging.info('Delete all vehicle stuff to save memory')
        del(self.all_vehicle_rows)
        del(self.new_vehicle_rows)
        del(self.filter_only_new_vehicles)
        del(self.vd)
